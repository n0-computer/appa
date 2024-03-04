use crate::state::{
    Appa, ALPN_APPA_CAR_MIRROR_PULL, ALPN_APPA_KEY_VALUE_FETCH, DATA_ROOT, PRIVATE_ACCESS_KEY,
    ROOT_DIR,
};
use crate::store::cache_missing::CacheMissing;
use crate::store::flatfs::Flatfs;
use anyhow::{bail, Result};
use car_mirror::incremental_verification::IncrementalDagVerification;
use car_mirror::messages::{Bloom, PullRequest};
use car_mirror::traits::InMemoryCache;
use cid::Cid;
use futures::{SinkExt, TryStreamExt};
use iroh_base::ticket::Ticket;
use iroh_net::magic_endpoint::accept_conn;
use iroh_net::ticket::NodeTicket;
use iroh_net::{MagicEndpoint, NodeId};
use quinn::Connection;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::LengthDelimitedCodec;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PullMsg {
    root: Vec<u8>,
    resources: Vec<Vec<u8>>,
    bloom: Bloom,
}

impl PullMsg {
    fn new(root: Cid, request: PullRequest) -> Self {
        Self {
            root: root.to_bytes(),
            bloom: request.bloom,
            resources: request.resources.iter().map(Cid::to_bytes).collect(),
        }
    }

    fn into_parts(self) -> Result<(Cid, PullRequest)> {
        let root = Cid::read_bytes(Cursor::new(self.root))?;
        let resources = self
            .resources
            .iter()
            .map(|cid| Cid::read_bytes(Cursor::new(cid)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            root,
            PullRequest {
                resources,
                bloom: self.bloom,
            },
        ))
    }
}

pub async fn listen() -> Result<()> {
    let appa = Appa::load().await?;
    let cache = InMemoryCache::new(10_000, 150_000);

    let endpoint = Arc::new(
        MagicEndpoint::builder()
            .secret_key(appa.peer_key.clone())
            .alpns(vec![
                ALPN_APPA_CAR_MIRROR_PULL.to_vec(),
                ALPN_APPA_KEY_VALUE_FETCH.to_vec(),
            ])
            .bind(0)
            .await?,
    );

    let ticket = NodeTicket::new(endpoint.my_addr().await?)?;
    println!("Connect with this ticket: {ticket}");

    while let Some(conn) = endpoint.clone().accept().await {
        let (peer_id, alpn, conn) = accept_conn(conn).await?;
        tracing::info!(
            "new connection from {peer_id} with ALPN {alpn} (coming from {})",
            conn.remote_address()
        );

        if let Err(e) =
            accept_connection(peer_id, alpn.as_bytes(), conn, &appa, &endpoint, &cache).await
        {
            tracing::error!(?e, alpn, "Failed running protocol. Continuing.");
        }
    }
    Ok(())
}

async fn accept_connection(
    peer_id: NodeId,
    alpn: &[u8],
    conn: Connection,
    appa: &Appa,
    endpoint: &Arc<MagicEndpoint>,
    cache: &InMemoryCache,
) -> Result<(), anyhow::Error> {
    Ok(match alpn {
        ALPN_APPA_KEY_VALUE_FETCH => {
            let (mut send, mut recv) = conn.accept_bi().await?;
            tracing::info!("Waiting for data...");
            let msg = recv.read_to_end(100).await?;
            tracing::info!("Got data!");
            let key = String::from_utf8(msg)?;
            let value = appa.fs.store.get(&key)?;
            send.write_all(&postcard::to_stdvec(&value)?).await?;
            send.finish().await?;
        }
        ALPN_APPA_CAR_MIRROR_PULL => {
            let appa = appa.clone();
            let endpoint = Arc::clone(endpoint);
            let cache = cache.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((send, recv)) = conn.accept_bi().await else {
                        return;
                    };
                    let appa = appa.clone();
                    let endpoint = Arc::clone(&endpoint);
                    let cache = cache.clone();
                    tokio::spawn(async move {
                        server_respond_pull(recv, send, endpoint, peer_id, appa, cache).await
                    });
                }
            });
        }
        _ => {
            bail!("Unsupported protocol identifier");
        }
    })
}

async fn server_respond_pull(
    recv: impl AsyncRead + Unpin,
    send: impl AsyncWrite + Send + Unpin,
    endpoint: Arc<MagicEndpoint>,
    peer_id: iroh_net::NodeId,
    appa: Appa,
    cache: InMemoryCache,
) -> Result<()> {
    tracing::debug!("accepted bi stream");
    let mut recv = LengthDelimitedCodec::builder()
        .max_frame_length(128 * 1024)
        .new_read(recv);

    let typ = endpoint
        .connection_info(peer_id)
        .await?
        .map(|info| info.conn_type.to_string())
        .unwrap_or("None".into());
    tracing::info!("Endpoint connection type: {typ}");

    let Some(message) = recv.try_next().await? else {
        tracing::info!("Got EOF, closing.");
        return Ok(());
    };

    tracing::info!("got pull message");
    let (root, request) = postcard::from_bytes::<PullMsg>(&message)?.into_parts()?;

    tracing::info!("responding with car stream");
    car_mirror::common::block_send_car_stream(
        root,
        Some(request.into()),
        send,
        &appa.fs.store,
        &cache,
    )
    .await?;
    Ok(())
}

pub async fn sync(ticket: String) -> Result<()> {
    let ticket: NodeTicket = Ticket::deserialize(ticket.as_ref())?;
    let store = CacheMissing::new(150_000, Flatfs::new(ROOT_DIR)?);
    let config = car_mirror_config();
    let cache = InMemoryCache::new(10_000, 150_000);

    let endpoint = MagicEndpoint::builder()
        .alpns(vec![
            ALPN_APPA_CAR_MIRROR_PULL.to_vec(),
            ALPN_APPA_KEY_VALUE_FETCH.to_vec(),
        ])
        .bind(0)
        .await?;

    tracing::info!("Opening connection");
    let connection = endpoint
        .connect(ticket.node_addr().clone(), ALPN_APPA_KEY_VALUE_FETCH)
        .await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(DATA_ROOT.as_bytes()).await?;
    send.finish().await?;
    let Some(root_bytes): Option<Vec<u8>> = postcard::from_bytes(&recv.read_to_end(100).await?)?
    else {
        println!("No data root on remote peer.");
        return Ok(());
    };
    let root = Cid::read_bytes(Cursor::new(root_bytes))?;

    println!("Fetched data root: {root}");

    let connection = endpoint
        .connect(ticket.node_addr().clone(), ALPN_APPA_KEY_VALUE_FETCH)
        .await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(PRIVATE_ACCESS_KEY.as_bytes()).await?;
    send.finish().await?;
    let Some(access_key_bytes): Option<Vec<u8>> =
        postcard::from_bytes(&recv.read_to_end(200).await?)?
    else {
        println!("Missing access key.");
        return Ok(());
    };
    println!("Fetched access key.");

    let connection = endpoint
        .connect(ticket.node_addr().clone(), ALPN_APPA_CAR_MIRROR_PULL)
        .await?;

    loop {
        let dag_verification = IncrementalDagVerification::new([root], &store, &cache).await?;
        tracing::info!(
            num_blocks_want = dag_verification.want_cids.len(),
            num_blocks_have = dag_verification.have_cids.len(),
            "State of transfer"
        );

        let mut req: PullRequest = dag_verification
            .into_receiver_state(config.bloom_fpr)
            .into();

        if req.indicates_finished() {
            println!("Done!");
            store.inner.put(PRIVATE_ACCESS_KEY, &access_key_bytes)?;
            store.inner.put(DATA_ROOT, root.to_bytes())?;
            break;
        }

        req.resources.truncate(config.max_roots_per_round);

        tracing::info!("Opening new connection.");
        let (send, recv) = connection.open_bi().await?;
        let mut send = LengthDelimitedCodec::builder()
            .max_frame_length(128 * 1024)
            .new_write(send);

        tracing::info!("Sending pull msg");
        let msg = postcard::to_stdvec(&PullMsg::new(root, req))?;
        send.send(msg.into()).await?;
        tracing::info!("Pull msg sent, waiting for response");

        if let Err(e) =
            car_mirror::common::block_receive_car_stream(root, recv, &config, &store, &cache).await
        {
            tracing::warn!(?e, "Got error on receive, continuing.");
        }
    }
    Ok(())
}

pub fn car_mirror_config() -> car_mirror::common::Config {
    let config = car_mirror::common::Config::default();
    config
}
