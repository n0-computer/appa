use std::io::Cursor;
use std::sync::Arc;

use crate::state::{
    Appa, ALPN_APPA_CAR_MIRROR_PULL, ALPN_APPA_KEY_VALUE_FETCH, DATA_ROOT, PRIVATE_ACCESS_KEY,
    ROOT_DIR,
};
use crate::store::cache_missing::CacheMissing;
use crate::store::flatfs::Flatfs;
use anyhow::Result;
use car_mirror::common::CarFile;
use car_mirror::incremental_verification::IncrementalDagVerification;
use car_mirror::messages::{Bloom, PullRequest};
use car_mirror::traits::InMemoryCache;
use cid::Cid;
use futures::{SinkExt, TryStreamExt};
use iroh_base::ticket::Ticket;
use iroh_net::magic_endpoint::accept_conn;
use iroh_net::ticket::NodeTicket;
use iroh_net::MagicEndpoint;

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
        match alpn.as_bytes() {
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
                let endpoint = Arc::clone(&endpoint);
                let config = car_mirror_config();
                let cache = cache.clone();
                tokio::spawn(async move {
                    let (send, recv) = conn.accept_bi().await?;
                    tracing::debug!("accepted bi stream, waiting for data...");

                    let mut recv = LengthDelimitedCodec::builder()
                        .max_frame_length(128 * 1024)
                        .new_read(recv);

                    let mut send = LengthDelimitedCodec::builder()
                        .max_frame_length(config.receive_maximum)
                        .new_write(send);

                    loop {
                        let typ = endpoint
                            .connection_info(peer_id)
                            .await?
                            .map(|info| info.conn_type.to_string())
                            .unwrap_or("None".into());
                        tracing::info!("Endpoint connection type: {typ}");

                        let Some(message) = recv.try_next().await? else {
                            tracing::info!("Got EOF, closing.");
                            return Ok::<_, anyhow::Error>(());
                        };
                        tracing::info!("got pull message");

                        let (root, request) =
                            postcard::from_bytes::<PullMsg>(&message)?.into_parts()?;

                        tracing::info!("decoded msg");

                        let response = car_mirror::pull::response(
                            root,
                            request,
                            &config,
                            &appa.fs.store,
                            &cache,
                        )
                        .await?;

                        tracing::info!("Sending response ({} bytes)", response.bytes.len());
                        send.send(response.bytes).await?;
                        tracing::info!("Sent.");
                    }
                });
            }
            _ => {
                println!("Unsupported protocol identifier (ALPN): {alpn}");
            }
        }
    }
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

    let (send, recv) = connection.open_bi().await?;
    let mut send = LengthDelimitedCodec::builder()
        .max_frame_length(128 * 1024)
        .new_write(send);
    let mut recv = LengthDelimitedCodec::builder()
        .max_frame_length(config.receive_maximum)
        .new_read(recv);

    let mut last_response = None;
    loop {
        let req = car_mirror::pull::request(root, last_response, &config, &store, &cache).await?;

        let dag_verification = IncrementalDagVerification::new([root], &store, &cache).await?;
        tracing::info!(
            num_blocks_want = dag_verification.want_cids.len(),
            num_blocks_have = dag_verification.have_cids.len(),
            "State of transfer"
        );

        if req.indicates_finished() {
            println!("Done!");
            store.inner.put(PRIVATE_ACCESS_KEY, &access_key_bytes)?;
            store.inner.put(DATA_ROOT, root.to_bytes())?;
            break;
        }
        let msg = postcard::to_stdvec(&PullMsg::new(root, req))?;
        tracing::info!("Sending pull msg");
        send.send(msg.into()).await?;
        tracing::info!("Pull msg sent, waiting for response");

        let Some(bytes) = recv.try_next().await? else {
            println!("Prematurely closed stream! Aborting.");
            break;
        };
        tracing::info!("Response received, {} bytes", bytes.len());
        let response = CarFile {
            bytes: bytes.into(),
        };

        last_response = Some(response);
    }
    Ok(())
}

pub fn car_mirror_config() -> car_mirror::common::Config {
    let mut config = car_mirror::common::Config::default();
    config.receive_maximum *= 100;
    config.send_minimum *= 180;
    config
}
