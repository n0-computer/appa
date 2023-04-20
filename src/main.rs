use std::sync::Arc;
use std::{net::SocketAddr, str::FromStr};

use anyhow::{Context as _, Result};
use appa::fs::Fs;
use appa::{hash_manifest::HashManifest, store::Store};
use bytes::Bytes;
use futures::{FutureExt};
use iroh::provider::DataSource;
use iroh::{
    porcelain::provide,
    protocol::GetRequest,
    provider::{create_collection, CustomHandler, Database},
    PeerId,
};
use tokio::io::AsyncWriteExt;

use clap::{Parser, Subcommand};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "appa")]
#[command(about = "A simple wnfs interface, syncing with iroh", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Init in `.appa`
    Init,
    /// Create a directory
    #[command(arg_required_else_help = true)]
    Mkdir {
        #[arg(value_name = "DIR")]
        dir: String,
    },
    /// Create the file at the given path, with the provided content.
    Add {
        #[arg(value_name = "NAME")]
        path: String,
        #[arg(value_name = "CONTENT")]
        content: String,
    },
    /// Delete the content at the given path.
    Rm {
        #[arg(value_name = "PATH")]
        path: String,
    },
    /// List the contents of the given directory.
    Ls {
        #[arg(value_name = "PATH")]
        path: String,
    },
    /// Show the content of the given file.
    Cat {
        #[arg(value_name = "PATH")]
        path: String,
    },
    /// Move from source to destination.
    Mv {
        #[arg(value_name = "SOURCE")]
        source: String,
        #[arg(value_name = "DESTINATION")]
        dest: String,
    },
    /// Print a manifest
    Manifest,
    /// Pull any missing files from the node
    Pull {
        #[clap(long, short)]
        peer: PeerId,
        /// The authentication token to present to the server.
        #[clap(long)]
        auth_token: String,
        /// Optional address of the provider, defaults to 127.0.0.1:4433.
        #[clap(long, short)]
        addr: Option<SocketAddr>,
    },
    /// Provide
    Provide {
        #[clap(long, short)]
        addr: Option<SocketAddr>,
        /// Auth token, defaults to random generated.
        #[clap(long)]
        auth_token: Option<String>,
    },

    /// Import a file tree
    Import {
        /// Source directory (on your machine)
        #[arg(value_name = "SOURCE")]
        source: String,

        /// Target path (in WNFS)
        #[arg(value_name = "TARGET")]
        target: String
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();

    let args = Cli::parse();

    const ROOT_DIR: &str = ".appa";

    match args.command {
        Commands::Init => {
            println!("Initializing ...");
            Fs::init(&ROOT_DIR).await?;
        }
        Commands::Mkdir { dir } => {
            let mut fs = Fs::load(&ROOT_DIR).await?;
            fs.mkdir(dir).await?;
            fs.commit().await?;
        }
        Commands::Add { path, content } => {
            let mut fs = Fs::load(&ROOT_DIR).await?;
            fs.add(path, content).await?;
            fs.commit().await?;
        }
        Commands::Rm { path } => {
            let mut fs = Fs::load(&ROOT_DIR).await?;
            fs.rm(path).await?;
            fs.commit().await?;
        }
        Commands::Ls { path } => {
            let fs = Fs::load(&ROOT_DIR).await.context("load")?;
            let list = fs.ls(path).await?;
            for (el, _) in list {
                println!("{}", el);
            }
        }
        Commands::Cat { path } => {
            let fs = Fs::load(&ROOT_DIR).await?;
            let content = fs.cat(path).await?;
            tokio::io::stdout().write_all(&content).await?;
        }
        Commands::Mv { source, dest } => {
            let mut fs = Fs::load(&ROOT_DIR).await?;
            fs.mv(source, dest).await?;
            fs.commit().await?;
        }
        Commands::Manifest => {
            let fs = Fs::load(&ROOT_DIR).await?;
            let manifest = fs.manifest_public()?;
            println!("public manifest: {manifest:#?}");

            let manifest = fs.manifest_private()?;
            println!("private manifest: {manifest:#?}");
        }
        Commands::Pull {
            peer,
            addr,
            auth_token,
        } => {
            let fs = Fs::load(&ROOT_DIR).await?;
            let manifest = fs.manifest()?;
            let store = fs.store().clone();
            // pull hashes from store & put hashes in a vec
            let mut opts = iroh::get::Options {
                peer_id: Some(peer),
                ..Default::default()
            };
            if let Some(addr) = addr {
                opts.addr = addr;
            };
            let token = iroh::protocol::AuthToken::from_str(&auth_token)
                .context("Wrong format for authentication token")?;

            let buf = postcard::to_stdvec(&manifest)?;
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    println!("interupting pull");
                }
                res = iroh::get::run(
                    Bytes::from(buf).into(),
                    token,
                    opts,
                    || async move { Ok(()) },
                    move |mut data| {
                        let store = store.clone();
                        async move {
                            if data.is_root() {
                                let hash = data.request().name;
                                let collection = data.read_collection(hash).await?;
                                data.set_limit(collection.total_entries() + 1);
                                data.user = Some(collection);
                            } else {
                                let index = usize::try_from(data.offset() - 1)?;
                                let hash = data.user.as_ref().unwrap().blobs()[index].hash;
                                let name = &data.user.as_ref().unwrap().blobs()[index].name;
                                let key = if name == appa::fs::LATEST {
                                    name.into()
                                } else {
                                    Store::key_for_hash(hash.as_ref())
                                };
                                let content = data.read_blob(hash).await?;
                                tracing::debug!("received blob for hash {hash:?}");
                                tokio::task::spawn_blocking(move || {
                                    store.put(&key, content)
                                }).await??;
                            }
                            data.end()
                        }
                    },
                    None,
                ) => {
                    res?;
                }
            };
        }
        Commands::Provide { addr, auth_token } => {
            let fs = Fs::load(&ROOT_DIR).await?;
            let manifest = fs.manifest()?;

            let db = Database::default();
            let custom_handler = CollectionMirrorHandler {
                manifest: Arc::new(manifest),
                store: fs.store().clone(),
            };
            let provider = provide(
                db.clone(),
                addr,
                auth_token,
                None,
                false,
                None,
                custom_handler,
            )
            .await?;

            iroh::porcelain::display_provider_info(&provider)?;

            let provider2 = provider.clone();
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    println!("Shutting down provider...");
                    provider2.shutdown();
                }
                res = provider => {
                    res?;
                }
            }
        }

        Commands::Import { source, target } => {
            let mut fs = Fs::load(&ROOT_DIR).await?;
            fs.import(&source, &target).await?;
            fs.commit().await?;
            println!("Imported {source} to {target}");
        }
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct CollectionMirrorHandler {
    manifest: Arc<HashManifest>,
    store: Store,
}

impl CustomHandler for CollectionMirrorHandler {
    fn handle(
        &self,
        data: Bytes,
        database: &Database,
    ) -> futures::future::BoxFuture<'static, anyhow::Result<GetRequest>> {
        let database = database.clone();
        let this = self.clone();

        async move {
            let requestor_manifest: HashManifest = postcard::from_bytes(&data)?;
            let diff = this.manifest.without(&requestor_manifest);
            let mut sources = diff.to_sources(&this.store)?;

            let path = this.store.get_path(appa::fs::LATEST)?;
            sources.push(DataSource::NamedFile {
                name: appa::fs::LATEST.into(),
                path,
            });

            let (new_db, hash) = create_collection(sources).await?;
            let new_db = new_db.to_inner();
            database.union_with(new_db);
            let request = GetRequest::all(hash);
            println!("{:?}", request);
            Ok(request)
        }
        .boxed()
    }
}
