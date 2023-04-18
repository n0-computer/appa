use anyhow::{Context as _, Result};
use appa::fs::Fs;
use std::net::SocketAddr;

use bytes::Bytes;
use futures::FutureExt;
use iroh::{
    porcelain::provide,
    protocol::GetRequest,
    provider::{CustomHandler, Database},
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
            fs.manifest()?;
        }
        Commands::Pull { .. } => {
            // let mut opts = get::Options {
            //     peer_id: Some(peer),
            //     keylog: cli.keylog,
            //     ..Default::default()
            // };
            // if let Some(addr) = addr {
            //     opts.addr = addr;
            // }
            // let token = AuthToken::from_str(&auth_token)
            //     .context("Wrong format for authentication token")?;
            // let get = GetInteractive::Hash {
            //     hash: *hash.as_hash(),
            //     opts,
            //     token,
            //     single,
            // };
            // tokio::select! {
            //     biased;
            //     res = get_interactive(get, out) => res,
            //     _ = tokio::signal::ctrl_c() => {
            //         println!("Ending transfer early...");
            //         Ok(())
            //     }
            // }
            todo!();
        }
        Commands::Provide { addr, auth_token } => {
            let db = Database::default();
            let custom_handler = CollectionMirrorHandler;
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
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct CollectionMirrorHandler;

impl CustomHandler for CollectionMirrorHandler {
    fn handle(
        &self,
        _data: Bytes,
        database: &Database,
    ) -> futures::future::BoxFuture<'static, anyhow::Result<GetRequest>> {
        let _database = database.clone();
        async move {
            // let readme = Path::new(env!("CARGO_MANIFEST_DIR")).join("CHANGELOG.md");
            // let sources = vec![DataSource::File(readme)];
            // let (new_db, hash) = create_collection(sources).await?;
            // let new_db = new_db.to_inner();
            // database.union_with(new_db);
            // let request = GetRequest::all(hash);
            // println!("{:?}", request);
            // Ok(request)
            todo!();
        }
        .boxed()
    }
}
