use std::{path::PathBuf, rc::Rc};

use anyhow::Result;
use appa::hash_manifest;
use chrono::Utc;
use tokio::io::AsyncWriteExt;
use wnfs::{common::BlockStore, public::PublicDirectory};

use clap::{Parser, Subcommand};

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
    /// Print a manifest
    Manifest,
}

const LATEST: &str = "LATEST";

async fn ensure_store(dir: &str) -> Result<(appa::store::Store, Rc<PublicDirectory>)> {
    let path = PathBuf::from(dir);
    anyhow::ensure!(
        path.exists(),
        "Appa is not initialized, please call 'appa init'"
    );

    let store = appa::store::Store::new(dir)?;

    let root_dir = if let Some(latest) = store.get(LATEST)? {
        let root_cid: cid::Cid = std::str::from_utf8(&latest)?.parse()?;
        let dir: PublicDirectory = store.get_deserializable(&root_cid).await?;
        Rc::new(dir)
    } else {
        Rc::new(PublicDirectory::new(Utc::now()))
    };

    Ok((store, root_dir))
}

async fn commit(
    store: &mut appa::store::Store,
    root_dir: &mut Rc<PublicDirectory>,
) -> Result<cid::Cid> {
    let root_cid = root_dir.store(store).await?;
    store.put(LATEST, root_cid.to_string().as_bytes())?;

    Ok(root_cid)
}

fn as_segments(path: String) -> Vec<String> {
    path.split("/")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    const ROOT_DIR: &str = ".appa";

    match args.command {
        Commands::Init => {
            println!("Initializing ...");
            if PathBuf::from(ROOT_DIR).exists() {
                anyhow::bail!("already initialized");
            }
            let mut store = appa::store::Store::new(ROOT_DIR)?;
            let mut root_dir = Rc::new(PublicDirectory::new(Utc::now()));
            let _root_cid = commit(&mut store, &mut root_dir).await?;
        }
        Commands::Mkdir { dir } => {
            let (mut store, mut root_dir) = ensure_store(&ROOT_DIR).await?;
            root_dir
                .mkdir(&as_segments(dir), Utc::now(), &store)
                .await?;
            let _root_cid = commit(&mut store, &mut root_dir).await?;
        }
        Commands::Add { path, content } => {
            let (mut store, mut root_dir) = ensure_store(&ROOT_DIR).await?;
            let content_cid = store
                .put_block(content.into(), libipld::IpldCodec::Raw.into())
                .await?;
            root_dir
                .write(&as_segments(path), content_cid, Utc::now(), &store)
                .await?;
            let _root_cid = commit(&mut store, &mut root_dir).await?;
        }
        Commands::Rm { path } => {
            let (mut store, mut root_dir) = ensure_store(&ROOT_DIR).await?;
            root_dir.rm(&as_segments(path), &store).await?;
            let _root_cid = commit(&mut store, &mut root_dir).await?;
        }
        Commands::Ls { path } => {
            let (store, root_dir) = ensure_store(&ROOT_DIR).await?;
            let list = root_dir.ls(&as_segments(path), &store).await?;
            for (el, _) in list {
                println!("{}", el);
            }
        }
        Commands::Cat { path } => {
            let (store, root_dir) = ensure_store(&ROOT_DIR).await?;
            let content_cid = root_dir.read(&as_segments(path), &store).await?;
            let content = store.get_block(&content_cid).await?;
            tokio::io::stdout().write_all(&content).await?;
        }
        Commands::Manifest => {
            let (mut store, root_dir) = ensure_store(&ROOT_DIR).await?;
            let root = root_dir.store(&mut store).await?;
            let manifest = hash_manifest::walk_dag(store, root)?;
            println!("{manifest:#?}");
        }
    }

    Ok(())
}
