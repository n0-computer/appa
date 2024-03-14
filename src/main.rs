use std::path::{Component, Path};

use anyhow::{Context as _, Result};
use appa::commands::doctor::doctor;
use appa::commands::listen_sync::{listen, sync};
use appa::nfs::AppaNfs;
use appa::state::Appa;
use futures::TryStreamExt;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use tokio::io::AsyncWriteExt;

use clap::{Parser, Subcommand};
use tracing_subscriber::{prelude::*, EnvFilter};
use wnfs::common::Storable;

#[derive(Debug, Parser)]
#[command(name = "appa")]
#[command(about = "A simple wnfs interface, syncing with iroh-net and car-mirror", long_about = None)]
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
    /// Import a file tree
    Import {
        /// Source directory (on your machine)
        #[arg(value_name = "SOURCE")]
        source: String,

        /// Target path (in WNFS)
        #[arg(value_name = "TARGET")]
        target: String,
    },
    /// Listen for syncing requests
    Listen,
    /// Issue a syncing request to a listening node
    Sync {
        /// The ticket printed on the other node's listen command
        #[arg(value_name = "TICKET")]
        ticket: String,
        /// The path of a directory on the other node to sync from
        #[arg(value_name = "PATH")]
        path: String,
    },
    /// The appa debugging tool
    Doctor {
        /// Item to debug, e.g. CID or store key
        #[arg(value_name = "ITEM")]
        item: String,
    },
    /// Run an NFS server for the appa filesystem locally
    Nfsserve,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();

    let args = Cli::parse();

    match args.command {
        Commands::Init => {
            println!("Initializing ...");
            let mut appa = Appa::init().await?;
            appa.commit().await?;
        }
        Commands::Mkdir { dir } => {
            let mut appa = Appa::load().await?;
            appa.fs.mkdir(&parse_path(dir)?).await?;
            appa.commit().await?;
        }
        Commands::Add { path, content } => {
            let mut appa = Appa::load().await?;
            // TODO: Streaming support in RootTree & Appa
            appa.fs
                .write(&parse_path(path)?, content.into_bytes())
                .await?;
            appa.commit().await?;
        }
        Commands::Rm { path } => {
            let mut appa = Appa::load().await?;
            appa.fs.rm(&parse_path(path)?).await?;
            appa.commit().await?;
        }
        Commands::Ls { path } => {
            let appa = Appa::load().await?;
            let list = appa.fs.ls(&parse_path(path)?).await?;
            for (el, _) in list {
                println!("{}", el);
            }
        }
        Commands::Cat { path } => {
            let appa = Appa::load().await?;
            let content = appa.fs.read(&parse_path(path)?).await?;
            tokio::io::stdout().write_all(&content).await?;
        }
        Commands::Mv { source, dest } => {
            let mut appa = Appa::load().await?;
            appa.fs
                .basic_mv(&parse_path(source)?, &parse_path(dest)?)
                .await?;
            appa.commit().await?;
        }
        Commands::Doctor { item } => {
            doctor(item).await?;
        }
        Commands::Import { source, target } => {
            let mut appa = Appa::load().await?;
            let forest_cid = appa.fs.forest.store(&appa.fs.store).await?;
            tracing::debug!(%forest_cid, "Before");
            let target_path = parse_path(target.clone())?;
            let mut files = futures::stream::iter(walkdir::WalkDir::new(&source));
            while let Some(file) = files.try_next().await? {
                let full_path = file.path();
                println!("Importing {}...", full_path.to_string_lossy());
                let rel_path = parse_path(full_path.strip_prefix(&source)?)?;
                let path = [target_path.clone(), rel_path].concat();
                if file.file_type().is_dir() {
                    appa.fs.mkdir(&path).await?;
                } else if file.file_type().is_file() {
                    let content = tokio::fs::read(full_path).await?;
                    // let size = content.len();
                    appa.fs.write(&path, content).await?;
                }
            }
            println!("Imported {source} to {target}, committing...");
            appa.commit().await?;
            let forest_cid = appa.fs.forest.store(&appa.fs.store).await?;
            tracing::debug!(%forest_cid, "After");
            println!("Committed.")
        }
        Commands::Listen => {
            listen().await?;
        }
        Commands::Sync { ticket, path } => {
            sync(ticket, path).await?;
        }
        Commands::Nfsserve => {
            const HOSTPORT: u32 = 11111;
            let appa = Appa::load().await?;
            let listener =
                NFSTcpListener::bind(&format!("127.0.0.1:{HOSTPORT}"), AppaNfs::new(appa))
                    .await
                    .unwrap();
            tracing::info!("Staring serve");
            listener.handle_forever().await?;
        }
    }

    Ok(())
}

/// converts a canonicalized relative path to a string, returning an error if
/// the path is not valid unicode
///
/// this will also fail if the path is non canonical, i.e. contains `..` or `.`,
/// or if the path components contain any windows or unix path separators
fn parse_path(path: impl AsRef<Path>) -> Result<Vec<String>> {
    path.as_ref()
        .components()
        .map(|c| {
            let c = if let Component::Normal(x) = c {
                x.to_str().context("invalid character in path")?
            } else {
                anyhow::bail!("invalid path component {:?}", c)
            };
            anyhow::ensure!(
                !c.contains('/') && !c.contains('\\'),
                "invalid path component {:?}",
                c
            );
            Ok(c.to_string())
        })
        .collect::<Result<Vec<_>>>()
}
