use std::io::Cursor;
use std::path::{Component, Path};

use anyhow::{anyhow, Context as _, Result};
use appa::store::flatfs::Flatfs;
use cid::Cid;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;

use clap::{Parser, Subcommand};
use tracing_subscriber::{prelude::*, EnvFilter};
use wnfs::private::AccessKey;
use wnfs::root_tree::RootTree;

#[derive(Debug, Parser)]
#[command(name = "appa")]
#[command(about = "A simple wnfs interface, syncing with iroh", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Appa data directory
const ROOT_DIR: &str = ".appa";

/// Key storing the latest data root CID
pub const DATA_ROOT: &str = "DATA_ROOT_V1";

/// Key storing the access symmetric key for the private root directory
pub const PRIVATE_ACCESS_KEY: &str = "PRIVATE_ACCESS_KEY_V1";

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
            let mut root_tree = RootTree::empty(Flatfs::new(ROOT_DIR)?);
            let access_key = root_tree.create_private_root(&["private".into()]).await?;
            root_tree
                .store
                .put(PRIVATE_ACCESS_KEY, access_key.to_bytes()?)?;
            commit_fs(root_tree).await?;
        }
        Commands::Mkdir { dir } => {
            let mut fs = load_fs().await?;
            fs.mkdir(&parse_path(dir)).await?;
            commit_fs(fs).await?;
        }
        Commands::Add { path, content } => {
            let mut fs = load_fs().await?;
            fs.write(&parse_path(path), content.into_bytes()).await?;
            commit_fs(fs).await?;
        }
        Commands::Rm { path } => {
            let mut fs = load_fs().await?;
            fs.rm(&parse_path(path)).await?;
            commit_fs(fs).await?;
        }
        Commands::Ls { path } => {
            let fs = load_fs().await.context("load")?;
            let list = fs.ls(&parse_path(path)).await?;
            for (el, _) in list {
                println!("{}", el);
            }
        }
        Commands::Cat { path } => {
            let fs = load_fs().await?;
            let content = fs.read(&parse_path(path)).await?;
            tokio::io::stdout().write_all(&content).await?;
        }
        Commands::Mv { source, dest } => {
            let mut fs = load_fs().await?;
            fs.basic_mv(&parse_path(source), &parse_path(dest)).await?;
            commit_fs(fs).await?;
        }
        Commands::Import { source, target } => {
            let mut fs = load_fs().await?;
            let target_path = parse_path(target.clone());
            let mut files = futures::stream::iter(walkdir::WalkDir::new(&source));
            while let Some(file) = files.try_next().await? {
                let full_path = file.path();
                println!("Importing {}...", full_path.to_string_lossy());
                let rel_path = canonicalize_path(full_path.strip_prefix(&source)?)?;
                let path = [target_path.clone(), parse_path(rel_path)].concat();
                if file.file_type().is_dir() {
                    fs.mkdir(&path).await?;
                } else if file.file_type().is_file() {
                    let content = tokio::fs::read(full_path).await?;
                    // let size = content.len();
                    fs.write(&path, content).await?;
                }
            }
            println!("Imported {source} to {target}, committing...");
            commit_fs(fs).await?;
            println!("Committed.")
        }
    }

    Ok(())
}

fn read_data_root(store: &Flatfs) -> Result<Cid> {
    Ok(Cid::read_bytes(Cursor::new(
        store.get(DATA_ROOT)?.ok_or(anyhow!("No data root"))?,
    ))?)
}

fn commit_data_root(store: &Flatfs, cid: Cid) -> Result<()> {
    store.put(DATA_ROOT, cid.to_bytes())?;
    Ok(())
}

fn read_access_key(store: &Flatfs) -> Result<AccessKey> {
    Ok(AccessKey::parse(
        store
            .get(PRIVATE_ACCESS_KEY)?
            .ok_or(anyhow!("No access key"))?,
    )?)
}

fn parse_path(path: String) -> Vec<String> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

async fn load_fs() -> Result<RootTree<Flatfs>> {
    let bs = Flatfs::new(ROOT_DIR)?;
    let data_root = read_data_root(&bs).context("reading data root")?;
    let mut root_tree = RootTree::load(&data_root, bs)
        .await
        .context("loading root tree")?;
    let access_key = read_access_key(&root_tree.store)?;
    root_tree
        .load_private_root(&["private".into()], &access_key)
        .await?;
    Ok(root_tree)
}

async fn commit_fs(mut root_tree: RootTree<Flatfs>) -> Result<()> {
    let cid = root_tree.store().await?;
    commit_data_root(&root_tree.store, cid)?;
    Ok(())
}
/// converts a canonicalized relative path to a string, returning an error if
/// the path is not valid unicode
///
/// this will also fail if the path is non canonical, i.e. contains `..` or `.`,
/// or if the path components contain any windows or unix path separators
fn canonicalize_path(path: impl AsRef<Path>) -> anyhow::Result<String> {
    let parts = path
        .as_ref()
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
            Ok(c)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(parts.join("/"))
}
