use std::io::Cursor;
use std::path::{Component, Path};

use anyhow::{anyhow, Context as _, Result};
use appa::store::flatfs::Flatfs;
use cid::Cid;
use futures::TryStreamExt;
use iroh_net::key::SecretKey;
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
const DATA_ROOT: &str = "DATA_ROOT_V1";

/// Key storing the access symmetric key for the private root directory
const PRIVATE_ACCESS_KEY: &str = "PRIVATE_ACCESS_KEY_V1";

/// Secret key used for the iroh-net magic endpoint
const PEER_SECRET_KEY: &str = "PEER_SECRET_KEY_V1";

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
    /// Listen for connections
    Listen,
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
        Commands::Import { source, target } => {
            let mut appa = Appa::load().await?;
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
            println!("Committed.")
        }
        Commands::Listen => {
            println!("Unimplemented. Sorry");
        }
    }

    Ok(())
}

struct Appa {
    fs: RootTree<Flatfs>,
    peer_key: SecretKey,
}

impl Appa {
    pub async fn init() -> Result<Self> {
        let mut root_tree = RootTree::empty(Flatfs::new(ROOT_DIR)?);

        let access_key = root_tree.create_private_root(&["private".into()]).await?;
        root_tree
            .store
            .put(PRIVATE_ACCESS_KEY, access_key.to_bytes()?)?;

        let peer_key = SecretKey::generate();
        root_tree
            .store
            .put(PEER_SECRET_KEY, peer_key.to_bytes().to_vec())?;

        Ok(Self {
            fs: root_tree,
            peer_key,
        })
    }

    pub async fn commit(&mut self) -> Result<()> {
        let cid = self.fs.store().await?;
        self.fs.store.put(DATA_ROOT, cid.to_bytes())?;
        Ok(())
    }

    pub async fn load() -> Result<Self> {
        let bs = Flatfs::new(ROOT_DIR)?;

        let data_root_entry = bs.get(DATA_ROOT)?.ok_or(anyhow!("No data root"))?;
        let data_root = Cid::read_bytes(Cursor::new(data_root_entry))?;

        let mut root_tree = RootTree::load(&data_root, bs).await?;

        let access_key_bytes = &root_tree
            .store
            .get(PRIVATE_ACCESS_KEY)?
            .ok_or(anyhow!("No access key"))?;
        let access_key = AccessKey::parse(access_key_bytes)?;

        root_tree
            .load_private_root(&["private".into()], &access_key)
            .await?;

        let peer_key_bytes = root_tree.store.get(PEER_SECRET_KEY)?.unwrap_or_else(|| {
            println!("No secret key for peering found - generating a new one.");
            SecretKey::generate().to_bytes().to_vec()
        });
        let peer_key = SecretKey::from_bytes(
            &peer_key_bytes
                .try_into()
                .map_err(|b: Vec<u8>| anyhow!("Wrong secret key len: {}", b.len()))?,
        );

        Ok(Self {
            fs: root_tree,
            peer_key,
        })
    }
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
