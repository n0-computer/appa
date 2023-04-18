use anyhow::{Context as _, Result};
use appa::fs::Fs;
use tokio::io::AsyncWriteExt;

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
    /// Move from source to destination.
    Mv {
        #[arg(value_name = "SOURCE")]
        source: String,
        #[arg(value_name = "DESTINATION")]
        dest: String,
    },
    /// Print a manifest
    Manifest,
}

#[tokio::main]
async fn main() -> Result<()> {
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
    }

    Ok(())
}
