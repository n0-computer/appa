use std::rc::Rc;

use anyhow::Result;
use appa::hash_manifest;
use chrono::Utc;
use wnfs::{common::BlockStore, public::PublicDirectory};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world from appa!");

    let dir = ".appa-store";
    let mut store = appa::store::Store::new(dir)?;

    // Create a new directory.
    let root_dir = &mut Rc::new(PublicDirectory::new(Utc::now()));

    // Add a /pictures/cats subdirectory.
    root_dir
        .mkdir(&["pictures".into(), "cats".into()], Utc::now(), &store)
        .await?;

    // Add a file to /pictures/dogs directory.
    let content_cid = store
        .put_block(b"Hi billie".to_vec(), libipld::IpldCodec::Raw.into())
        .await?;
    root_dir
        .write(
            &["pictures".into(), "dogs".into(), "billie.jpeg".into()],
            content_cid,
            Utc::now(),
            &store,
        )
        .await?;

    // Delete /pictures/cats directory.
    root_dir
        .rm(&["pictures".into(), "cats".into()], &store)
        .await?;

    // List all the children of /pictures directory.
    let result = root_dir.ls(&["pictures".into()], &store).await?;

    // Print the result.
    println!("Files in /pictures: {:#?}", result);

    // Store
    let root_cid = root_dir.store(&mut store).await?;

    println!(
        "store under {} at {}",
        root_cid,
        store.get_path(&root_cid.to_string())?.display()
    );

    let manifest = hash_manifest::walk_dag(store, root_cid)?;
    println!("{manifest:#?}");

    Ok(())
}
