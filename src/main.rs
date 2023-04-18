use anyhow::Result;
use wnfs::common::BlockStore;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world from appa!");

    let dir = "appa-store";
    let mut store = appa::store::Store::new(dir)?;
    let data = b"hello world";

    let cid = store
        .put_block(data.to_vec(), libipld::IpldCodec::Raw)
        .await?;
    println!("stored data under {}", cid);

    let res = store.get_block(&cid).await?;
    let path = store.get_path(&cid.to_string())?;

    println!(
        "got hello => {}, stored in {}",
        std::str::from_utf8(&res).unwrap(),
        path.display()
    );

    Ok(())
}
