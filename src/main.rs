use anyhow::Result;

fn main() -> Result<()> {
    println!("Hello, world!");

    let dir = "appa-store";
    let store = appa::store::Store::new(dir)?;

    store.put("hello", b"world")?;
    let res = store.get("hello")?;
    let path = store.get_path("hello")?;
    println!(
        "got hello => {}, stored in {}",
        std::str::from_utf8(&res).unwrap(),
        path.display()
    );

    Ok(())
}
