use anyhow::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["protos/remote.proto"], &["protos/"])?;
    Ok(())
}
