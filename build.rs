use anyhow::Result;
fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.compile_protos(&["protos/remote.proto"], &["protos/"])?;
    Ok(())
}
