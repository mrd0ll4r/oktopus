use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["pb/unixfs.proto"], &["pb/"])?;
    Ok(())
}
