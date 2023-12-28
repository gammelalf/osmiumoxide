use std::io::Result;

use prost_build::Config;

fn main() -> Result<()> {
    Config::new().bytes(&["."]).compile_protos(
        &["./proto/fileformat.proto", "./proto/osmformat.proto"],
        &["./proto/"],
    )?;
    Ok(())
}
