fn main() -> Result<(), Box<dyn std::error::Error>> {
    match shadow_rs::ShadowBuilder::builder().build() {
        Ok(_) => {}
        Err(e) => {
            println!("cargo:warning=shadow_rs error: {}", e);
        }
    }
    Ok(())
}
