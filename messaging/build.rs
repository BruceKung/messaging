fn main() -> Result<(), Box<dyn std::error::Error>> {
    match shadow_rs::new() {
        Ok(_) => {}
        Err(e) => {
            println!("cargo:warning=shadow_rs error: {}", e);
        }
    }
    Ok(())
}
