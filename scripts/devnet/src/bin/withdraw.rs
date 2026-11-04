fn main() -> Result<(), Box<dyn std::error::Error>> {
    devnet_scripts::withdraw::run(false)
}
