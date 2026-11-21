#[tokio::main]
async fn main() {
    std::process::exit(oxget::cli::main_entry().await);
}
