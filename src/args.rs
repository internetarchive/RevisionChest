use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Wikipedia dump file (.bz2 or .7z)
    pub input: Option<PathBuf>,

    /// Directory containing Wikipedia dump files
    #[arg(short = 'd')]
    pub input_dir: Option<PathBuf>,

    /// Output directory for .mwrev.zst files
    #[arg(short = 'o')]
    pub output_dir: Option<PathBuf>,

    /// Namespaces to include, comma-separated (e.g., --namespace=0,118)
    #[arg(long, value_delimiter = ',')]
    pub namespace: Option<Vec<String>>,

    /// SQLite database file
    #[arg(long, default_value = "index.db")]
    pub db: PathBuf,

    /// Override or specify the wiki domain (e.g., en.wikipedia.org)
    #[arg(long)]
    pub domain: Option<String>,
}
