use std::path::PathBuf;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Build .mwrev.zst files from Wikipedia dumps
    Build(BuildArgs),
    /// Sync recent changes from Wikipedia API
    Sync(SyncArgs),
}

#[derive(Parser, Debug)]
pub struct SyncArgs {
    /// Output directory for .mwrev.zst files
    #[arg(short = 'o')]
    pub output_dir: PathBuf,

    /// Namespaces to include, comma-separated (e.g., --namespace=0,118)
    #[arg(long, value_delimiter = ',')]
    pub namespace: Option<Vec<String>>,

    /// SQLite database file (if not using Postgres)
    #[arg(long, default_value = "index.db")]
    pub db: PathBuf,

    /// Wikipedia domain (e.g., en.wikipedia.org)
    #[arg(long)]
    pub domain: String,

    /// Keep checking for recent changes every 10 minutes (or specified interval)
    #[arg(long)]
    pub ongoing: bool,

    /// Interval in minutes for ongoing sync (default: 10)
    #[arg(long)]
    pub interval: Option<u64>,

    /// Number of concurrent jobs (default: number of logical CPUs)
    #[arg(short = 'j', long)]
    pub jobs: Option<usize>,
}

#[derive(Parser, Debug)]
pub struct BuildArgs {
    /// Wikipedia dump file (.bz2, .7z or .xml)
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

    /// Do not create or update the database
    #[arg(long)]
    pub no_db: bool,

    /// Output metadata to a Parquet file. Use with --no-db if you want to skip database ingestion entirely.
    #[arg(long)]
    pub parquet: Option<PathBuf>,

    /// Number of concurrent jobs (default: number of logical CPUs)
    #[arg(short = 'j', long)]
    pub jobs: Option<usize>,
}
