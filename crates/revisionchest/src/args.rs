use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "RevisionChest", author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Build .mwrev.zst bundles from MediaWiki XML dumps
    Build(BuildArgs),
    /// Append recent changes fetched from the site's API to daily bundles
    Sync(SyncArgs),
}

/// Where page and revision metadata goes. Without any of these, only the
/// bundles are written.
#[derive(Parser, Debug, Clone)]
pub struct MetaArgs {
    /// Write metadata to this Parquet file (for `build`), or into this
    /// directory as one file per run (for `sync`, when the path is a directory)
    #[arg(long)]
    pub parquet: Option<PathBuf>,

    /// Write metadata into the Wikipedia Citations Database Postgres schema
    /// (connection from DATABASE_URL, or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS).
    /// Also enabled by the historical `DATABASE=postgres` environment variable.
    #[arg(long)]
    pub postgres: bool,

    /// Accepted for compatibility; metadata is only written where asked for.
    #[arg(long, hide = true)]
    pub no_db: bool,
}

#[derive(Parser, Debug)]
pub struct BuildArgs {
    /// Dump file (.xml, .xml.bz2, .xml.gz or .xml.7z); bundle text goes to
    /// stdout uncompressed unless -o is given
    pub input: Option<PathBuf>,

    /// Directory of dump files, processed in parallel (requires -o)
    #[arg(short = 'd')]
    pub input_dir: Option<PathBuf>,

    /// Output directory for .mwrev.zst bundles
    #[arg(short = 'o')]
    pub output_dir: Option<PathBuf>,

    /// Namespaces to include, comma-separated (e.g. --namespace=0,118); default all
    #[arg(long, value_delimiter = ',')]
    pub namespace: Option<Vec<i64>>,

    /// The site's domain (e.g. en.wikipedia.org); default: taken from the dump's <siteinfo>
    #[arg(long)]
    pub domain: Option<String>,

    /// Number of dump files processed concurrently (default: logical CPUs)
    #[arg(short = 'j', long)]
    pub jobs: Option<usize>,

    #[command(flatten)]
    pub meta: MetaArgs,
}

#[derive(Parser, Debug)]
pub struct SyncArgs {
    /// Output directory; bundles land in <dir>/recentchanges/<date>.mwrev.zst
    #[arg(short = 'o')]
    pub output_dir: PathBuf,

    /// Namespaces to include, comma-separated (e.g. --namespace=0,118); default all
    #[arg(long, value_delimiter = ',')]
    pub namespace: Option<Vec<i64>>,

    /// The site's domain (e.g. en.wikipedia.org)
    #[arg(long)]
    pub domain: String,

    /// Keep polling recent changes
    #[arg(long)]
    pub ongoing: bool,

    /// Minutes between polls when --ongoing (default: 10, or SYNC_INTERVAL)
    #[arg(long)]
    pub interval: Option<u64>,

    /// Start from this timestamp (YYYY-MM-DDTHH:MM:SSZ) instead of the latest
    /// recorded revision minus 24 hours
    #[arg(long)]
    pub since: Option<String>,

    #[command(flatten)]
    pub meta: MetaArgs,
}
