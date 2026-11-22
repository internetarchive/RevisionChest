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
    /// directory as one file per run (for `sync`, when the path is a directory;
    /// `sync --ongoing` writes one file per cycle and requires a directory)
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

    /// Seconds within which the newest changes are read by time, backed off
    /// ten seconds, since a wiki can commit a change late (default: 120)
    #[arg(long)]
    pub settle: Option<u64>,

    /// Seconds behind the newest change the tail pass re-reads once caught
    /// up, for changes committed late; also how far before the newest
    /// recorded revision a sync without a position starts (default: 900; 0
    /// turns the tail pass off)
    #[arg(long)]
    pub tail: Option<u64>,

    /// Resume even when the position is older than the oldest change the
    /// wiki still has: start from that change and record the gap in
    /// recentchanges/gaps.jsonl (without it, sync refuses to start)
    #[arg(long)]
    pub accept_gap: bool,

    /// Days the wiki keeps recent changes ($wgRCMaxAge): 30 on Wikimedia
    /// wikis, MediaWiki's default 90. A position within this window of now
    /// is never a gap, however old the wiki's oldest change (default: 30)
    #[arg(long, default_value = "30")]
    pub rc_max_age: u64,

    /// Replaced by the committed position and --tail; accepted and ignored
    #[arg(long, hide = true)]
    pub overlap: Option<u64>,

    /// Retries, with exponential backoff, when an API response cannot be
    /// decoded (default: 15)
    #[arg(long, default_value = "15")]
    pub max_retries: u32,

    /// Minutes after which a cycle that has not caught up stops, commits what
    /// it has fetched, and the next cycle continues from the committed
    /// position, so a restart during a long catch-up resumes from there
    /// instead of re-reading the backlog (default: 10; 0 commits only once
    /// caught up; a single --parquet file always keeps one cycle, which the
    /// next would overwrite)
    #[arg(long, default_value = "10")]
    pub commit_every: u64,

    /// Start from this timestamp (YYYY-MM-DDTHH:MM:SSZ) when there is no
    /// committed position (recentchanges/position.json), instead of the latest
    /// recorded revision less --tail
    #[arg(long)]
    pub since: Option<String>,

    #[command(flatten)]
    pub meta: MetaArgs,
}
