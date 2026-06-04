# RevisionChest

RevisionChest is a high-performance Rust utility designed to extract and compress revision history from Wikipedia XML dumps. It supports `.bz2` and `.7z` compressed XML dumps and outputs the extracted data in a custom `.mwrev.zst` format using Zstandard compression.

## Features

- **Format Support**: Processes Wikipedia XML dumps compressed with `bzip2` (`.bz2`) or `7-Zip` (`.7z`). Note that bz2 has better performance due to support for streaming decompression.
- **Parallel Processing**: Efficiently processes multiple dump files in parallel using the `rayon` library.
- **Zstandard Compression**: Outputs revisions in a space-efficient `.zst` format.
- **Metadata Extraction**: Captures key revision metadata including page ID, namespace, revision ID, parent ID, timestamp, and contributor ID.

## Installation

Ensure you have [Rust and Cargo](https://rustup.rs/) installed. Clone the repository and build the project:

```bash
cargo build --release
```

The binary will be available at `target/release/RevisionChest`.

## Usage

RevisionChest can be used to process a single file or an entire directory of dump files.

### Processing a Single File

To process a single Wikipedia dump file and output to standard output:

```bash
./target/release/RevisionChest build <path_to_dump.xml.bz2_or_7z>
```

To process a single file and save the output to a specific directory:

```bash
./target/release/RevisionChest build <path_to_dump.xml.bz2> -o <output_directory>
```

### Processing an Entire Directory

To process all `.bz2` and `.7z` files in a directory in parallel:

```bash
./target/release/RevisionChest build -d <input_directory> -o <output_directory>
```

When using the `-d` (directory) flag, the `-o` (output directory) flag is **required**. Each input file will generate a corresponding `.mwrev.zst` file in the output directory.

## Synchronizing Recent Changes

The `sync` command allows you to fetch the most recent Wikipedia edits and update your local index and data files. It bridges the gap between static XML dumps and live data.

### Configuration

To comply with Wikipedia's API policy, you must define a User-Agent in a `.env` file in the project root. You can use `example.env` as a template:

```env
UA_APP_NAME=RevisionChest
UA_APP_VERSION=0.1.0
UA_CONTACT_INFO=https://github.com/yourusername/RevisionChest
```

### Usage

To sync recent changes for a specific domain:

```bash
./target/release/RevisionChest sync --domain en.wikipedia.org -o <output_directory>
```

**Key Features & Constraints:**
- **Automatic Resume**: The command looks up the most recent timestamp in your database and fetches edits starting from that point (minus 24 hours to ensure overlap).
- **30-Day Limit**: If the most recent revision in your database is older than 30 days, the sync will fail, as Wikipedia's Recent Changes API only retains data for 30 days.
- **Daily Rotation**: New revisions are stored in a `recentchanges/` subdirectory within your output folder, organized by date (e.g., `recentchanges/2026-05-26.mwrev.zst`).
- **Incremental Appends**: If a file for a specific day already exists, the tool appends new data as a new Zstandard frame, preserving the validity of the archive.
- **Namespace Filtering**: You can restrict the sync to specific namespaces:
  ```bash
  ./target/release/RevisionChest sync --domain en.wikipedia.org -o <output_directory> --namespace 0,118
  ```
- **Ongoing Sync**: Keep the tool running and poll for new changes periodically:
  ```bash
  ./target/release/RevisionChest sync --domain en.wikipedia.org -o <output_directory> --ongoing --interval 5
  ```
  The interval defaults to 10 minutes and can also be set via `SYNC_INTERVAL` in `.env`.

## Output Format

The output `.mwrev.zst` files contain revisions in the following format:

```text
# page_id=... ns=... rev_id=... parent_rev_id=... timestamp=... user_id=...
 <line of text>
 <line of text>
...
```

- Each revision starts with a header line beginning with `#`.
- The actual revision text follows, with each line prefixed by a single space.
- The output is compressed using Zstandard.

## Dependencies

- `bzip2`: For decompressing `.bz2` files.
- `sevenz-rust`: For decompressing `.7z` files.
- `quick-xml`: For fast XML parsing.
- `zstd`: For Zstandard compression.
- `rayon`: For parallel data processing.
- `clap`: For command-line argument parsing.
- `chrono`: For logging timestamps.
