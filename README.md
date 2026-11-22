# RevisionChest

RevisionChest turns MediaWiki XML dumps into compact `.mwrev.zst` revision
bundles — every revision's text, in order, zstd-compressed — and records
where each revision landed, so that a pipeline can read any revision's text
by `(bundle, offset, length)` without ever touching the XML again. It also
keeps bundles current from a wiki's recent changes.

It is the first stage of the [Wikipedia Citations Database](https://github.com/internetarchive/wiki-references-db)
pipeline, and is organised as reusable crates so that other tools can read
dumps and bundles the same way.

## Crates

| Crate | What it is |
|---|---|
| [`mwdump`](crates/mwdump) | Streaming reader for MediaWiki XML dumps (`.xml`, `.bz2`, `.gz`, `.7z` — all streamed, 7z included). One event per site-info block, page start, revision and page end; a revision carries page id, namespace, ids, timestamp, contributor, flags, comment, model, text and the dump's SHA-1. Nothing larger than one revision is held in memory. |
| [`mwrecent`](crates/mwrecent) | Following a wiki by `list=recentchanges`, without sending anything itself: the next query, and each batch read with a settle window near the present, a tail pass for late rows, deduplication by `rcid`, and log rows (deletions, moves, merges, suppression). Rows about one title are coalesced to the highest revision by default; `Coalesce::Off` keeps every one. Ported from Triplespace's follower. |
| [`mwrev`](crates/mwrev) | The `.mwrev.zst` bundle format: header line + space-prefixed text lines per revision. `BundleWriter` (create or append, with decompressed byte offsets), `BundleReader` (streams every frame back), `RevisionHeader` parse/format, `read_at` for tooling. |
| [`revisionchest-meta`](crates/revisionchest-meta) | Where the metadata goes: `MetaEvent` (site, page, revision location), the `MetaSink` trait, a `ParquetSink` (feature `parquet`, default) and a `PostgresSink` (feature `postgres`) that writes the WCD schema — `domains`, `revision_bundles`, `revisions`, `page_metadata`, article `web_resources`. |
| [`revisionchest`](crates/revisionchest) | The `RevisionChest` binary: `build` (dumps → bundles, in parallel over a directory) and `sync` (recent changes → daily bundles). |

The crates depend only downwards: `revisionchest` → `mwdump`, `mwrev`,
`revisionchest-meta`; the libraries are independent of each other.

`mwdump`, `mwrecent` and `mwrev` are published to crates.io. They carry
their own versions, starting at 0.1.0, which follow semver for each crate's
own API; their public structs and `mwdump`'s `Event` are non-exhaustive, so a
field or a kind of event can be added in a minor release.
The binary and `revisionchest-meta` are not published and share the
workspace version; "Changes from 0.1" below is about them.

## Building

Stable Rust ≥ 1.85 (edition 2024).

```bash
cargo build --release                      # binary at target/release/RevisionChest
cargo build --release --no-default-features -p revisionchest   # without Postgres support
cargo test --workspace
```

## Usage

### Building bundles from dumps

```bash
# one dump; the bundle text goes to stdout uncompressed
RevisionChest build enwiki-20260901-pages-meta-history1.xml-p1p857.bz2

# one dump into a directory → <dir>/enwiki-…-p1p857.mwrev.zst
RevisionChest build enwiki-…-p1p857.bz2 -o bundles/

# every .bz2/.7z/.gz/.xml in a directory, in parallel (-j N to limit)
RevisionChest build -d dumps/ -o bundles/ --parquet bundles/metadata.parquet
```

Options: `--namespace 0,118` keeps only those namespaces (other pages are
skipped inside the XML reader, their text never decoded); `--domain
en.wikipedia.org` overrides the domain read from the dump's `<siteinfo>`
(needed for dumps without one).

### Where the metadata goes

Bundles alone are not enough to find a revision; `build` and `sync` record
`(page_id, rev_id, parent, timestamp, bundle, offset_begin, length)` and the
page titles and namespaces to one or both of:

- `--parquet FILE` — one Parquet file with a `type` column (`page` /
  `revision`); for `sync`, pass a directory and each run writes its own
  file there.
- `--postgres` — the WCD Postgres schema. Connection from `DATABASE_URL`,
  or `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASS` (a `.env` file is
  read). `DATABASE=postgres` in the environment also turns it on. The
  tables must already exist (`wcd init-db` in the monorepo).

With neither, only the bundles are written. (There is no SQLite index any
more; `--no-db` is accepted and ignored.)

### Synchronizing recent changes

```bash
RevisionChest sync --domain en.wikipedia.org -o bundles/ --namespace 0 --parquet bundles/sync/
RevisionChest sync --domain en.wikipedia.org -o bundles/ --postgres --ongoing --interval 5
```

`sync` follows the wiki's recent changes with `mwrecent`, fetches the content
of revisions it has not recorded yet, and appends them to
`bundles/recentchanges/<YYYY-MM-DD>.mwrev.zst` (one new zstd frame per batch).
Each kind of error has its own mechanism:

- **Changes committed late.** Near the present (`--settle` seconds, default
  120) recent changes are read by time from ten seconds before the position,
  and once caught up a tail pass re-reads the last `--tail` seconds (default
  900). Rows already seen are dropped by `rcid`, so nothing is fetched twice.
- **Restarts.** A cycle's bundle frames, its metadata and the poller's
  position commit together: `recentchanges/position.json` holds the poller's
  time, continuation and seen `rcid`s, each bundle's committed length, and,
  while a cycle runs, the Parquet file it writes. A cycle that did not commit
  is rolled forward on the next start: the revisions already appended are
  reported again from the bundles (not fetched again), a frame cut off part
  way is cut back, and the cycle's unfinished Parquet file is removed. A sync
  with no position starts at `--since`, else `--tail` seconds before the
  newest recorded revision (what is already recorded is not fetched again),
  else 24 hours ago. `--overlap` is gone. A cycle still behind after
  `--commit-every` minutes (default 10; 0 for never) stops after the batch in
  hand and commits, and the next continues from the committed continuation,
  so a restart during a long catch-up resumes from its last commit instead
  of re-reading the backlog. Metadata written to a single `--parquet` file
  keeps one cycle, since the next would overwrite it.
- **Gaps.** A wiki keeps recent changes for `--rc-max-age` days (30 on
  Wikimedia wikis, MediaWiki's default 90). A position older than that, and
  older than the oldest change the wiki still has, cannot be resumed without
  missing revisions: `sync` refuses to start and says so. `--accept-gap`
  resumes from the oldest change and appends the gap's bounds to
  `recentchanges/gaps.jsonl`; rebuilding that window from a dump closes it.
- **Deletions and hidden revisions.** Log events (deletions, restorations,
  revision-visibility changes, suppressions, moves, merges) are recorded as
  they come: `log` rows in the Parquet metadata, the `log_events` table in
  Postgres. Bundles are never rewritten; a reader derives what is deleted or
  hidden from the events (a `delete`/`revision` event lists its revisions in
  `log_params`), and a later restoration lifts it.

API responses that do not decode are retried with exponential backoff
(`--max-retries`, default 15). With `--postgres`, revisions already in the
database are skipped as well.

Each cycle (one poll, or every poll of an `--ongoing` sync) writes its
metadata through its own sink, finished before the next poll: with a
`--parquet` directory that is one closed, readable
`revisionchest-<time>.parquet` per cycle, so metadata is never held in a file
that is only completed when the process exits. `--ongoing` therefore needs
`--parquet` to be an existing directory, not a single file.

Set a User-Agent contact for the Wikimedia API policy: `UA_CONTACT_INFO`
(or `CONTACT_EMAIL`), optionally `UA_APP_NAME` / `UA_APP_VERSION`, in the
environment or `.env`.

## The bundle format

```text
# page_id=101 ns=0 rev_id=1001 parent_rev_id= timestamp=2004-01-01T00:00:00Z user_id=7
 '''Example Island''' is an island.
 
 == References ==
# page_id=101 ns=0 rev_id=1002 parent_rev_id=1001 timestamp=2005-06-15T12:00:00Z user_id=0
 …
```

`parent_rev_id` is empty for a first revision; `user_id` is `0` for
anonymous or hidden contributors. A revision whose text the wiki has hidden
(revision deletion or suppression) is written with no text lines and a
trailing ` text_hidden=1` on its header, so a reader can tell "hidden" from
"blank"; other headers do not carry the key, and readers ignore keys they do
not know. Text is written line by line, so a
trailing newline is not kept and `\r\n` becomes `\n`.

**Offsets** are decompressed byte offsets from the start of the bundle's
content, across all of its zstd frames: `offset_begin` points at the first
space-prefixed text line and `length` covers the text lines. A bundle that
`sync` appends to keeps a `<bundle>.len` sidecar with its decompressed
length so the next append continues the count; if the sidecar is missing
the bundle is measured once.

Reading a revision back therefore means decompressing the bundle up to
`offset_begin` — sequential, as the pipeline consumes bundles — rather than
seeking; `mwrev::read_at` does that for tooling.

**Compression.** Frames are zstd at the default level with long-distance
matching and a 128 MiB window (`mwrev::WINDOW_LOG` = 27), so a revision can
reuse its page's earlier revisions even when they run to megabytes; on
enwiki history that roughly halves the bundles. 128 MiB is zstd's default
decoder limit, so any zstd reader (`zstd -d`, python-zstandard, the `zstd`
crate) reads a bundle with default settings, and older bundles (2 MiB
window) read the same way. Budget up to 128 MiB per open encoder or decoder.

## Changes from 0.1

- Bundles are written with zstd long-distance matching and a 128 MiB window
  (about half the size on enwiki history). Readers need no change: 128 MiB
  is zstd's default decoder limit, and 0.1 readers read new bundles.

- A revision whose text the wiki has hidden carries ` text_hidden=1` on its
  bundle header (`RevisionHeader::text_hidden`); it used to be
  indistinguishable from a blank revision. Bundles are otherwise unchanged.
  The marker is 14 bytes, so in a bundle rebuilt from the same dump the
  text of that revision and of every later one starts 14 bytes further on,
  and `offset_begin` in the metadata moves with it. Bundles and metadata
  from one build agree; do not pair metadata from a 0.1 build with bundles
  rebuilt by this one, or the reverse.
- Split into four crates; the binary's command line is unchanged except
  that `--db` (the SQLite index) is gone and `--postgres` is explicit.
- `.7z` dumps are streamed; previously the whole decompressed dump was read
  into memory first.
- `.gz` dumps are accepted.
- Revision text is taken verbatim; leading and trailing whitespace of a
  revision is no longer trimmed.
- `sync` offsets were wrong (a compressed file size added to an
  uncompressed in-frame offset); they are now decompressed offsets like
  `build`'s. Bundles written by `build` are byte-for-byte the same as before.
- The metadata file name for a plain `.xml` dump now matches the bundle
  actually written (`fixture.mwrev.zst`, not `fixture.xml.mwrev.zst`).
- Postgres: `revisions.revision_timestamp` is written as `TIMESTAMP`,
  `revisions.domain_id` is set, page titles and namespaces go to
  `page_metadata (domain_id, page_id)`, and nothing is written to
  `documents`.
- A truncated dump is an error rather than a silent end of input.
