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
| [`mwrev`](crates/mwrev) | The `.mwrev.zst` bundle format: header line + space-prefixed text lines per revision. `BundleWriter` (create or append, with decompressed byte offsets), `BundleReader` (streams every frame back), `RevisionHeader` parse/format, `read_at` for tooling. |
| [`revisionchest-meta`](crates/revisionchest-meta) | Where the metadata goes: `MetaEvent` (site, page, revision location), the `MetaSink` trait, a `ParquetSink` (feature `parquet`, default) and a `PostgresSink` (feature `postgres`) that writes the WCD schema — `domains`, `revision_bundles`, `revisions`, `page_metadata`, article `web_resources`. |
| [`revisionchest`](crates/revisionchest) | The `RevisionChest` binary: `build` (dumps → bundles, in parallel over a directory) and `sync` (recent changes → daily bundles). |

The crates depend only downwards: `revisionchest` → `mwdump`, `mwrev`,
`revisionchest-meta`; the three libraries are independent of each other.

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

Options: `--namespace 0,118` keeps only those namespaces; `--domain
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

`sync` polls `list=recentchanges`, fetches the content of revisions it has
not recorded yet, and appends them to `bundles/recentchanges/<YYYY-MM-DD>.mwrev.zst`
(one new zstd frame per batch). It resumes from the latest recorded
revision timestamp minus 24 hours — read from Postgres, or from the Parquet
file(s) — or from `--since`; with nothing recorded it starts 24 hours ago.
Recent changes only reach back 30 days, so a store older than that must be
rebuilt from a dump. With `--postgres`, revisions already in the database are
skipped; otherwise an in-memory cache deduplicates within a run.

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
anonymous or hidden contributors. Text is written line by line, so a
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

## Changes from 0.1

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
