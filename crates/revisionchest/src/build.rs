//! `build`: one dump file → one bundle, in parallel over a directory.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use mwdump::{Event, open};
use mwrev::{BundleWriter, RevisionHeader};
use revisionchest_meta::{MetaEvent, MetaSender};

use crate::log;

/// The bundle file name for a dump: one trailing extension is dropped —
/// the compression suffix, or `.xml` for an uncompressed dump — and
/// `.mwrev.zst` is appended (`enwiki-…-pages-meta-history1.xml-p1p857.bz2`
/// → `…-p1p857.mwrev.zst`, `rmwiki-…-pages-meta-history.xml.bz2` →
/// `rmwiki-…-pages-meta-history.xml.mwrev.zst`, `fixture.xml` →
/// `fixture.mwrev.zst`). This is the naming every existing bundle
/// directory already has.
pub fn bundle_name(input: &Path) -> String {
    let name = input
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let stem = ["bz2", "7z", "gz", "xml"]
        .iter()
        .find_map(|ext| name.strip_suffix(&format!(".{ext}")))
        .unwrap_or(&name);
    format!("{stem}.mwrev.zst")
}

/// Processes one dump file. With `output_dir` the bundle is written there
/// (zstd); without, the bundle text goes to stdout uncompressed.
pub fn build_file(
    input: &Path,
    output_dir: Option<&Path>,
    namespaces: Option<&[i64]>,
    domain_override: Option<&str>,
    meta: &MetaSender,
    progress: (usize, usize),
) -> io::Result<()> {
    let file_name = input
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    log(&format!(
        "Starting {} ({}/{})",
        file_name, progress.0, progress.1
    ));

    let bundle = bundle_name(input);
    let mut writer: BundleWriter<Box<dyn io::Write + Send>> = match output_dir {
        Some(dir) => {
            fs::create_dir_all(dir)?;
            let file: Box<dyn io::Write + Send> = Box::new(fs::File::create(dir.join(&bundle))?);
            BundleWriter::new(file)?
        }
        None => BundleWriter::plain(Box::new(io::stdout())),
    };

    let allowed = |ns: i64| namespaces.is_none_or(|list| list.contains(&ns));
    let mut n_revisions = 0u64;

    for event in open(input).map_err(io::Error::other)? {
        match event.map_err(io::Error::other)? {
            Event::SiteInfo(si) => {
                if domain_override.is_none() {
                    match si.domain() {
                        Some(domain) => meta.send(MetaEvent::SiteInfo {
                            domain,
                            language_code: si.lang.clone(),
                        }),
                        None => eprintln!(
                            "warning: {file_name}: no <base> in <siteinfo>; pass --domain"
                        ),
                    }
                }
            }
            Event::PageStart(p) => {
                if allowed(p.ns) {
                    meta.send(MetaEvent::Page {
                        id: p.id,
                        ns: p.ns,
                        title: p.title,
                    });
                }
            }
            Event::Revision(r) => {
                if !allowed(r.ns) {
                    continue;
                }
                let header = RevisionHeader {
                    page_id: r.page_id,
                    ns: r.ns,
                    rev_id: r.id,
                    parent_rev_id: r.parent_id,
                    timestamp: r.timestamp.clone(),
                    user_id: r.user_id.unwrap_or(0),
                };
                let loc = writer.write_revision(&header, &r.text)?;
                n_revisions += 1;
                meta.send(MetaEvent::Revision {
                    rev_id: r.id,
                    parent_rev_id: r.parent_id,
                    page_id: r.page_id,
                    file_path: bundle.clone(),
                    offset_begin: loc.offset_begin,
                    length: loc.length,
                    timestamp: r.timestamp,
                });
            }
            Event::PageEnd { .. } => {}
        }
    }
    writer.finish()?;
    meta.send(MetaEvent::Finalize);
    log(&format!(
        "Finished {} ({}/{}): {} revisions → {}",
        file_name, progress.0, progress.1, n_revisions, bundle
    ));
    Ok(())
}

/// Dump files in a directory, sorted by name.
pub fn list_dumps(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name().is_some_and(|n| {
                    let n = n.to_string_lossy();
                    n.ends_with(".bz2")
                        || n.ends_with(".7z")
                        || n.ends_with(".gz")
                        || n.ends_with(".xml")
                })
        })
        .collect();
    entries.sort();
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::bundle_name;
    use std::path::Path;

    #[test]
    fn names() {
        assert_eq!(
            bundle_name(Path::new(
                "/x/enwiki-20240101-pages-meta-history1.xml-p1p857.bz2"
            )),
            "enwiki-20240101-pages-meta-history1.xml-p1p857.mwrev.zst"
        );
        assert_eq!(bundle_name(Path::new("a.xml.7z")), "a.xml.mwrev.zst");
        assert_eq!(bundle_name(Path::new("a.xml.gz")), "a.xml.mwrev.zst");
        assert_eq!(bundle_name(Path::new("a.xml.bz2")), "a.xml.mwrev.zst");
        assert_eq!(bundle_name(Path::new("a.xml")), "a.mwrev.zst");
        assert_eq!(bundle_name(Path::new("a")), "a.mwrev.zst");
    }
}
