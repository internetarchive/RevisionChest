//! Reads tests/fixtures/fixture.xml (and its compressed copies) and compares
//! every revision with expected.json, which tests/fixtures/make_dump.py wrote
//! alongside the XML.

use std::path::{Path, PathBuf};

use mwdump::{Event, open};
use serde_json::Value;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn read_all(name: &str) -> Vec<Event> {
    open(&fixture(name)).unwrap().map(|e| e.unwrap()).collect()
}

fn check(events: &[Event]) {
    let expected: Vec<Value> =
        serde_json::from_str(&std::fs::read_to_string(fixture("expected.json")).unwrap()).unwrap();

    // siteinfo first
    let Event::SiteInfo(si) = &events[0] else {
        panic!("first event is not SiteInfo: {:?}", events[0])
    };
    assert_eq!(si.dbname.as_deref(), Some("enwiki"));
    assert_eq!(si.domain().as_deref(), Some("en.wikipedia.org"));
    assert_eq!(si.lang.as_deref(), Some("en"));
    assert_eq!(si.namespaces.len(), 3);
    assert_eq!(si.namespaces[0].key, 0);
    assert_eq!(si.namespaces[0].name, "");
    assert_eq!(si.namespaces[2].name, "Draft");

    // page structure: every revision sits between its page's start and end
    let mut current: Option<u64> = None;
    let mut pages = 0;
    let mut revisions = Vec::new();
    for e in &events[1..] {
        match e {
            Event::PageStart(p) => {
                assert!(current.is_none());
                current = Some(p.id);
                pages += 1;
                if p.id == 103 {
                    assert_eq!(p.redirect.as_deref(), Some("Example Island"));
                    assert_eq!(p.title, "Redirect Page");
                } else {
                    assert!(p.redirect.is_none());
                }
            }
            Event::Revision(r) => {
                assert_eq!(Some(r.page_id), current);
                revisions.push(r.clone());
            }
            Event::PageEnd { id } => {
                assert_eq!(Some(*id), current);
                current = None;
            }
            Event::SiteInfo(_) => panic!("second siteinfo"),
        }
    }
    assert!(current.is_none());
    assert_eq!(pages, 4);
    assert_eq!(revisions.len(), expected.len());

    for (r, x) in revisions.iter().zip(&expected) {
        let ctx = format!("rev {}", x["rev_id"]);
        assert_eq!(r.page_id, x["page_id"].as_u64().unwrap(), "{ctx}");
        assert_eq!(r.ns, x["ns"].as_i64().unwrap(), "{ctx}");
        assert_eq!(r.id, x["rev_id"].as_u64().unwrap(), "{ctx}");
        assert_eq!(r.parent_id, x["parent"].as_u64(), "{ctx}");
        assert_eq!(r.timestamp, x["timestamp"].as_str().unwrap(), "{ctx}");
        assert_eq!(r.user_id, x["user_id"].as_u64(), "{ctx}");
        assert_eq!(r.user_name.as_deref(), x["user_name"].as_str(), "{ctx}");
        assert_eq!(r.ip.as_deref(), x["ip"].as_str(), "{ctx}");
        assert_eq!(r.user_hidden, x["user_hidden"].as_bool().unwrap(), "{ctx}");
        assert_eq!(r.minor, x["minor"].as_bool().unwrap(), "{ctx}");
        assert_eq!(r.comment.as_deref(), x["comment"].as_str(), "{ctx}");
        assert_eq!(r.text_hidden, x["text_deleted"].as_bool().unwrap(), "{ctx}");
        assert_eq!(r.text, x["text"].as_str().unwrap(), "{ctx}");
        assert_eq!(r.model.as_deref(), Some("wikitext"), "{ctx}");
        if r.text_hidden {
            assert!(r.sha1.is_none(), "{ctx}");
        } else {
            assert_eq!(
                r.sha1.as_deref(),
                Some("phoiac9h4m842xq45sp7s6u21eteeq1"),
                "{ctx}"
            );
            assert_eq!(r.text_bytes, Some(r.text.len() as u64), "{ctx}");
        }
    }
}

#[test]
fn plain_xml() {
    check(&read_all("fixture.xml"));
}

#[test]
fn bzip2() {
    check(&read_all("fixture.xml.bz2"));
}

#[test]
fn gzip() {
    check(&read_all("fixture.xml.gz"));
}

#[test]
fn seven_zip_streams() {
    check(&read_all("fixture.xml.7z"));
}

#[test]
fn all_formats_agree() {
    let a = read_all("fixture.xml");
    for f in ["fixture.xml.bz2", "fixture.xml.gz", "fixture.xml.7z"] {
        assert_eq!(a, read_all(f), "{f}");
    }
}

/// What a namespace filter must produce: the unfiltered events minus every
/// event of a page outside `keep`.
fn filtered_by_hand(events: &[Event], keep: &[i64]) -> Vec<Event> {
    let mut out = Vec::new();
    let mut in_kept_page = false;
    for e in events {
        match e {
            Event::SiteInfo(_) => out.push(e.clone()),
            Event::PageStart(p) => {
                in_kept_page = keep.contains(&p.ns);
                if in_kept_page {
                    out.push(e.clone());
                }
            }
            Event::Revision(_) | Event::PageEnd { .. } => {
                if in_kept_page {
                    out.push(e.clone());
                }
            }
        }
    }
    out
}

fn read_filtered(name: &str, keep: &[i64]) -> Vec<Event> {
    open(&fixture(name))
        .unwrap()
        .with_namespaces(keep)
        .map(|e| e.unwrap())
        .collect()
}

#[test]
fn namespace_filter_drops_whole_pages_in_every_format() {
    let all = read_all("fixture.xml");
    for keep in [&[0][..], &[1], &[0, 118], &[118], &[], &[0, 1, 118]] {
        let expected = filtered_by_hand(&all, keep);
        for f in [
            "fixture.xml",
            "fixture.xml.bz2",
            "fixture.xml.gz",
            "fixture.xml.7z",
        ] {
            assert_eq!(read_filtered(f, keep), expected, "{f} keep={keep:?}");
        }
    }
    // The fixture has pages in 0, 1 and 118, so [0] really removes something.
    assert!(filtered_by_hand(&all, &[0]).len() < all.len());
}

#[test]
fn namespace_filter_still_reports_a_dump_cut_inside_a_skipped_page() {
    let xml = "<mediawiki><page><title>Talk:X</title><ns>1</ns><id>5</id><revision><id>1</id><text>unclosed";
    let results: Vec<_> = mwdump::DumpReader::new(std::io::Cursor::new(xml))
        .with_namespaces(&[0])
        .collect();
    assert!(results.iter().any(|r| r.is_err()), "{results:?}");
}

#[test]
fn namespace_filter_resumes_after_a_skipped_page() {
    let xml = r#"<mediawiki><page><title>Talk:A</title><ns>1</ns><id>1</id><revision><id>10</id><text>a &amp; &lt;b&gt;</text></revision></page><page><title>B</title><ns>0</ns><id>2</id><revision><id>20</id><text>b</text></revision></page></mediawiki>"#;
    let events: Vec<Event> = mwdump::DumpReader::new(std::io::Cursor::new(xml))
        .with_namespaces(&[0])
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(events.len(), 3, "{events:?}");
    assert!(matches!(&events[0], Event::PageStart(p) if p.id == 2 && p.title == "B"));
    assert!(matches!(&events[1], Event::Revision(r) if r.id == 20 && r.text == "b"));
    assert!(matches!(&events[2], Event::PageEnd { id: 2 }));
}

#[test]
fn page_without_revisions_still_starts_and_ends() {
    let xml = r#"<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/"><page><title>Empty</title><ns>0</ns><id>5</id></page></mediawiki>"#;
    let events: Vec<Event> = mwdump::DumpReader::new(std::io::Cursor::new(xml))
        .map(|e| e.unwrap())
        .collect();
    assert!(matches!(&events[0], Event::PageStart(p) if p.id == 5 && p.title == "Empty"));
    assert!(matches!(&events[1], Event::PageEnd { id: 5 }));
    assert_eq!(events.len(), 2);
}

#[test]
fn malformed_xml_is_an_error_not_a_panic() {
    let xml = "<mediawiki><page><title>Broken</title><ns>0</ns><id>5</id><revision><id>1</id><text>unclosed";
    let results: Vec<_> = mwdump::DumpReader::new(std::io::Cursor::new(xml)).collect();
    assert!(results.iter().any(|r| r.is_err()));
}
