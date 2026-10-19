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
