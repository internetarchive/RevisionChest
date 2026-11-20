//! Round trips over real dumps, read, written again and read back to the same events.
//! The dumps are Triplespace's `docs/samples` (Librarybase, English Wikipedia talk pages),
//! which are CC BY-SA and so are not copied here: set `MWDUMP_SAMPLES` to that directory
//! to run these, or they pass without reading anything.

use std::path::PathBuf;

use mwdump::{Event, Writer, open};

fn sample(path: &str) -> Option<PathBuf> {
    let Some(dir) = std::env::var_os("MWDUMP_SAMPLES") else {
        eprintln!("MWDUMP_SAMPLES is not set: skipping {path}");
        return None;
    };
    Some(PathBuf::from(dir).join(path))
}

fn round_trip(path: &str, pages_expected: usize, revisions_expected: usize) {
    let Some(file) = sample(path) else { return };
    let events: Vec<Event> = open(&file)
        .unwrap_or_else(|e| panic!("{path}: {e}"))
        .map(|e| e.unwrap())
        .collect();
    let pages = events
        .iter()
        .filter(|e| matches!(e, Event::PageStart(_)))
        .count();
    let revisions = events
        .iter()
        .filter(|e| matches!(e, Event::Revision(_)))
        .count();
    assert_eq!(
        (pages, revisions),
        (pages_expected, revisions_expected),
        "{path}"
    );

    let mut w = Writer::new(Vec::new());
    for e in &events {
        match e {
            Event::SiteInfo(si) => w.start(si).unwrap(),
            Event::PageStart(p) => w.open_page(p).unwrap(),
            Event::Revision(r) => w.revision(r).unwrap(),
            Event::PageEnd { .. } => w.close_page().unwrap(),
            other => panic!("unexpected event {other:?}"),
        }
    }
    let written = w.finish().unwrap();
    let back: Vec<Event> = mwdump::DumpReader::new(written.as_slice())
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(back.len(), events.len(), "{path}");
    for (a, b) in events.iter().zip(&back) {
        assert_eq!(a, b, "{path}");
    }
}

#[test]
fn librarybase_wikitext_pages_round_trip() {
    round_trip("librarybase/dump/librarybase-wikitext.xml.gz", 39, 147);
}

#[test]
fn librarybase_entities_round_trip() {
    round_trip("librarybase/dump/librarybase-sample.xml.gz", 254, 295);
}

#[test]
fn enwiki_talk_current_round_trips() {
    round_trip("enwiki-talk/current.xml.bz2", 9, 9);
}

#[test]
fn enwiki_talk_history_round_trips() {
    round_trip("enwiki-talk/history.xml.bz2", 1, 4677);
}
