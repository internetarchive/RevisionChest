//! A schema 0.11 dump with what only some dumps carry: a further content slot, a hidden
//! comment and contributor, hidden text with an empty `<sha1 />`, an IP contributor and a
//! redirect.

use mwdump::{DumpReader, Event, PageHeader, Revision};

const SAMPLE: &str = r#"<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.11/" version="0.11" xml:lang="en">
  <siteinfo>
    <sitename>Librarybase</sitename>
    <dbname>librarybasewiki</dbname>
    <base>https://librarybase.org/wiki/Project:Home</base>
    <generator>MediaWiki 1.43.9</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="0" case="first-letter" />
      <namespace key="120" case="first-letter">Item</namespace>
    </namespaces>
  </siteinfo>
  <page>
    <title>Item:Q6</title>
    <ns>120</ns>
    <id>12</id>
    <revision>
      <id>41877</id>
      <parentid>41000</parentid>
      <timestamp>2026-09-20T14:02:11Z</timestamp>
      <contributor><username>Bob</username><id>9</id></contributor>
      <minor/>
      <comment>/* wbsetlabel-add:1|en */ Six</comment>
      <origin>41877</origin>
      <model>wikibase-item</model>
      <format>application/json</format>
      <text bytes="40" xml:space="preserve">{"labels":{"en":{"value":"Six &amp; more"}}}</text>
      <sha1>abc</sha1>
      <content>
        <role>notes</role>
        <origin>41876</origin>
        <model>wikitext</model>
        <format>text/x-wiki</format>
        <text bytes="5" xml:space="preserve">Hello</text>
        <sha1>def</sha1>
      </content>
    </revision>
  </page>
  <page>
    <title>Item:Q12</title>
    <ns>120</ns>
    <id>40</id>
    <redirect title="Item:Q6" />
    <revision>
      <id>903</id>
      <timestamp>2024-01-03T09:15:40Z</timestamp>
      <contributor deleted="deleted" />
      <comment deleted="deleted" />
      <model>wikibase-item</model>
      <format>application/json</format>
      <text xml:space="preserve">{}</text>
    </revision>
    <revision>
      <id>904</id>
      <timestamp>2024-01-04T00:00:00Z</timestamp>
      <contributor><ip>127.0.0.1</ip></contributor>
      <model>wikibase-item</model>
      <format>application/json</format>
      <text bytes="5" deleted="deleted" />
      <sha1 />
    </revision>
  </page>
</mediawiki>
"#;

fn read() -> Vec<Event> {
    DumpReader::new(SAMPLE.as_bytes())
        .map(Result::unwrap)
        .collect()
}

fn revisions(events: &[Event]) -> Vec<&Revision> {
    events
        .iter()
        .filter_map(|e| match e {
            Event::Revision(r) => Some(r),
            _ => None,
        })
        .collect()
}

#[test]
fn siteinfo_carries_case() {
    let Event::SiteInfo(si) = &read()[0] else {
        panic!("no siteinfo")
    };
    assert_eq!(si.case.as_deref(), Some("first-letter"));
    assert_eq!(si.namespaces[1].case.as_deref(), Some("first-letter"));
    assert_eq!(si.namespaces[1].name, "Item");
}

#[test]
fn a_further_slot_is_read_and_leaves_the_main_slot_alone() {
    let events = read();
    let r = revisions(&events)[0];
    assert_eq!(r.origin, Some(41877));
    assert_eq!(r.text, r#"{"labels":{"en":{"value":"Six & more"}}}"#);
    assert_eq!(r.text_bytes, Some(40));
    assert_eq!(r.sha1.as_deref(), Some("abc"));
    assert_eq!(r.model.as_deref(), Some("wikibase-item"));
    assert_eq!(r.slots.len(), 1);
    let s = &r.slots[0];
    assert_eq!(s.role, "notes");
    assert_eq!(s.origin, Some(41876));
    assert_eq!(s.model.as_deref(), Some("wikitext"));
    assert_eq!(s.format.as_deref(), Some("text/x-wiki"));
    assert_eq!(s.text, "Hello");
    assert_eq!(s.text_bytes, Some(5));
    assert_eq!(s.sha1.as_deref(), Some("def"));
    assert!(!s.text_hidden);
}

#[test]
fn hidden_parts_are_flagged() {
    let events = read();
    let rs = revisions(&events);
    let hidden = rs[1];
    assert!(hidden.user_hidden && hidden.comment_hidden && !hidden.text_hidden);
    assert_eq!(hidden.comment, None);
    assert!(rs[0].slots.iter().all(|s| !s.text_hidden));
    let text_hidden = rs[2];
    assert!(text_hidden.text_hidden && !text_hidden.comment_hidden);
    assert_eq!(text_hidden.ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(text_hidden.text, "");
    assert_eq!(text_hidden.sha1, None, "an empty <sha1 /> is none");
    assert!(events.contains(&Event::PageStart(PageHeader {
        id: 40,
        ns: 120,
        title: "Item:Q12".into(),
        redirect: Some("Item:Q6".into()),
    })));
}
