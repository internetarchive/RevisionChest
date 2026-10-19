//! Runs the Postgres sink against a scratch database. Skipped unless
//! `REVISIONCHEST_TEST_DATABASE_URL` is set; the database's public schema is
//! reset from tests/schema.sql on every run.
#![cfg(feature = "postgres")]

use postgres::{Client, NoTls};
use revisionchest_meta::postgres::{
    PostgresConfig, PostgresSink, existing_revisions, latest_timestamp,
};
use revisionchest_meta::{MetaEvent, MetaSink};

fn config() -> Option<PostgresConfig> {
    let url = std::env::var("REVISIONCHEST_TEST_DATABASE_URL").ok()?;
    Some(PostgresConfig {
        url,
        workers: 2,
        batch_size: 3,
    })
}

fn reset(cfg: &PostgresConfig) {
    let mut c = Client::connect(&cfg.url, NoTls).unwrap();
    c.batch_execute("DROP TABLE IF EXISTS revisions, page_metadata, web_resources, revision_bundles, domains CASCADE").unwrap();
    c.batch_execute(include_str!("schema.sql")).unwrap();
}

fn events() -> Vec<MetaEvent> {
    let rev = |rev_id, parent, page_id, off, len, ts: &str| MetaEvent::Revision {
        rev_id,
        parent_rev_id: parent,
        page_id,
        file_path: "fixture.xml.mwrev.zst".into(),
        offset_begin: off,
        length: len,
        timestamp: ts.into(),
    };
    vec![
        MetaEvent::SiteInfo {
            domain: "en.wikipedia.org".into(),
            language_code: Some("en".into()),
        },
        MetaEvent::Page {
            id: 101,
            ns: 0,
            title: "Example Island".into(),
        },
        rev(1001, None, 101, 87, 132, "2004-01-01T00:00:00Z"),
        rev(1002, Some(1001), 101, 310, 95, "2005-06-15T12:00:00Z"),
        MetaEvent::Page {
            id: 102,
            ns: 1,
            title: "Talk:Example Island".into(),
        },
        rev(2001, None, 102, 893, 59, "2006-02-02T00:00:00Z"),
        MetaEvent::Page {
            id: 104,
            ns: 118,
            title: "Draft:Tab\tand\\backslash".into(),
        },
        rev(4001, None, 104, 1160, 52, "2020-05-05T05:05:05Z"),
        MetaEvent::Finalize,
    ]
}

fn run(cfg: &PostgresConfig) {
    let mut sink: Box<dyn MetaSink> = Box::new(PostgresSink::connect(cfg).unwrap());
    for e in events() {
        sink.handle(e).unwrap();
    }
    sink.finish().unwrap();
}

#[test]
fn writes_every_table_and_is_idempotent() {
    let Some(cfg) = config() else {
        eprintln!("REVISIONCHEST_TEST_DATABASE_URL not set; skipping");
        return;
    };
    reset(&cfg);
    run(&cfg);
    run(&cfg); // a second pass must not duplicate anything

    let mut c = Client::connect(&cfg.url, NoTls).unwrap();
    let count = |c: &mut Client, sql: &str| -> i64 { c.query_one(sql, &[]).unwrap().get(0) };
    assert_eq!(count(&mut c, "SELECT count(*) FROM domains"), 1);
    assert_eq!(count(&mut c, "SELECT count(*) FROM revision_bundles"), 1);
    assert_eq!(count(&mut c, "SELECT count(*) FROM revisions"), 4);
    assert_eq!(count(&mut c, "SELECT count(*) FROM page_metadata"), 3);
    assert_eq!(count(&mut c, "SELECT count(*) FROM web_resources"), 3);

    let row = c
        .query_one(
            "SELECT r.domain_id, r.page_id, r.parent_revision_id, r.revision_timestamp::text, b.file_path, r.offset_begin, r.length
             FROM revisions r JOIN revision_bundles b ON b.id = r.found_in_bundle WHERE r.revision_id = 1002",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, i32>(1), 101);
    assert_eq!(row.get::<_, Option<i64>>(2), Some(1001));
    assert_eq!(row.get::<_, String>(3), "2005-06-15 12:00:00");
    assert_eq!(row.get::<_, String>(4), "fixture.xml.mwrev.zst");
    assert_eq!((row.get::<_, i64>(5), row.get::<_, i64>(6)), (310, 95));

    let title: String = c
        .query_one(
            "SELECT page_title FROM page_metadata WHERE page_id = 104",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(title, "Draft:Tab\tand\\backslash", "COPY escaping");
    let ns: i32 = c
        .query_one(
            "SELECT page_namespace FROM page_metadata WHERE page_id = 102",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(ns, 1);
    let (url, doc): (String, Option<i32>) = {
        let r = c
            .query_one(
                "SELECT url, instance_of_document FROM web_resources WHERE numeric_page_id = 101",
                &[],
            )
            .unwrap();
        (r.get(0), r.get(1))
    };
    assert_eq!(url, "https://en.wikipedia.org/w/index.php?curid=101");
    assert!(doc.is_none(), "no documents are written any more");

    assert_eq!(
        latest_timestamp(&cfg).as_deref(),
        Some("2020-05-05T05:05:05Z")
    );
    let existing = existing_revisions(&cfg, &[1001, 1002, 9999]);
    assert!(existing.contains(&1001) && existing.contains(&1002) && !existing.contains(&9999));
}
