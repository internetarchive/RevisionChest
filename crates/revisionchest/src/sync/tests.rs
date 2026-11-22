use super::*;
use clap::Parser;
use revisionchest_meta::{MetaSink, SinkError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct Collect(Arc<Mutex<Vec<MetaEvent>>>);
impl MetaSink for Collect {
    fn handle(&mut self, e: MetaEvent) -> Result<(), SinkError> {
        self.0.lock().unwrap().push(e);
        Ok(())
    }
    fn finish(self: Box<Self>) -> Result<(), SinkError> {
        Ok(())
    }
}

/// 2026-10-19T00:00:00Z
const T0: u64 = 1_792_368_000;

/// A clock and the seconds each list request moves it on.
type Tick = (Arc<AtomicU64>, u64);

/// A revision the fake wiki serves: its page, its text (`None` when hidden), its time.
type FakeRevision = (u64, Option<String>, u64);

/// A wiki: its recent changes, the revisions they name, and the oldest change it
/// still retains. Records every `prop=revisions` request.
#[derive(Clone, Default)]
struct FakeWiki {
    rows: Arc<Mutex<Vec<Value>>>,
    revisions: Arc<Mutex<HashMap<u64, FakeRevision>>>,
    oldest: Arc<Mutex<Option<u64>>>,
    fetched: Arc<Mutex<Vec<u64>>>,
    /// A clock each `list=recentchanges` request moves on, and by how much.
    tick: Arc<Mutex<Option<Tick>>>,
}

impl FakeWiki {
    fn edit(&self, rcid: u64, revid: u64, page: u64, t: u64, text: Option<&str>) {
        self.rows.lock().unwrap().push(json!({
            "rcid": rcid, "type": "edit", "ns": 0, "title": format!("P{page}"),
            "revid": revid, "pageid": page, "timestamp": mwrecent::iso(t),
        }));
        self.revisions
            .lock()
            .unwrap()
            .insert(revid, (page, text.map(str::to_string), t));
    }

    fn log(&self, rcid: u64, page: u64, t: u64, action: &str, params: Value) {
        self.rows.lock().unwrap().push(json!({
            "rcid": rcid, "type": "log", "ns": 0, "title": format!("P{page}"),
            "revid": 0, "pageid": page, "timestamp": mwrecent::iso(t),
            "logid": rcid + 1000, "logtype": "delete", "logaction": action, "logparams": params,
        }));
    }

    fn fetched(&self) -> Vec<u64> {
        self.fetched.lock().unwrap().clone()
    }
}

impl Wiki for FakeWiki {
    fn get(&self, params: &[(String, String)]) -> io::Result<String> {
        let p: HashMap<&str, &str> = params
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        if p.get("prop") == Some(&"revisions") {
            let revisions = self.revisions.lock().unwrap();
            let mut pages: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
            for id in p["revids"].split('|') {
                let id: u64 = id.parse().unwrap();
                self.fetched.lock().unwrap().push(id);
                let (page, text, t) = &revisions[&id];
                let main = match text {
                    Some(text) => json!({"content": text, "contentmodel": "wikitext"}),
                    None => json!({"texthidden": true}),
                };
                pages.entry(*page).or_default().push(json!({
                    "revid": id, "parentid": 0, "timestamp": mwrecent::iso(*t), "userid": 42,
                    "slots": {"main": main},
                }));
            }
            let pages: Vec<Value> = pages
                .into_iter()
                .map(|(id, revs)| json!({"pageid": id, "ns": 0, "title": format!("P{id}"), "revisions": revs}))
                .collect();
            return Ok(json!({"query": {"pages": pages}}).to_string());
        }
        // list=recentchanges, oldest first
        if let Some((clock, by)) = &*self.tick.lock().unwrap() {
            clock.fetch_add(*by, Ordering::SeqCst);
        }
        let oldest = *self.oldest.lock().unwrap();
        let mut rows: Vec<Value> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|r| {
                let t = mwrecent::parse_iso(r["timestamp"].as_str().unwrap()).unwrap();
                oldest.is_none_or(|o| t >= o)
            })
            .cloned()
            .collect();
        let key = |r: &Value| {
            (
                mwrecent::parse_iso(r["timestamp"].as_str().unwrap()).unwrap(),
                r["rcid"].as_u64().unwrap(),
            )
        };
        rows.sort_by_key(key);
        let from = match (p.get("rccontinue"), p.get("rcstart")) {
            (Some(c), _) => {
                let (t, id) = c.split_once('|').unwrap();
                (t.parse().unwrap(), id.parse().unwrap())
            }
            (None, Some(s)) => (mwrecent::parse_iso(s).unwrap(), 0),
            (None, None) => (0, 0),
        };
        rows.retain(|r| key(r) >= from);
        let limit: usize = p["rclimit"].parse().unwrap();
        let more = rows.len() > limit;
        let mut v = json!({"query": {"recentchanges": rows[..rows.len().min(limit)]}});
        if more {
            let (t, id) = key(&rows[limit]);
            v["continue"] = json!({"rccontinue": format!("{t}|{id}"), "continue": "-||"});
        }
        Ok(v.to_string())
    }
}

fn args(dir: &Path, extra: &[&str]) -> SyncArgs {
    let mut argv = vec![
        "sync".to_string(),
        "-o".into(),
        dir.display().to_string(),
        "--domain".into(),
        "test.example".into(),
        "--since".into(),
        mwrecent::iso(T0),
    ];
    argv.extend(extra.iter().map(ToString::to_string));
    SyncArgs::parse_from(argv)
}

fn clock(now: &Arc<AtomicU64>) -> Box<dyn Fn() -> u64> {
    let now = now.clone();
    Box::new(move || now.load(Ordering::SeqCst))
}

fn syncer<'a>(args: &'a SyncArgs, wiki: &FakeWiki, now: &Arc<AtomicU64>) -> io::Result<Syncer<'a>> {
    Syncer::new(
        args,
        Box::new(wiki.clone()),
        clock(now),
        None,
        Box::new(|_: &[u64]| HashSet::new()),
    )
}

/// One cycle as `main` runs it; `commit: false` stops before the commit, as a
/// crash would.
fn run(s: &mut Syncer, commit: bool, parquet: Option<&Path>) -> Vec<MetaEvent> {
    run_bounded(s, commit, parquet, None).0
}

/// [`run`] with a cycle budget (seconds); also returns whether it caught up.
fn run_bounded(
    s: &mut Syncer,
    commit: bool,
    parquet: Option<&Path>,
    budget: Option<u64>,
) -> (Vec<MetaEvent>, bool) {
    let events = Arc::new(Mutex::new(Vec::new()));
    s.begin(parquet).unwrap();
    let (meta, handle) = revisionchest_meta::spawn(Box::new(Collect(events.clone())), 64);
    let caught_up = s.cycle(&meta, budget).unwrap();
    drop(meta);
    handle.join().unwrap();
    if commit {
        s.commit().unwrap();
    }
    let events = events.lock().unwrap().clone();
    (events, caught_up)
}

fn revisions(events: &[MetaEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e {
            MetaEvent::Revision { rev_id, .. } => Some(*rev_id),
            _ => None,
        })
        .collect()
}

fn bundle_ids(dir: &Path, day: &str) -> Vec<u64> {
    mwrev::open_bundle(&dir.join(format!("recentchanges/{day}.mwrev.zst")))
        .unwrap()
        .map(|r| r.unwrap().0.rev_id)
        .collect()
}

#[test]
fn a_restart_neither_misses_nor_repeats_a_revision() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 600));
    wiki.edit(1, 101, 7, T0 + 10, Some("one"));
    wiki.edit(2, 102, 7, T0 + 20, Some("two"));

    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert_eq!(revisions(&run(&mut s, true, None)), [101, 102]);

    // The next cycle fetches and appends, then the process dies before it commits,
    // with its Parquet file half written and a frame cut off at the end of a bundle.
    wiki.edit(3, 103, 8, T0 + 30, Some("three"));
    wiki.edit(4, 104, 8, T0 + 40, Some("four"));
    let pending = dir.path().join("cycle.parquet");
    fs::write(&pending, b"PAR1 half written").unwrap();
    let crashed = run(&mut s, false, Some(&pending));
    assert_eq!(revisions(&crashed), [103, 104]);
    drop(s);
    let bundle = dir.path().join("recentchanges/2026-10-19.mwrev.zst");
    let bytes = fs::read(&bundle).unwrap();
    let mut f = fs::OpenOptions::new().append(true).open(&bundle).unwrap();
    std::io::Write::write_all(&mut f, &bytes[..bytes.len() / 3]).unwrap();
    drop(f);

    // Restart: what the dead cycle wrote is reported again from the bundle, not
    // fetched, and the cycle's Parquet file is gone.
    wiki.edit(5, 105, 9, T0 + 50, Some("five"));
    let fetched_before = wiki.fetched().len();
    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert!(!pending.exists(), "the uncommitted Parquet file is removed");
    let events = run(&mut s, true, None);
    assert_eq!(revisions(&events), [103, 104, 105]);
    assert_eq!(
        wiki.fetched()[fetched_before..],
        [105],
        "nothing fetched twice"
    );
    assert_eq!(
        bundle_ids(dir.path(), "2026-10-19"),
        [101, 102, 103, 104, 105]
    );

    // The recovered locations read back their revisions' text.
    for e in &events {
        if let MetaEvent::Revision {
            rev_id,
            file_path,
            offset_begin,
            length,
            ..
        } = e
        {
            let text = mwrev::read_at(&dir.path().join(file_path), *offset_begin, *length).unwrap();
            let want = match rev_id {
                103 => " three\n",
                104 => " four\n",
                _ => " five\n",
            };
            assert_eq!(String::from_utf8(text).unwrap(), want);
        }
    }

    // A clean restart after a commit fetches nothing again.
    drop(s);
    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert!(revisions(&run(&mut s, true, None)).is_empty());
    assert_eq!(
        bundle_ids(dir.path(), "2026-10-19"),
        [101, 102, 103, 104, 105]
    );
}

#[test]
fn the_pending_parquet_file_is_recorded_by_its_absolute_path() {
    // A restart from another working directory must still find the file to remove it
    // (found in a live run against test.wikipedia.org on 2026-11-20).
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 600));
    let s = syncer(&a, &wiki, &now).unwrap();
    s.begin(Some(Path::new("meta/cycle.parquet"))).unwrap();
    let pos: Value = serde_json::from_slice(
        &fs::read(dir.path().join("recentchanges").join(POSITION_FILE)).unwrap(),
    )
    .unwrap();
    let recorded = Path::new(pos["pending_parquet"].as_str().unwrap());
    assert!(recorded.is_absolute(), "{}", recorded.display());
    assert!(recorded.ends_with("meta/cycle.parquet"));
}

#[test]
fn a_duplicate_row_and_a_late_one_are_each_written_once() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 600));
    wiki.edit(1, 101, 7, T0 + 10, Some("one"));
    wiki.edit(2, 102, 7, T0 + 20, Some("two"));
    // The same revision under a second rcid, as a wiki can list it twice.
    wiki.edit(3, 102, 7, T0 + 20, Some("two"));
    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert_eq!(revisions(&run(&mut s, true, None)), [101, 102]);

    // A change committed late, timestamped before the position: the tail pass
    // finds it; the rows already seen are not read again.
    wiki.edit(4, 103, 8, T0 + 15, Some("late"));
    now.store(T0 + 700, Ordering::SeqCst);
    assert_eq!(revisions(&run(&mut s, true, None)), [103]);
    assert_eq!(wiki.fetched(), [101, 102, 103]);
    assert_eq!(bundle_ids(dir.path(), "2026-10-19"), [101, 102, 103]);
}

#[test]
fn a_quiet_wiki_within_its_retention_is_no_gap() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    // Nothing for 20 days after the position; the wiki keeps 30.
    let now = Arc::new(AtomicU64::new(T0 + 25 * 86_400));
    wiki.edit(1, 101, 7, T0 + 20 * 86_400, Some("after a quiet spell"));
    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert_eq!(revisions(&run(&mut s, true, None)), [101]);
    assert!(!dir.path().join("recentchanges").join(GAPS_FILE).exists());
}

#[test]
fn a_gap_is_refused_unless_accepted_and_then_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 90 * 86_400));
    wiki.edit(1, 101, 7, T0 + 10, Some("lost"));
    wiki.edit(2, 102, 7, T0 + 60 * 86_400, Some("kept"));
    *wiki.oldest.lock().unwrap() = Some(T0 + 30 * 86_400);

    let a = args(dir.path(), &[]);
    let err = syncer(&a, &wiki, &now).err().unwrap();
    assert!(err.to_string().contains("--accept-gap"), "{err}");

    let a = args(dir.path(), &["--accept-gap"]);
    let mut s = syncer(&a, &wiki, &now).unwrap();
    assert_eq!(revisions(&run(&mut s, true, None)), [102]);
    let gaps = fs::read_to_string(dir.path().join("recentchanges").join(GAPS_FILE)).unwrap();
    let gap: Value = serde_json::from_str(gaps.lines().next().unwrap()).unwrap();
    assert_eq!(gap["from"], mwrecent::iso(T0));
    // The oldest change the wiki still has, at day 60, not its retention boundary.
    assert_eq!(gap["to"], mwrecent::iso(T0 + 60 * 86_400));
}

#[test]
fn log_events_are_recorded_and_hidden_text_is_flagged() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 600));
    wiki.edit(1, 101, 7, T0 + 10, None); // its text already hidden
    wiki.log(
        2,
        7,
        T0 + 20,
        "revision",
        json!({"ids": [101], "type": "revision"}),
    );
    let mut s = syncer(&a, &wiki, &now).unwrap();
    let events = run(&mut s, true, None);

    let log = events
        .iter()
        .find_map(|e| match e {
            MetaEvent::Log {
                rcid,
                log_id,
                log_type,
                log_action,
                page_id,
                params,
                ..
            } => Some((
                *rcid,
                *log_id,
                log_type.clone(),
                log_action.clone(),
                *page_id,
                params.clone(),
            )),
            _ => None,
        })
        .unwrap();
    assert_eq!(log.0, 2);
    assert_eq!(log.1, Some(1002));
    assert_eq!((log.2.as_str(), log.3.as_str()), ("delete", "revision"));
    assert_eq!(log.4, Some(7));
    let params: Value = serde_json::from_str(&log.5.unwrap()).unwrap();
    assert_eq!(params["ids"][0], 101);

    let (header, text) = mwrev::open_bundle(&dir.path().join("recentchanges/2026-10-19.mwrev.zst"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert!(header.text_hidden);
    assert_eq!(text, "");
}

#[test]
fn a_long_backlog_is_read_by_continuation() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 86_400));
    for i in 0..1_200u64 {
        wiki.edit(i + 1, 10_000 + i, 7 + i % 3, T0 + 10 + i * 10, Some("x"));
    }
    let mut s = syncer(&a, &wiki, &now).unwrap();
    let got = revisions(&run(&mut s, true, None));
    assert_eq!(got.len(), 1_200);
    assert_eq!(got.iter().copied().collect::<HashSet<_>>().len(), 1_200);
}

#[test]
fn appended_offsets_locate_each_revision_across_batches_and_days() {
    let dir = tempfile::tempdir().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let (meta, handle) = revisionchest_meta::spawn(Box::new(Collect(events.clone())), 16);

    let resp = |revs: &[(u64, u64, Option<u64>, &str, &str)]| {
        let mut pages = serde_json::Map::new();
        for (page_id, rev_id, parent, ts, text) in revs {
            let page = pages.entry(page_id.to_string()).or_insert_with(|| {
                json!({"pageid": page_id, "ns": 0, "title": format!("P{page_id}"), "revisions": []})
            });
            page["revisions"].as_array_mut().unwrap().push(json!({
                "revid": rev_id, "parentid": parent.unwrap_or(0), "timestamp": ts, "userid": 42,
                "slots": {"main": {"content": text, "contentmodel": "wikitext"}}
            }));
        }
        json!({"query": {"pages": pages.values().cloned().collect::<Vec<_>>()}})
    };
    let batch1 = resp(&[
        (
            7,
            100,
            None,
            "2026-10-18T23:59:00Z",
            "late on the 18th\nsecond line",
        ),
        (
            7,
            101,
            Some(100),
            "2026-10-19T00:00:10Z",
            "first on the 19th",
        ),
        (8, 200, None, "2026-10-19T01:00:00Z", "another page\n"),
    ]);
    let batch2 = resp(&[(
        7,
        102,
        Some(101),
        "2026-10-19T02:00:00Z",
        "a later batch appends a new frame",
    )]);
    append_revisions(parse_revisions(&batch1, None), dir.path(), &meta).unwrap();
    append_revisions(parse_revisions(&batch2, None), dir.path(), &meta).unwrap();
    drop(meta);
    handle.join().unwrap();

    let events = events.lock().unwrap();
    let revs: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            MetaEvent::Revision {
                rev_id,
                file_path,
                offset_begin,
                length,
                ..
            } => Some((*rev_id, file_path.clone(), *offset_begin, *length)),
            _ => None,
        })
        .collect();
    assert_eq!(
        revs.iter().map(|r| r.0).collect::<Vec<_>>(),
        [100, 101, 200, 102]
    );
    assert_eq!(revs[0].1, "recentchanges/2026-10-18.mwrev.zst");
    let expected = [
        (100u64, " late on the 18th\n second line\n"),
        (101, " first on the 19th\n"),
        (200, " another page\n"),
        (102, " a later batch appends a new frame\n"),
    ];
    for ((rev_id, file_path, off, len), (exp_id, exp_text)) in revs.iter().zip(expected) {
        assert_eq!(*rev_id, exp_id);
        let bytes = mwrev::read_at(&dir.path().join(file_path), *off, *len).unwrap();
        assert_eq!(String::from_utf8(bytes).unwrap(), exp_text, "rev {rev_id}");
    }
}

#[test]
fn namespace_filter_and_bad_rows() {
    let resp = |ns: i64, rev_id: u64, ts: &str| {
        json!({"query": {"pages": [{"pageid": 7, "ns": ns, "title": "P7", "revisions": [
            {"revid": rev_id, "parentid": 0, "timestamp": ts, "userid": 42,
             "slots": {"main": {"content": "x", "contentmodel": "wikitext"}}}
        ]}]}})
    };
    let r = resp(118, 1, "2026-10-19T00:00:00Z");
    assert!(parse_revisions(&r, Some(&[0])).is_empty());
    assert_eq!(parse_revisions(&r, Some(&[0, 118])).len(), 1);
    assert!(parse_revisions(&resp(0, 0, ""), None).is_empty());
}

#[test]
fn a_long_catch_up_commits_as_it_goes_and_resumes_from_its_last_commit() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 100_000));
    let n: u64 = 1_400; // three list requests of 500
    for i in 1..=n {
        wiki.edit(i, 1_000 + i, i % 7, T0 + i * 10, Some("text"));
    }
    // every list request takes a minute; a cycle commits once it has run one
    *wiki.tick.lock().unwrap() = Some((now.clone(), 60));
    let budget = Some(60);

    // the first cycle stops behind, after its first batch, and commits it with
    // the continuation
    let mut s = syncer(&a, &wiki, &now).unwrap();
    let (first, caught_up) = run_bounded(&mut s, true, None, budget);
    assert!(!caught_up);
    assert_eq!(revisions(&first).len(), 500);
    let pos: Value =
        serde_json::from_slice(&fs::read(dir.path().join("recentchanges/position.json")).unwrap())
            .unwrap();
    assert!(pos["continue"].is_object(), "{pos}");

    // the second does not commit (a crash); a restart rolls its batch forward
    // from the bundle and reads on from the first commit, not from the start
    let (second, caught_up) = run_bounded(&mut s, false, None, budget);
    assert!(!caught_up);
    assert_eq!(revisions(&second).len(), 500);
    drop(s);
    let lists_before = wiki.fetched().len();
    let mut s = syncer(&a, &wiki, &now).unwrap();
    let mut rest = Vec::new();
    loop {
        let (events, caught_up) = run_bounded(&mut s, true, None, budget);
        rest.extend(revisions(&events));
        if caught_up {
            break;
        }
    }
    // revisions 501..=1000 reported again from the bundle, the rest fetched now
    assert_eq!(rest.len(), 900);
    let fetched = wiki.fetched();
    assert_eq!(
        fetched.len(),
        usize::try_from(n).unwrap(),
        "each revision fetched once"
    );
    assert_eq!(fetched.len() - lists_before, 400);

    let mut ids: Vec<u64> = std::fs::read_dir(dir.path().join("recentchanges"))
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().unwrap();
            name.strip_suffix(".mwrev.zst").map(str::to_string)
        })
        .flat_map(|day| bundle_ids(dir.path(), &day))
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (1_001..=1_000 + n).collect::<Vec<_>>(),
        "every revision in the bundles once"
    );
}

#[test]
fn an_unbounded_cycle_reads_the_whole_backlog() {
    let dir = tempfile::tempdir().unwrap();
    let a = args(dir.path(), &[]);
    let wiki = FakeWiki::default();
    let now = Arc::new(AtomicU64::new(T0 + 100_000));
    for i in 1..=1_200 {
        wiki.edit(i, 1_000 + i, 1, T0 + i * 10, Some("text"));
    }
    *wiki.tick.lock().unwrap() = Some((now.clone(), 60));
    let mut s = syncer(&a, &wiki, &now).unwrap();
    let (events, caught_up) = run_bounded(&mut s, true, None, None);
    assert!(caught_up);
    assert_eq!(revisions(&events).len(), 1_200);
}
