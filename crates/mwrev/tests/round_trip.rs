use std::io::Cursor;

use mwrev::{BundleReader, BundleWriter, RevisionHeader, read_at};

fn h(page: u64, rev: u64, parent: Option<u64>) -> RevisionHeader {
    RevisionHeader {
        page_id: page,
        ns: 0,
        rev_id: rev,
        parent_rev_id: parent,
        timestamp: "2004-01-01T00:00:00Z".into(),
        user_id: 7,
    }
}

#[test]
fn write_then_read_and_offsets_locate_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.mwrev.zst");
    let texts = [
        "first\nsecond\n",
        "no trailing newline",
        "",
        "crlf\r\nlines\r\n",
        "ünïcødé 日本語 🙂\n* bullet",
    ];
    let mut locs = Vec::new();
    {
        let mut w = BundleWriter::create(&path).unwrap();
        for (i, t) in texts.iter().enumerate() {
            locs.push(
                w.write_revision(&h(1, i as u64 + 1, (i > 0).then_some(i as u64)), t)
                    .unwrap(),
            );
        }
        w.finish().unwrap();
    }
    let back: Vec<_> = BundleReader::open(&path)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(back.len(), texts.len());
    let expected_texts = [
        "first\nsecond",
        "no trailing newline",
        "",
        "crlf\nlines",
        "ünïcødé 日本語 🙂\n* bullet",
    ];
    for (i, ((hdr, text), exp)) in back.iter().zip(expected_texts).enumerate() {
        assert_eq!(hdr.rev_id, i as u64 + 1);
        assert_eq!(text, exp, "revision {}", i + 1);
        // the recorded location points at exactly the space-prefixed lines
        let bytes = read_at(&path, locs[i].offset_begin, locs[i].length).unwrap();
        let expected_bytes: String = exp.lines().map(|l| format!(" {l}\n")).collect();
        let expected_bytes = if exp.is_empty() {
            String::new()
        } else {
            expected_bytes
        };
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            expected_bytes,
            "location of revision {}",
            i + 1
        );
    }
    assert_eq!(locs[2].length, 0);
}

#[test]
fn append_continues_offsets_across_frames() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("day.mwrev.zst");
    let a = {
        let mut w = BundleWriter::create(&path).unwrap();
        let l = w.write_revision(&h(1, 1, None), "alpha\nbeta\n").unwrap();
        w.finish().unwrap();
        l
    };
    // no sidecar yet (create() does not write one): append measures the file
    let b = {
        let mut w = BundleWriter::append(&path).unwrap();
        let l = w.write_revision(&h(1, 2, Some(1)), "gamma\n").unwrap();
        w.finish().unwrap();
        l
    };
    // sidecar now exists; a third append must agree with it
    let c = {
        let mut w = BundleWriter::append(&path).unwrap();
        let l = w
            .write_revision(&h(2, 3, None), "delta\nepsilon\n")
            .unwrap();
        w.finish().unwrap();
        l
    };
    assert!(b.offset_begin > a.offset_begin + a.length);
    assert!(c.offset_begin > b.offset_begin + b.length);
    assert_eq!(
        String::from_utf8(read_at(&path, a.offset_begin, a.length).unwrap()).unwrap(),
        " alpha\n beta\n"
    );
    assert_eq!(
        String::from_utf8(read_at(&path, b.offset_begin, b.length).unwrap()).unwrap(),
        " gamma\n"
    );
    assert_eq!(
        String::from_utf8(read_at(&path, c.offset_begin, c.length).unwrap()).unwrap(),
        " delta\n epsilon\n"
    );
    // the reader walks all three frames
    let ids: Vec<u64> = BundleReader::open(&path)
        .unwrap()
        .map(|r| r.unwrap().0.rev_id)
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
    let sidecar = std::fs::read_to_string(mwrev::writer_sidecar_path(&path)).unwrap();
    assert_eq!(
        sidecar.trim().parse::<u64>().unwrap(),
        c.offset_begin + c.length
    );
}

#[test]
fn reader_skips_junk_lines_and_tolerates_missing_text() {
    let data = "# page_id=1 ns=0 rev_id=1 parent_rev_id= timestamp=t user_id=0\njunk line\n# page_id=1 ns=0 rev_id=2 parent_rev_id=1 timestamp=t user_id=0\n x\n";
    let items: Vec<_> = BundleReader::from_decompressed(Cursor::new(data))
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].1, "");
    assert_eq!(items[1].1, "x");
}
