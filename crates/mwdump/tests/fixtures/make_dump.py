#!/usr/bin/env python3
"""Write a small MediaWiki export-0.11 dump exercising what a dump reader must handle.

Outputs <out>/fixture.xml plus .bz2, .gz and .7z copies, and <out>/expected.json
describing every revision (for the mwdump unit tests and the before/after gate).
"""
import bz2
import gzip
import json
import sys
from pathlib import Path

import py7zr

NS = "http://www.mediawiki.org/xml/export-0.11/"

PAGES = [
    # (title, ns, id, redirect, revisions)
    ("Example Island", 0, 101, None, [
        dict(id=1001, parent=None, ts="2004-01-01T00:00:00Z", user=("Alice", 7), minor=False,
             comment="start", text="'''Example Island''' is an island.<ref name=\"d\">{{cite book |last=Diamond |title=Collapse}}</ref>\n\n== References ==\n{{reflist}}\n"),
        dict(id=1002, parent=1001, ts="2005-06-15T12:00:00Z", ip="203.0.113.5", minor=True,
             comment="typo & <fix>", text="Line with entities: &amp; &lt;b&gt; and a tab\there.\nSecond line.\n\nFourth line after blank.\n"),
        dict(id=1003, parent=1002, ts="2010-03-03T03:03:03Z", user=("Bob", 9), minor=False,
             comment=None, text="no trailing newline"),
        dict(id=1004, parent=1003, ts="2011-01-01T00:00:00Z", user_hidden=True, minor=False,
             comment=None, text_deleted=True),
        dict(id=1005, parent=1004, ts="2012-01-01T00:00:00Z", user=("Alice", 7), minor=False,
             comment="crlf", text="first\r\nsecond\r\n"),
        dict(id=1006, parent=1005, ts="2013-01-01T00:00:00Z", user=("Alice", 7), minor=False,
             comment="empty", text=""),
    ]),
    ("Talk:Example Island", 1, 102, None, [
        dict(id=2001, parent=None, ts="2006-02-02T00:00:00Z", user=("Carol", 11), minor=False,
             comment="talk", text="A talk page revision. Should be skipped by --namespace=0.\n"),
    ]),
    ("Redirect Page", 0, 103, "Example Island", [
        dict(id=3001, parent=None, ts="2007-07-07T07:07:07Z", user=("Dave", 12), minor=False,
             comment=None, text="#REDIRECT [[Example Island]]\n"),
    ]),
    ("Draft:Something", 118, 104, None, [
        dict(id=4001, parent=None, ts="2020-05-05T05:05:05Z", user=("Erin", 13), minor=False,
             comment="unicode — ünïcødé 日本語", text="Unicode text: ünïcødé 日本語 🙂\n* bullet\n"),
    ]),
]


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def render() -> str:
    out = ['<?xml version="1.0" encoding="utf-8"?>',
           f'<mediawiki xmlns="{NS}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
           f'xsi:schemaLocation="{NS} http://www.mediawiki.org/xml/export-0.11.xsd" version="0.11" xml:lang="en">',
           "  <siteinfo>", "    <sitename>Wikipedia</sitename>", "    <dbname>enwiki</dbname>",
           "    <base>https://en.wikipedia.org/wiki/Main_Page</base>", "    <generator>MediaWiki 1.44</generator>",
           "    <case>first-letter</case>", "    <namespaces>",
           '      <namespace key="0" case="first-letter" />', '      <namespace key="1" case="first-letter">Talk</namespace>',
           '      <namespace key="118" case="first-letter">Draft</namespace>', "    </namespaces>", "  </siteinfo>"]
    for title, ns, pid, redirect, revs in PAGES:
        out += ["  <page>", f"    <title>{esc(title)}</title>", f"    <ns>{ns}</ns>", f"    <id>{pid}</id>"]
        if redirect:
            out.append(f'    <redirect title="{esc(redirect)}" />')
        for r in revs:
            out += ["    <revision>", f"      <id>{r['id']}</id>"]
            if r["parent"] is not None:
                out.append(f"      <parentid>{r['parent']}</parentid>")
            out.append(f"      <timestamp>{r['ts']}</timestamp>")
            if r.get("user_hidden"):
                out.append('      <contributor deleted="deleted" />')
            elif "ip" in r:
                out += ["      <contributor>", f"        <ip>{r['ip']}</ip>", "      </contributor>"]
            else:
                name, uid = r["user"]
                out += ["      <contributor>", f"        <username>{esc(name)}</username>", f"        <id>{uid}</id>", "      </contributor>"]
            if r["minor"]:
                out.append("      <minor />")
            if r.get("comment") is not None:
                out.append(f"      <comment>{esc(r['comment'])}</comment>")
            out += ["      <origin>%d</origin>" % r["id"], "      <model>wikitext</model>", "      <format>text/x-wiki</format>"]
            if r.get("text_deleted"):
                out.append('      <text bytes="0" deleted="deleted" />')
            else:
                t = r.get("text", "")
                out.append(f'      <text bytes="{len(t.encode())}" xml:space="preserve">{esc(t)}</text>')
                out.append("      <sha1>phoiac9h4m842xq45sp7s6u21eteeq1</sha1>")
            out.append("    </revision>")
        out.append("  </page>")
    out.append("</mediawiki>")
    return "\n".join(out) + "\n"


def expected() -> list[dict]:
    rows = []
    for title, ns, pid, redirect, revs in PAGES:
        for r in revs:
            rows.append({
                "page_id": pid, "ns": ns, "title": title, "redirect": redirect,
                "rev_id": r["id"], "parent": r["parent"], "timestamp": r["ts"],
                "user_id": (r["user"][1] if "user" in r else None),
                "user_name": (r["user"][0] if "user" in r else None),
                "ip": r.get("ip"), "user_hidden": bool(r.get("user_hidden")), "minor": r["minor"],
                "comment": r.get("comment"), "text": r.get("text", ""), "text_deleted": bool(r.get("text_deleted")),
            })
    return rows


def main(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    xml = render().encode("utf-8")
    (out / "fixture.xml").write_bytes(xml)
    (out / "fixture.xml.bz2").write_bytes(bz2.compress(xml))
    (out / "fixture.xml.gz").write_bytes(gzip.compress(xml))
    with py7zr.SevenZipFile(out / "fixture.xml.7z", "w") as z:
        z.writestr(xml, "fixture.xml")
    (out / "expected.json").write_text(json.dumps(expected(), indent=1, ensure_ascii=False))
    print(f"wrote {len(xml)} bytes of XML to {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
