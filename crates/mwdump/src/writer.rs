//! Writing a dump in export schema 0.11, as MediaWiki 1.43's `XmlDumpWriter` writes one:
//! the root element and `<siteinfo>` first, then each page with its revisions, then the
//! closing tag. A hidden part is written as MediaWiki writes revision-deleted content:
//! `<text bytes="…" deleted="deleted" />`, `<comment deleted="deleted" />`,
//! `<contributor deleted="deleted" />`, with `<sha1 />` where the text is hidden. A
//! revision's further slots are `<content>` elements.
//!
//! Text is escaped as `htmlspecialchars(…, ENT_NOQUOTES)` escapes it (`&`, `<`, `>`),
//! attribute values with `"` as well, and characters XML 1.0 forbids are replaced by
//! U+FFFD, as `Xml::elementClean` cleans them. The output reads back through
//! [`crate::DumpReader`] to the same events.
//!
//! Ported from Triplespace's `scatter-mediawiki-xml`.

use std::io::Write;

use crate::reader::{PageHeader, Revision, SiteInfo, Slot};
use crate::{DumpError, SCHEMA_NAMESPACE, SCHEMA_VERSION, sha1_base36};

/// A dump being written, page by page and revision by revision, so a page with any
/// number of revisions is written without holding them all.
///
/// ```
/// use mwdump::{PageHeader, Revision, SiteInfo, Writer};
///
/// // The types are non-exhaustive: start from their defaults and set what you have.
/// let mut site = SiteInfo::default();
/// site.sitename = Some("Example".into());
/// let mut page = PageHeader::default();
/// page.id = 1;
/// page.title = "Main Page".into();
/// let mut rev = Revision::default();
/// rev.id = 10;
/// rev.timestamp = "2026-01-01T00:00:00Z".into();
/// rev.text = "Hello".into();
///
/// let mut w = Writer::new(Vec::new());
/// w.start(&site)?;
/// w.open_page(&page)?;
/// w.revision(&rev)?;
/// w.close_page()?;
/// let xml = String::from_utf8(w.finish()?).unwrap();
/// assert!(xml.contains("<text bytes=\"5\""));
/// # Ok::<(), mwdump::DumpError>(())
/// ```
pub struct Writer<W: Write> {
    out: W,
    opened: bool,
}

/// Text content, escaped.
fn text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' | '\n' | '\r' => out.push(c),
            c if (c as u32) < 0x20 || c == '\u{FFFE}' || c == '\u{FFFF}' => out.push('\u{FFFD}'),
            c => out.push(c),
        }
    }
    out
}

/// An attribute value, escaped.
fn attr(s: &str) -> String {
    text(s).replace('"', "&quot;")
}

impl<W: Write> Writer<W> {
    /// Over a sink.
    pub fn new(out: W) -> Self {
        Self { out, opened: false }
    }

    /// The sink, for a caller that drains it between pages or revisions.
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.out
    }

    /// Writes the root element and `<siteinfo>`. Called once, before any page; a page
    /// written first opens the dump with an empty `<siteinfo>`. A field that is `None` is
    /// left out.
    ///
    /// # Errors
    ///
    /// A write failure, or a dump already opened.
    pub fn start(&mut self, site: &SiteInfo) -> Result<(), DumpError> {
        if self.opened {
            return Err(DumpError::Malformed("the dump is already open".into()));
        }
        self.opened = true;
        let o = &mut self.out;
        let lang = site
            .lang
            .as_deref()
            .map_or(String::new(), |l| format!(" xml:lang=\"{}\"", attr(l)));
        writeln!(
            o,
            "<mediawiki xmlns=\"{SCHEMA_NAMESPACE}\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"{SCHEMA_NAMESPACE} http://www.mediawiki.org/xml/export-{SCHEMA_VERSION}.xsd\" version=\"{SCHEMA_VERSION}\"{lang}>"
        )?;
        writeln!(o, "  <siteinfo>")?;
        for (name, value) in [
            ("sitename", &site.sitename),
            ("dbname", &site.dbname),
            ("base", &site.base),
            ("generator", &site.generator),
            ("case", &site.case),
        ] {
            if let Some(v) = value {
                writeln!(o, "    <{name}>{}</{name}>", text(v))?;
            }
        }
        writeln!(o, "    <namespaces>")?;
        for ns in &site.namespaces {
            let case = ns
                .case
                .as_deref()
                .map_or(String::new(), |c| format!(" case=\"{}\"", attr(c)));
            if ns.name.is_empty() {
                writeln!(o, "      <namespace key=\"{}\"{case} />", ns.key)?;
            } else {
                writeln!(
                    o,
                    "      <namespace key=\"{}\"{case}>{}</namespace>",
                    ns.key,
                    text(&ns.name)
                )?;
            }
        }
        writeln!(o, "    </namespaces>")?;
        writeln!(o, "  </siteinfo>")?;
        Ok(())
    }

    /// Writes an XML comment between pages: what the export has to say about itself.
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn comment(&mut self, note: &str) -> Result<(), DumpError> {
        self.ensure_open()?;
        writeln!(self.out, "  <!-- {} -->", note.replace("--", "- -"))?;
        Ok(())
    }

    fn ensure_open(&mut self) -> Result<(), DumpError> {
        if !self.opened {
            self.start(&SiteInfo::default())?;
        }
        Ok(())
    }

    /// Writes a whole page with its revisions.
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn page(&mut self, page: &PageHeader, revisions: &[Revision]) -> Result<(), DumpError> {
        self.open_page(page)?;
        for r in revisions {
            self.revision(r)?;
        }
        self.close_page()
    }

    /// Opens a page: its title, namespace, ID and redirect. Its revisions follow through
    /// [`Self::revision`], then [`Self::close_page`].
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn open_page(&mut self, page: &PageHeader) -> Result<(), DumpError> {
        self.ensure_open()?;
        let o = &mut self.out;
        writeln!(o, "  <page>")?;
        writeln!(o, "    <title>{}</title>", text(&page.title))?;
        writeln!(o, "    <ns>{}</ns>", page.ns)?;
        writeln!(o, "    <id>{}</id>", page.id)?;
        if let Some(target) = &page.redirect {
            writeln!(o, "    <redirect title=\"{}\" />", attr(target))?;
        }
        Ok(())
    }

    /// Writes one revision of the open page. Its `page_id` and `ns` are the page's and
    /// are not written. A revision with no model or format is written as wikitext; one
    /// with no SHA-1 gets MediaWiki's SHA-1 of its text.
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn revision(&mut self, r: &Revision) -> Result<(), DumpError> {
        let o = &mut self.out;
        writeln!(o, "    <revision>")?;
        writeln!(o, "      <id>{}</id>", r.id)?;
        if let Some(p) = r.parent_id {
            writeln!(o, "      <parentid>{p}</parentid>")?;
        }
        writeln!(o, "      <timestamp>{}</timestamp>", text(&r.timestamp))?;
        if r.user_hidden {
            writeln!(o, "      <contributor deleted=\"deleted\" />")?;
        } else if r.user_name.is_some() || r.user_id.is_some() {
            writeln!(o, "      <contributor>")?;
            if let Some(name) = &r.user_name {
                writeln!(o, "        <username>{}</username>", text(name))?;
            }
            if let Some(id) = r.user_id {
                writeln!(o, "        <id>{id}</id>")?;
            }
            writeln!(o, "      </contributor>")?;
        } else if let Some(ip) = &r.ip {
            writeln!(o, "      <contributor>")?;
            writeln!(o, "        <ip>{}</ip>", text(ip))?;
            writeln!(o, "      </contributor>")?;
        } else {
            writeln!(o, "      <contributor deleted=\"deleted\" />")?;
        }
        if r.minor {
            writeln!(o, "      <minor />")?;
        }
        if r.comment_hidden {
            writeln!(o, "      <comment deleted=\"deleted\" />")?;
        } else if let Some(c) = &r.comment {
            writeln!(o, "      <comment>{}</comment>", text(c))?;
        }
        if let Some(origin) = r.origin {
            writeln!(o, "      <origin>{origin}</origin>")?;
        }
        writeln!(
            o,
            "      <model>{}</model>",
            text(r.model.as_deref().unwrap_or("wikitext"))
        )?;
        writeln!(
            o,
            "      <format>{}</format>",
            text(r.format.as_deref().unwrap_or("text/x-wiki"))
        )?;
        Self::text_element(
            o,
            "      ",
            &r.text,
            r.text_hidden,
            r.text_bytes,
            r.sha1.as_deref(),
        )?;
        for slot in &r.slots {
            Self::slot(o, slot)?;
        }
        writeln!(o, "    </revision>")?;
        Ok(())
    }

    /// `<text>` and `<sha1>` of a slot.
    fn text_element(
        o: &mut W,
        indent: &str,
        content: &str,
        hidden: bool,
        bytes: Option<u64>,
        sha1: Option<&str>,
    ) -> Result<(), DumpError> {
        let bytes = bytes.unwrap_or(content.len() as u64);
        if hidden {
            writeln!(o, "{indent}<text bytes=\"{bytes}\" deleted=\"deleted\" />")?;
            writeln!(o, "{indent}<sha1 />")?;
            return Ok(());
        }
        let sha1 = sha1.map_or_else(|| sha1_base36(content), str::to_string);
        writeln!(
            o,
            "{indent}<text bytes=\"{bytes}\" sha1=\"{}\" xml:space=\"preserve\">{}</text>",
            attr(&sha1),
            text(content)
        )?;
        writeln!(o, "{indent}<sha1>{}</sha1>", text(&sha1))?;
        Ok(())
    }

    /// A further slot as a `<content>` element.
    fn slot(o: &mut W, slot: &Slot) -> Result<(), DumpError> {
        writeln!(o, "      <content>")?;
        writeln!(o, "        <role>{}</role>", text(&slot.role))?;
        if let Some(origin) = slot.origin {
            writeln!(o, "        <origin>{origin}</origin>")?;
        }
        writeln!(
            o,
            "        <model>{}</model>",
            text(slot.model.as_deref().unwrap_or("wikitext"))
        )?;
        writeln!(
            o,
            "        <format>{}</format>",
            text(slot.format.as_deref().unwrap_or("text/x-wiki"))
        )?;
        Self::text_element(
            o,
            "        ",
            &slot.text,
            slot.text_hidden,
            slot.text_bytes,
            slot.sha1.as_deref(),
        )?;
        writeln!(o, "      </content>")?;
        Ok(())
    }

    /// Closes the open page.
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn close_page(&mut self) -> Result<(), DumpError> {
        writeln!(self.out, "  </page>")?;
        Ok(())
    }

    /// Writes the closing tag and hands the sink back.
    ///
    /// # Errors
    ///
    /// A write failure.
    pub fn finish(mut self) -> Result<W, DumpError> {
        self.ensure_open()?;
        writeln!(self.out, "</mediawiki>")?;
        self.out.flush()?;
        Ok(self.out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Namespace;
    use crate::{DumpReader, Event};

    fn site() -> SiteInfo {
        SiteInfo {
            lang: Some("en".into()),
            sitename: Some("Librarybase".into()),
            dbname: Some("librarybase".into()),
            base: Some("https://librarybase.org/wiki/Project:Home".into()),
            generator: Some("MediaWiki 1.43.9".into()),
            case: Some("first-letter".into()),
            namespaces: vec![
                Namespace {
                    key: 0,
                    name: String::new(),
                    case: Some("first-letter".into()),
                },
                Namespace {
                    key: 4,
                    name: "Librarybase".into(),
                    case: Some("first-letter".into()),
                },
            ],
        }
    }

    fn page() -> (PageHeader, Vec<Revision>) {
        let header = PageHeader {
            id: 489,
            ns: 4,
            title: "Librarybase:Home".into(),
            redirect: None,
        };
        let base = Revision {
            page_id: 489,
            ns: 4,
            model: Some("wikitext".into()),
            format: Some("text/x-wiki".into()),
            ..Revision::default()
        };
        let revisions = vec![
            Revision {
                id: 10,
                timestamp: "2023-05-04T05:00:02Z".into(),
                user_id: Some(3),
                user_name: Some("Harej".into()),
                comment: Some("Created <page> & \"more\"".into()),
                origin: Some(10),
                text: "Hello <b>&amp;</b>\n\tworld".into(),
                ..base.clone()
            },
            Revision {
                id: 11,
                parent_id: Some(10),
                timestamp: "2023-05-05T05:00:02Z".into(),
                ip: Some("127.0.0.1".into()),
                minor: true,
                comment_hidden: true,
                text_hidden: true,
                text_bytes: Some(6),
                ..base.clone()
            },
            Revision {
                id: 12,
                parent_id: Some(11),
                timestamp: "2023-05-06T05:00:02Z".into(),
                user_hidden: true,
                text: "x".into(),
                slots: vec![Slot {
                    role: "notes".into(),
                    origin: Some(12),
                    model: Some("json".into()),
                    format: Some("application/json".into()),
                    text: "{}".into(),
                    ..Slot::default()
                }],
                ..base
            },
        ];
        (header, revisions)
    }

    #[test]
    fn writes_mediawikis_shape_and_reads_it_back() {
        let (header, revisions) = page();
        let mut w = Writer::new(Vec::new());
        w.start(&site()).unwrap();
        w.comment("templates: none added -- see the note").unwrap();
        w.page(&header, &revisions).unwrap();
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert!(out.starts_with(
            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.11/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.11/ http://www.mediawiki.org/xml/export-0.11.xsd\" version=\"0.11\" xml:lang=\"en\">\n  <siteinfo>\n    <sitename>Librarybase</sitename>\n"
        ));
        assert!(out.contains("      <namespace key=\"0\" case=\"first-letter\" />\n"));
        assert!(out.contains(
            "      <namespace key=\"4\" case=\"first-letter\">Librarybase</namespace>\n"
        ));
        assert!(out.contains("  <!-- templates: none added - - see the note -->\n"));
        assert!(out.contains("      <comment>Created &lt;page&gt; &amp; \"more\"</comment>\n"));
        assert!(out.contains(&format!(
            "      <text bytes=\"25\" sha1=\"{0}\" xml:space=\"preserve\">Hello &lt;b&gt;&amp;amp;&lt;/b&gt;\n\tworld</text>\n      <sha1>{0}</sha1>\n",
            sha1_base36(&revisions[0].text)
        )));
        assert!(out.contains("      <text bytes=\"6\" deleted=\"deleted\" />\n      <sha1 />\n"));
        assert!(out.contains("      <comment deleted=\"deleted\" />\n"));
        assert!(out.contains("      <contributor deleted=\"deleted\" />\n"));
        assert!(out.contains("        <ip>127.0.0.1</ip>\n"));
        assert!(out.contains("      <minor />\n"));
        assert!(out.contains("      <content>\n        <role>notes</role>\n"));
        assert!(out.ends_with("  </page>\n</mediawiki>\n"));

        let events: Vec<Event> = DumpReader::new(out.as_bytes())
            .map(Result::unwrap)
            .collect();
        assert_eq!(events[0], Event::SiteInfo(site()));
        assert_eq!(events[1], Event::PageStart(header));
        let back: Vec<Revision> = events
            .iter()
            .filter_map(|e| match e {
                Event::Revision(r) => Some(r.clone()),
                _ => None,
            })
            .collect();
        // What the writer filled in: the text's size and SHA-1, where the input had none.
        let expected: Vec<Revision> = revisions
            .into_iter()
            .map(|r| Revision {
                text_bytes: r.text_bytes.or(Some(r.text.len() as u64)),
                sha1: (!r.text_hidden).then(|| sha1_base36(&r.text)),
                slots: r
                    .slots
                    .into_iter()
                    .map(|s| Slot {
                        text_bytes: Some(s.text.len() as u64),
                        sha1: Some(sha1_base36(&s.text)),
                        ..s
                    })
                    .collect(),
                ..r
            })
            .collect();
        assert_eq!(back, expected);
    }

    #[test]
    fn forbidden_characters_are_replaced() {
        assert_eq!(text("a\u{1}b\tc"), "a\u{FFFD}b\tc");
        assert_eq!(attr("\"<&>\""), "&quot;&lt;&amp;&gt;&quot;");
    }

    #[test]
    fn a_page_before_start_opens_the_dump() {
        let mut w = Writer::new(Vec::new());
        w.page(&PageHeader::default(), &[]).unwrap();
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert!(out.starts_with("<mediawiki "));
        assert!(out.contains("<siteinfo>"));
        let mut w = Writer::new(Vec::new());
        w.start(&SiteInfo::default()).unwrap();
        assert!(w.start(&SiteInfo::default()).is_err());
    }
}
