//! The event reader.

use std::io::BufRead;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event as XmlEvent};
use quick_xml::name::QName;

use crate::DumpError;

/// One namespace from `<siteinfo><namespaces>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Namespace {
    /// The namespace number (`key` attribute).
    pub key: i64,
    /// The canonical name; empty for the main namespace.
    pub name: String,
    /// The `case` attribute: `first-letter` or `case-sensitive`.
    pub case: Option<String>,
}

/// The dump's `<siteinfo>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct SiteInfo {
    /// `<sitename>`.
    pub sitename: Option<String>,
    /// `<dbname>`, e.g. `enwiki`.
    pub dbname: Option<String>,
    /// `<base>`: the URL of the main page, from which the domain is taken.
    pub base: Option<String>,
    /// `<generator>`.
    pub generator: Option<String>,
    /// `<case>`: `first-letter` or `case-sensitive`.
    pub case: Option<String>,
    /// The `xml:lang` of the root element, when present.
    pub lang: Option<String>,
    /// The namespaces table.
    pub namespaces: Vec<Namespace>,
}

impl SiteInfo {
    /// The site's domain, taken from `<base>` (`https://en.wikipedia.org/wiki/Main_Page`
    /// → `en.wikipedia.org`).
    #[must_use]
    pub fn domain(&self) -> Option<String> {
        let base = self.base.as_deref()?;
        let rest = base.split_once("://")?.1;
        let host = rest.split(['/', '?', '#']).next()?;
        let host = host.rsplit('@').next()?; // userinfo, never present in practice
        let host = host.split(':').next()?;
        (!host.is_empty()).then(|| host.to_string())
    }
}

/// What a page carries besides its revisions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct PageHeader {
    /// The page ID (`<id>` directly under `<page>`).
    pub id: u64,
    /// The namespace number.
    pub ns: i64,
    /// The full title, namespace prefix included.
    pub title: String,
    /// The redirect target, when the page is a redirect.
    pub redirect: Option<String>,
}

/// A further content slot of a revision (export schema 0.11's `<content>`): every slot
/// but the main one, which [`Revision`] carries itself.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Slot {
    /// The slot role (`mediainfo`, …).
    pub role: String,
    /// The revision whose content this is (`<origin>`).
    pub origin: Option<u64>,
    /// The content model.
    pub model: Option<String>,
    /// The content format.
    pub format: Option<String>,
    /// The content, verbatim. Empty when hidden; see [`Slot::text_hidden`].
    pub text: String,
    /// The dump hid the content (`<text deleted="deleted" />`).
    pub text_hidden: bool,
    /// The `bytes` attribute of `<text>`, when present.
    pub text_bytes: Option<u64>,
    /// The dump's `<sha1>` of the content; `None` when absent or empty.
    pub sha1: Option<String>,
}

/// One revision of a page.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
// One per flag the export writes: `<minor />` and three `deleted`.
#[allow(clippy::struct_excessive_bools)]
pub struct Revision {
    /// The page this revision belongs to (repeated here so a consumer that
    /// only looks at revisions needs no state).
    pub page_id: u64,
    /// The page's namespace number.
    pub ns: i64,
    /// The revision ID.
    pub id: u64,
    /// The parent revision ID; `None` for a page's first revision.
    pub parent_id: Option<u64>,
    /// The timestamp exactly as the dump writes it (`2026-09-20T14:02:11Z`).
    pub timestamp: String,
    /// The contributor's user ID, for a registered user.
    pub user_id: Option<u64>,
    /// The contributor's user name, for a registered user.
    pub user_name: Option<String>,
    /// The contributor's IP address, for an anonymous edit.
    pub ip: Option<String>,
    /// The dump hid the contributor (`<contributor deleted="deleted" />`).
    pub user_hidden: bool,
    /// A minor edit.
    pub minor: bool,
    /// The edit summary; `None` when absent or hidden.
    pub comment: Option<String>,
    /// The dump hid the edit summary (`<comment deleted="deleted" />`).
    pub comment_hidden: bool,
    /// The revision whose main-slot content this is (`<origin>`), where the dump says.
    pub origin: Option<u64>,
    /// The content model (`wikitext`, `wikibase-item`, …).
    pub model: Option<String>,
    /// The content format (`text/x-wiki`, …).
    pub format: Option<String>,
    /// The revision text, verbatim (no trimming, entities decoded). Empty when
    /// the text is empty or hidden; see [`Revision::text_hidden`].
    pub text: String,
    /// The dump hid the text (`<text deleted="deleted" />`).
    pub text_hidden: bool,
    /// The `bytes` attribute of `<text>`, when present.
    pub text_bytes: Option<u64>,
    /// The dump's `<sha1>` of the text (MediaWiki's base-36 SHA-1); `None` when absent
    /// or empty, as it is where the text is hidden.
    pub sha1: Option<String>,
    /// The further content slots, in dump order; empty for a single-slot revision.
    pub slots: Vec<Slot>,
}

/// What the reader yields. More kinds of event may be added in a minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The `<siteinfo>` block; comes first when the dump has one.
    SiteInfo(SiteInfo),
    /// A page begins. Its revisions follow.
    PageStart(PageHeader),
    /// One revision of the current page.
    Revision(Revision),
    /// The current page ends.
    PageEnd {
        /// The page ID, matching the preceding [`Event::PageStart`].
        id: u64,
    },
}

/// What handling one XML event produced.
// Returned by value and matched at once; boxing `Event` would only add an
// allocation per revision.
#[allow(clippy::large_enum_variant)]
enum Step {
    Continue,
    Emit(Event),
    Eof,
}

/// A streaming reader over a dump. Iterate it for [`Event`]s.
pub struct DumpReader<R: BufRead> {
    reader: Reader<R>,
    buf: Vec<u8>,
    /// Local names of the open elements, outermost first.
    path: Vec<String>,
    root_lang: Option<String>,
    siteinfo: Option<SiteInfo>,
    page: Option<PageHeader>,
    page_sent: bool,
    revision: Option<Revision>,
    /// Text accumulated for the innermost leaf element.
    text: String,
    /// A `PageEnd` queued behind a `PageStart` for a page with no revisions.
    pending_end: Option<Event>,
    done: bool,
    /// Namespaces to keep; `None` keeps every page.
    namespaces: Option<Vec<i64>>,
    /// The current page's `<ns>` is not wanted: skip to its `</page>`.
    skip_page: bool,
}

impl<R: BufRead> DumpReader<R> {
    /// Wraps a decompressed XML stream.
    pub fn new(inner: R) -> Self {
        let mut reader = Reader::from_reader(inner);
        // Keep every byte of text: revision text may begin or end with whitespace.
        reader.config_mut().trim_text(false);
        reader.config_mut().expand_empty_elements = false;
        Self {
            reader,
            buf: Vec::with_capacity(64 * 1024),
            path: Vec::with_capacity(8),
            root_lang: None,
            siteinfo: None,
            page: None,
            page_sent: false,
            revision: None,
            text: String::new(),
            pending_end: None,
            done: false,
            namespaces: None,
            skip_page: false,
        }
    }

    /// Keeps only pages in these namespaces. Any other page yields no events
    /// at all (no `PageStart`, revisions or `PageEnd`), and its revisions are
    /// skipped at the XML level, so their text is never decoded or copied
    /// into strings. The decision is made at the page's `<ns>` element; a
    /// page without one counts as namespace 0, as in [`PageHeader::ns`].
    #[must_use]
    pub fn with_namespaces(mut self, namespaces: &[i64]) -> Self {
        self.namespaces = Some(namespaces.to_vec());
        self
    }

    fn wanted(&self, ns: i64) -> bool {
        self.namespaces
            .as_ref()
            .is_none_or(|list| list.contains(&ns))
    }

    /// Consumes the rest of the current page through its `</page>`, without
    /// building any events; quick-xml clears `buf` after each XML event, so at
    /// most one revision's raw text is held at a time.
    fn skip_rest_of_page(&mut self) -> Result<(), DumpError> {
        self.skip_page = false;
        self.buf.clear();
        self.reader
            .read_to_end_into(QName("page"), &mut self.buf)
            .map_err(|e| self.xml_err(e))?;
        if self.path.last().map(String::as_str) == Some("page") {
            self.path.pop();
        }
        self.page = None;
        self.revision = None;
        self.text.clear();
        Ok(())
    }

    fn xml_err(&self, source: quick_xml::Error) -> DumpError {
        DumpError::Xml {
            position: self.reader.buffer_position(),
            source,
        }
    }

    fn parent(&self) -> Option<&str> {
        self.path
            .len()
            .checked_sub(2)
            .map(|i| self.path[i].as_str())
    }

    fn attr(e: &BytesStart<'_>, name: &str) -> Option<String> {
        e.attributes()
            .flatten()
            .find(|a| a.key.local_name().as_ref() == name)
            .and_then(|a| {
                a.normalized_value(quick_xml::XmlVersion::default())
                    .ok()
                    .map(|v| v.into_owned())
            })
    }

    /// Handles the start of an element (also called for empty elements).
    fn on_start(&mut self, e: &BytesStart<'_>, empty: bool) -> Result<Option<Event>, DumpError> {
        let name = e.local_name().as_ref().to_owned();
        self.text.clear();
        let mut out = None;
        match name.as_str() {
            "mediawiki" if self.path.is_empty() => {
                self.root_lang = e
                    .attributes()
                    .flatten()
                    .find(|a| a.key.as_ref() == "xml:lang")
                    .and_then(|a| {
                        a.normalized_value(quick_xml::XmlVersion::default())
                            .ok()
                            .map(|v| v.into_owned())
                    });
            }
            "siteinfo" => {
                self.siteinfo = Some(SiteInfo {
                    lang: self.root_lang.clone(),
                    ..SiteInfo::default()
                });
            }
            // `name` is not on the path yet, so the innermost open element is the parent.
            "namespace" if self.path.last().map(String::as_str) == Some("namespaces") => {
                if let Some(si) = self.siteinfo.as_mut() {
                    let key = Self::attr(e, "key")
                        .and_then(|k| k.parse().ok())
                        .unwrap_or(0);
                    si.namespaces.push(Namespace {
                        key,
                        name: String::new(),
                        case: Self::attr(e, "case"),
                    });
                }
            }
            "page" => {
                self.page = Some(PageHeader::default());
                self.page_sent = false;
            }
            "redirect" => {
                if let Some(p) = self.page.as_mut() {
                    p.redirect = Self::attr(e, "title");
                }
            }
            "revision" => {
                // The page header is complete once its first revision starts.
                out = self.emit_page_start();
                let (page_id, ns) = self.page.as_ref().map_or((0, 0), |p| (p.id, p.ns));
                self.revision = Some(Revision {
                    page_id,
                    ns,
                    ..Revision::default()
                });
            }
            "contributor" => {
                if let Some(r) = self.revision.as_mut() {
                    if Self::attr(e, "deleted").is_some() {
                        r.user_hidden = true;
                    }
                }
            }
            "minor" => {
                if let Some(r) = self.revision.as_mut() {
                    r.minor = true;
                }
            }
            // `name` is not on the path yet, so the innermost open element is the parent.
            "content" if self.path.last().map(String::as_str) == Some("revision") => {
                if let Some(r) = self.revision.as_mut() {
                    r.slots.push(Slot::default());
                }
            }
            "text" => {
                let bytes = Self::attr(e, "bytes").and_then(|b| b.parse().ok());
                let hidden = Self::attr(e, "deleted").is_some();
                match self.path.last().map(String::as_str) {
                    Some("revision") => self.rv(|r| {
                        r.text_bytes = bytes;
                        r.text_hidden |= hidden;
                    }),
                    Some("content") => self.sl(|s| {
                        s.text_bytes = bytes;
                        s.text_hidden |= hidden;
                    }),
                    _ => {}
                }
            }
            "comment" if self.path.last().map(String::as_str) == Some("revision") => {
                if let Some(r) = self.revision.as_mut() {
                    if Self::attr(e, "deleted").is_some() {
                        r.comment = None;
                        r.comment_hidden = true;
                    } else if empty {
                        r.comment = Some(String::new());
                    }
                }
            }
            _ => {}
        }
        if !empty {
            self.path.push(name);
        }
        Ok(out)
    }

    fn emit_page_start(&mut self) -> Option<Event> {
        if self.page_sent {
            return None;
        }
        let p = self.page.as_ref()?;
        self.page_sent = true;
        Some(Event::PageStart(p.clone()))
    }

    /// Handles the end of an element whose text has been accumulated.
    fn on_end(&mut self, name: &str) -> Option<Event> {
        let text = std::mem::take(&mut self.text);
        let parent = self.parent().map(str::to_owned);
        let in_revision = self.revision.is_some();
        let mut out = None;
        match (name, parent.as_deref()) {
            // siteinfo
            ("sitename", Some("siteinfo")) => self.si(|s| s.sitename = Some(text)),
            ("dbname", Some("siteinfo")) => self.si(|s| s.dbname = Some(text)),
            ("base", Some("siteinfo")) => self.si(|s| s.base = Some(text)),
            ("generator", Some("siteinfo")) => self.si(|s| s.generator = Some(text)),
            ("case", Some("siteinfo")) => self.si(|s| s.case = Some(text)),
            ("namespace", Some("namespaces")) => self.si(|s| {
                if let Some(ns) = s.namespaces.last_mut() {
                    ns.name = text;
                }
            }),
            ("siteinfo", _) => {
                if let Some(si) = self.siteinfo.take() {
                    out = Some(Event::SiteInfo(si));
                }
            }
            // page header
            ("title", Some("page")) => self.pg(|p| p.title = text),
            ("ns", Some("page")) => {
                self.pg(|p| p.ns = text.trim().parse().unwrap_or(0));
                let ns = self.page.as_ref().map_or(0, |p| p.ns);
                // Only before any revision: `<ns>` precedes them in every export format.
                if !self.page_sent && !self.wanted(ns) {
                    self.skip_page = true;
                }
            }
            ("id", Some("page")) => self.pg(|p| p.id = text.trim().parse().unwrap_or(0)),
            // revision
            ("id", Some("revision")) => self.rv(|r| r.id = text.trim().parse().unwrap_or(0)),
            ("parentid", Some("revision")) => self.rv(|r| r.parent_id = text.trim().parse().ok()),
            ("timestamp", Some("revision")) => self.rv(|r| r.timestamp = text.trim().to_string()),
            ("comment", Some("revision")) => self.rv(|r| {
                if !r.comment_hidden {
                    r.comment = Some(text);
                }
            }),
            ("origin", Some("revision")) => self.rv(|r| r.origin = text.trim().parse().ok()),
            ("model", Some("revision")) => self.rv(|r| r.model = Some(text.trim().to_string())),
            ("format", Some("revision")) => self.rv(|r| r.format = Some(text.trim().to_string())),
            ("sha1", Some("revision")) => self.rv(|r| r.sha1 = non_empty(&text)),
            ("text", Some("revision")) => self.rv(|r| r.text = text),
            // a further slot
            ("role", Some("content")) => self.sl(|s| s.role = text.trim().to_string()),
            ("origin", Some("content")) => self.sl(|s| s.origin = text.trim().parse().ok()),
            ("model", Some("content")) => self.sl(|s| s.model = Some(text.trim().to_string())),
            ("format", Some("content")) => self.sl(|s| s.format = Some(text.trim().to_string())),
            ("text", Some("content")) => self.sl(|s| s.text = text),
            ("sha1", Some("content")) => self.sl(|s| s.sha1 = non_empty(&text)),
            ("username", Some("contributor")) if in_revision => {
                self.rv(|r| r.user_name = Some(text))
            }
            ("id", Some("contributor")) if in_revision => {
                self.rv(|r| r.user_id = text.trim().parse().ok())
            }
            ("ip", Some("contributor")) if in_revision => {
                self.rv(|r| r.ip = Some(text.trim().to_string()))
            }
            ("revision", _) => {
                if let Some(r) = self.revision.take() {
                    out = Some(Event::Revision(r));
                }
            }
            ("page", _) => {
                // A page with no revisions still gets its start event.
                let start = self.emit_page_start();
                if let Some(p) = self.page.take() {
                    out = Some(match start {
                        // Both events are due; keep the start and queue the end.
                        Some(s) => {
                            self.pending_end = Some(Event::PageEnd { id: p.id });
                            s
                        }
                        None => Event::PageEnd { id: p.id },
                    });
                }
            }
            _ => {}
        }
        out
    }

    fn si(&mut self, f: impl FnOnce(&mut SiteInfo)) {
        if let Some(s) = self.siteinfo.as_mut() {
            f(s);
        }
    }
    fn pg(&mut self, f: impl FnOnce(&mut PageHeader)) {
        if let Some(p) = self.page.as_mut() {
            f(p);
        }
    }
    fn rv(&mut self, f: impl FnOnce(&mut Revision)) {
        if let Some(r) = self.revision.as_mut() {
            f(r);
        }
    }
    fn sl(&mut self, f: impl FnOnce(&mut Slot)) {
        if let Some(s) = self.revision.as_mut().and_then(|r| r.slots.last_mut()) {
            f(s);
        }
    }
}

fn non_empty(text: &str) -> Option<String> {
    let t = text.trim();
    (!t.is_empty()).then(|| t.to_string())
}

impl<R: BufRead> DumpReader<R> {
    fn next_event(&mut self) -> Result<Option<Event>, DumpError> {
        if let Some(e) = self.pending_end.take() {
            return Ok(Some(e));
        }
        loop {
            if self.skip_page {
                self.skip_rest_of_page()?;
            }
            // The buffer is taken out of `self` so the event can borrow it
            // while the handler mutates `self`: no copy of each event (the
            // revision text included), which `into_owned` used to make.
            let mut buf = std::mem::take(&mut self.buf);
            buf.clear();
            let step = match self.reader.read_event_into(&mut buf) {
                Ok(ev) => self.handle(ev),
                Err(e) => Err(self.xml_err(e)),
            };
            self.buf = buf;
            match step? {
                Step::Continue => {}
                Step::Emit(e) => return Ok(Some(e)),
                Step::Eof => return Ok(None),
            }
        }
    }

    fn handle(&mut self, ev: XmlEvent<'_>) -> Result<Step, DumpError> {
        match ev {
            XmlEvent::Start(e) => {
                if let Some(out) = self.on_start(&e, false)? {
                    return Ok(Step::Emit(out));
                }
            }
            XmlEvent::Empty(e) => {
                let name = e.local_name().as_ref().to_owned();
                let out = self.on_start(&e, true)?;
                // An empty element also "ends": `<text … />`, `<namespace key="0" />`.
                self.path.push(name.clone());
                let end = self.on_end(&name);
                self.path.pop();
                if let Some(out) = out.or(end) {
                    return Ok(Step::Emit(out));
                }
            }
            XmlEvent::End(_) => {
                let Some(name) = self.path.pop() else {
                    return Ok(Step::Continue);
                };
                // on_end needs the element still on the path to find its parent.
                self.path.push(name.clone());
                let out = self.on_end(&name);
                self.path.pop();
                if let Some(out) = out {
                    return Ok(Step::Emit(out));
                }
            }
            // Text comes decoded; each entity reference between its runs is an
            // event of its own (`&amp;`, `&#10;`), resolved here.
            XmlEvent::Text(e) => {
                if self.wants_text() {
                    self.text.push_str(&e);
                }
            }
            XmlEvent::GeneralRef(r) => {
                if self.wants_text() {
                    match r.resolve_char_ref().map_err(|e| self.xml_err(e))? {
                        Some(c) => self.text.push(c),
                        None => {
                            let name: &str = &r;
                            self.text.push_str(match name {
                                "amp" => "&",
                                "lt" => "<",
                                "gt" => ">",
                                "quot" => "\"",
                                "apos" => "'",
                                other => {
                                    return Err(DumpError::Malformed(format!(
                                        "unknown entity reference `&{other};` at byte {}",
                                        self.reader.buffer_position()
                                    )));
                                }
                            });
                        }
                    }
                }
            }
            XmlEvent::CData(e) => {
                if self.wants_text() {
                    self.text.push_str(&e);
                }
            }
            XmlEvent::Eof => {
                self.done = true;
                // quick-xml reports a truncated document as a plain end of
                // input; an element still open at EOF means the dump was cut.
                if let Some(open) = self.path.last() {
                    return Err(DumpError::Malformed(format!(
                        "unexpected end of input inside <{open}> at byte {}",
                        self.reader.buffer_position()
                    )));
                }
                return Ok(Step::Eof);
            }
            XmlEvent::Decl(_) | XmlEvent::PI(_) | XmlEvent::Comment(_) | XmlEvent::DocType(_) => {}
        }
        Ok(Step::Continue)
    }

    /// Only leaf elements we read get their text accumulated; this keeps the
    /// indentation between elements out of the buffers.
    fn wants_text(&self) -> bool {
        matches!(
            self.path.last().map(String::as_str),
            Some(
                "sitename"
                    | "dbname"
                    | "base"
                    | "generator"
                    | "case"
                    | "namespace"
                    | "title"
                    | "ns"
                    | "id"
                    | "parentid"
                    | "origin"
                    | "role"
                    | "timestamp"
                    | "comment"
                    | "model"
                    | "format"
                    | "sha1"
                    | "text"
                    | "username"
                    | "ip"
            )
        )
    }
}

impl<R: BufRead> Iterator for DumpReader<R> {
    type Item = Result<Event, DumpError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_event() {
            Ok(Some(e)) => Some(Ok(e)),
            Ok(None) => None,
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
