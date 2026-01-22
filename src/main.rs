use bzip2::read::BzDecoder;
use chrono::Local;
use clap::Parser;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Wikipedia dump file (.bz2 or .7z)
    input: Option<PathBuf>,

    /// Directory containing Wikipedia dump files
    #[arg(short = 'd')]
    input_dir: Option<PathBuf>,

    /// Output directory for .mwrev.zst files
    #[arg(short = 'o')]
    output_dir: Option<PathBuf>,
}

fn process_file(input_path: &Path, output_dir: Option<&Path>) -> io::Result<()> {
    let filename = input_path.file_name().unwrap().to_string_lossy();
    eprintln!("[{}] Starting {}", Local::now().format("%Y-%m-%d %H:%M:%S"), filename);

    let input_reader: Box<dyn Read> = if input_path.extension().map_or(false, |ext| ext == "7z") {
        let mut reader = sevenz_rust::SevenZReader::open(input_path, sevenz_rust::Password::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        let mut first_file_content = Vec::new();
        reader.for_each_entries(|entry, reader| {
            if entry.is_directory() {
                return Ok(true);
            }
            reader.read_to_end(&mut first_file_content)?;
            Ok(false) // stop after first file
        }).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
        Box::new(io::Cursor::new(first_file_content))
    } else {
        let file = File::open(input_path)?;
        Box::new(BzDecoder::new(file))
    };

    let mut writer: Box<dyn Write> = if let Some(out_dir) = output_dir {
        fs::create_dir_all(out_dir)?;
        let mut out_name = filename.to_string();
        if out_name.ends_with(".bz2") {
            out_name = out_name.strip_suffix(".bz2").unwrap().to_string();
        } else if out_name.ends_with(".7z") {
            out_name = out_name.strip_suffix(".7z").unwrap().to_string();
        }
        out_name.push_str(".mwrev.zst");
        let out_path = out_dir.join(out_name);
        let file = File::create(out_path)?;
        Box::new(zstd::Encoder::new(file, 0)?.auto_finish())
    } else {
        Box::new(io::stdout())
    };

    let mut reader = Reader::from_reader(BufReader::new(input_reader));
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut current_page_id = String::new();
    let mut current_ns = String::new();
    
    let mut current_tag = String::new();
    let mut in_page = false;
    let mut in_revision = false;
    let mut in_contributor = false;

    // Revision metadata
    let mut rev_id = String::new();
    let mut parent_rev_id = String::new();
    let mut timestamp = String::new();
    let mut user_id = String::new();
    let mut text = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "page" => in_page = true,
                    "revision" => {
                        in_revision = true;
                        rev_id.clear();
                        parent_rev_id.clear();
                        timestamp.clear();
                        user_id.clear();
                        text.clear();
                    }
                    "contributor" => in_contributor = true,
                    _ => {}
                }
                current_tag = name;
            }
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                match name.as_str() {
                    "page" => {
                        in_page = false;
                        current_page_id.clear();
                        current_ns.clear();
                    }
                    "revision" => {
                        in_revision = false;
                        let display_user_id = if user_id.is_empty() { "0" } else { &user_id };

                        writeln!(
                            writer,
                            "# page_id={} ns={} rev_id={} parent_rev_id={} timestamp={} user_id={}",
                            current_page_id, current_ns, rev_id, parent_rev_id, timestamp, display_user_id
                        )?;
                        for line in text.lines() {
                            writeln!(writer, " {}", line)?;
                        }

                        rev_id.clear();
                        parent_rev_id.clear();
                        timestamp.clear();
                        user_id.clear();
                        text.clear();
                    }
                    "contributor" => in_contributor = false,
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(ref e)) => {
                let content = e.unescape().unwrap_or_default().into_owned();
                match current_tag.as_str() {
                    "id" => {
                        if in_contributor {
                            user_id = content;
                        } else if in_revision {
                            rev_id = content;
                        } else if in_page {
                            current_page_id = content;
                        }
                    }
                    "ns" => {
                        if in_page {
                            current_ns = content;
                        }
                    }
                    "parentid" => {
                        if in_revision {
                            parent_rev_id = content;
                        }
                    }
                    "timestamp" => {
                        if in_revision {
                            timestamp = content;
                        }
                    }
                    "text" => {
                        if in_revision {
                            text = content;
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("Error at position {}: {:?}", reader.buffer_position(), e);
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    eprintln!("[{}] Finished {}", Local::now().format("%Y-%m-%d %H:%M:%S"), filename);
    Ok(())
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    if let Some(input_dir) = args.input_dir {
        let output_dir = args.output_dir.as_deref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "-o is required when using -d")
        })?;

        let entries: Vec<PathBuf> = fs::read_dir(input_dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file() && (path.extension() == Some("bz2".as_ref()) || path.extension() == Some("7z".as_ref()))
            })
            .collect();

        entries.into_par_iter().for_each(|path| {
            if let Err(e) = process_file(&path, Some(output_dir)) {
                eprintln!("Error processing {:?}: {}", path, e);
            }
        });
    } else if let Some(input_path) = args.input {
        process_file(&input_path, args.output_dir.as_deref())?;
    } else {
        eprintln!("Usage: RevisionChest <wikipedia_dump.xml.bz2|7z> OR -d <input_dir> -o <output_dir>");
        std::process::exit(1);
    }

    Ok(())
}
