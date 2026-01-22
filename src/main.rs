use bzip2::read::BzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::env;
use std::fs::File;
use std::io::{self, BufReader, Read};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <wikipedia_dump.xml.bz2|7z>", args[0]);
        std::process::exit(1);
    }

    let path = &args[1];
    let input_reader: Box<dyn Read> = if path.ends_with(".7z") {
        let mut reader = sevenz_rust::SevenZReader::open(path, sevenz_rust::Password::empty())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Since we expect one file in the 7z (Wikipedia dump), we read the first one.
        // Unfortunately sevenz-rust seems to require reading the whole entry into memory
        // or extracting to disk if we want to treat it as a stream easily, 
        // because SevenZReader::read_values uses a callback.
        
        // Let's try to find if there is a better way. 
        // Actually, Wikipedia 7z can be very large.
        
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
        let file = File::open(path)?;
        Box::new(BzDecoder::new(file))
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
                        // Clear revision metadata at start of revision
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
                        
                        // Default user_id to "0" if it was not found in <contributor>
                        let display_user_id = if user_id.is_empty() { "0" } else { &user_id };

                        // Print the mwrev format
                        println!(
                            "# page_id={} ns={} rev_id={} parent_rev_id={} timestamp={} user_id={}",
                            current_page_id, current_ns, rev_id, parent_rev_id, timestamp, display_user_id
                        );
                        for line in text.lines() {
                            println!(" {}", line);
                        }

                        // Clear revision metadata
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

    Ok(())
}
