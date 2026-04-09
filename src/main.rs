use anyhow::{Context, Result};
use calamine::{open_workbook_from_rs, Data, DataType, Reader, Xlsx};
use clap::Parser;
use csv::WriterBuilder;
use flate2::{write::GzEncoder, Compression};
use rayon::ThreadPoolBuilder;
use std::{
    fs,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use zip::ZipArchive;

#[derive(Parser, Debug)]
#[command(author, version, about = "Convert xlsx files inside a zip to csv.gz")]
struct Args {
    /// Path to the input .zip file
    zip_path: PathBuf,

    /// Number of parallel workers
    #[arg(short, long, default_value_t = 2)]
    parallel: usize,
}

fn cell_to_string(cell: &Data) -> String {
    match cell {
        Data::Empty => String::new(),
        Data::String(s) => s.clone(),
        Data::Float(f) => {
            // Avoid scientific notation for large/small numbers when they are integers
            if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("{}", *f as i64)
            } else {
                format!("{}", f)
            }
        }
        Data::Int(i) => format!("{}", i),
        Data::Bool(b) => format!("{}", b),
        Data::Error(e) => format!("{:?}", e),
        Data::DateTime(dt) => format!("{}", dt),
        Data::DateTimeIso(s) => s.clone(),
        Data::DurationIso(s) => s.clone(),
        _ => String::new(),
    }
}

/// Read all bytes of a zip entry by index into memory.
/// This is necessary because ZipFile borrows the archive mutably and cannot be
/// sent across threads; we extract to Vec<u8> first, then parse on the worker.
fn extract_entry_bytes(zip_path: &Path, entry_index: usize) -> Result<Vec<u8>> {
    let file = fs::File::open(zip_path)
        .with_context(|| format!("Opening zip file {:?}", zip_path))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| "Parsing zip archive")?;
    let mut entry = archive
        .by_index(entry_index)
        .with_context(|| format!("Accessing zip entry {}", entry_index))?;
    let mut buf = Vec::with_capacity(entry.size() as usize);
    entry.read_to_end(&mut buf)?;
    Ok(buf)
}

/// Convert one sheet to a gzip-compressed CSV written to `out_path`.
/// Uses a streaming CSV writer feeding into GzEncoder — no full CSV in memory.
fn sheet_to_csv_gz(
    sheet_data: &calamine::Range<Data>,
    out_path: &Path,
) -> Result<()> {
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let out_file = fs::File::create(out_path)
        .with_context(|| format!("Creating output file {:?}", out_path))?;

    let gz = GzEncoder::new(out_file, Compression::default());
    let mut csv_writer = WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_writer(gz);

    let (height, width) = sheet_data.get_size();
    let start = sheet_data.start().unwrap_or((0, 0));

    for row_idx in 0..height as u32 {
        let actual_row = start.0 + row_idx;
        let mut record: Vec<String> = Vec::with_capacity(width);
        for col_idx in 0..width as u32 {
            let actual_col = start.1 + col_idx;
            let cell = sheet_data.get_value((actual_row, actual_col));
            record.push(cell.map(cell_to_string).unwrap_or_default());
        }
        csv_writer
            .write_record(&record)
            .with_context(|| format!("Writing CSV row {}", row_idx))?;
    }

    // Flush CSV then finish gzip stream
    let gz_inner = csv_writer
        .into_inner()
        .map_err(|e| anyhow::anyhow!("CSV flush error: {}", e))?;
    gz_inner.finish().with_context(|| "Finishing gzip stream")?;

    Ok(())
}

/// Process one xlsx file (given as raw bytes with a logical path inside the zip).
fn process_xlsx(xlsx_bytes: Vec<u8>, xlsx_zip_path: &str, output_root: &Path) -> Result<()> {
    let cursor = Cursor::new(xlsx_bytes);
    let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)
        .with_context(|| format!("Parsing xlsx: {}", xlsx_zip_path))?;

    let sheet_names: Vec<String> = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        eprintln!("  [skip] {} has no sheets", xlsx_zip_path);
        return Ok(());
    }

    // Build base output dir: output_root / xlsx_zip_path (without extension)
    let xlsx_path = Path::new(xlsx_zip_path);
    let stem = xlsx_path
        .file_stem()
        .unwrap_or_else(|| xlsx_path.as_os_str())
        .to_string_lossy();
    let parent = xlsx_path.parent().unwrap_or_else(|| Path::new(""));
    let base_dir = output_root.join(parent).join(stem.as_ref());

    for sheet_name in &sheet_names {
        let range = match workbook.worksheet_range(sheet_name) {
            Ok(r) => r,
            Err(e) => {
                eprintln!(
                    "  [warn] Could not read sheet '{}' in {}: {}",
                    sheet_name, xlsx_zip_path, e
                );
                continue;
            }
        };

        // Sanitise sheet name for use as a filename
        let safe_name: String = sheet_name
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
            .collect();

        let out_path = base_dir.join(format!("{}.csv.gz", safe_name));
        println!("  -> {:?}", out_path);
        sheet_to_csv_gz(&range, &out_path)
            .with_context(|| format!("Sheet '{}' in {}", sheet_name, xlsx_zip_path))?;
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let zip_path = Arc::new(args.zip_path.clone());
    let output_root = Arc::new(
        zip_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
    );

    // Collect xlsx entries from the zip (index + name)
    let xlsx_entries: Vec<(usize, String)> = {
        let file = fs::File::open(&*zip_path)
            .with_context(|| format!("Opening {:?}", zip_path))?;
        let mut archive = ZipArchive::new(file)?;
        (0..archive.len())
            .filter_map(|i| {
                let entry = archive.by_index(i).ok()?;
                let name = entry.name().to_string();
                if name.to_lowercase().ends_with(".xlsx") && !name.starts_with("__MACOSX") {
                    Some((i, name))
                } else {
                    None
                }
            })
            .collect()
    };

    if xlsx_entries.is_empty() {
        eprintln!("No .xlsx files found in {:?}", zip_path);
        return Ok(());
    }

    println!(
        "Found {} xlsx file(s) in {:?}, using {} parallel worker(s)",
        xlsx_entries.len(),
        zip_path,
        args.parallel
    );

    // Build a fixed-size thread pool
    let pool = ThreadPoolBuilder::new()
        .num_threads(args.parallel)
        .build()
        .context("Building thread pool")?;

    // Collect errors from workers
    let (err_tx, err_rx) = crossbeam_channel::unbounded::<String>();

    pool.scope(|s| {
        for (idx, name) in &xlsx_entries {
            let zip_path = Arc::clone(&zip_path);
            let output_root = Arc::clone(&output_root);
            let err_tx = err_tx.clone();
            let name = name.clone();
            let idx = *idx;

            s.spawn(move |_| {
                println!("Processing: {}", name);
                match extract_entry_bytes(&zip_path, idx) {
                    Err(e) => {
                        let _ = err_tx.send(format!("Extract '{}': {:#}", name, e));
                    }
                    Ok(bytes) => {
                        if let Err(e) = process_xlsx(bytes, &name, &output_root) {
                            let _ = err_tx.send(format!("Convert '{}': {:#}", name, e));
                        }
                    }
                }
            });
        }
    });

    drop(err_tx);

    let mut had_errors = false;
    for msg in err_rx {
        eprintln!("[error] {}", msg);
        had_errors = true;
    }

    if had_errors {
        std::process::exit(1);
    }

    println!("Done.");
    Ok(())
}