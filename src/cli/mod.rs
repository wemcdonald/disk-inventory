pub mod output;

use crate::config::Config;
use crate::db::Database;
use crate::query;
use anyhow::Result;

/// Output format enum (used in clap args)
#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

fn open_db() -> Result<Database> {
    let config = Config::load()?;
    let db_path = config.db_path();
    if !db_path.exists() {
        anyhow::bail!(
            "No index found at {}. Run `disk-inventory daemon run` first.",
            db_path.display()
        );
    }
    Database::open(db_path)
}

pub fn run_usage(path: Option<String>, depth: u32, format: &OutputFormat) -> Result<()> {
    let db = open_db()?;
    let result = query::query_overview(&db, path.as_deref(), depth)?;
    match format {
        OutputFormat::Json => output::print_json(&result),
        OutputFormat::Csv => {
            let headers = &["Path", "Size", "Files", "%"];
            let rows: Vec<Vec<String>> = result
                .children
                .iter()
                .map(|c| {
                    vec![
                        c.name.clone(),
                        c.total_size_human.clone(),
                        c.file_count.to_string(),
                        format!("{:.1}", c.percentage),
                    ]
                })
                .collect();
            output::print_csv(headers, &rows);
        }
        OutputFormat::Table => {
            println!("Disk usage for: {}", result.path);
            println!(
                "Total: {} ({} files, {} dirs)\n",
                result.total_size_human, result.file_count, result.dir_count
            );
            let headers = &["Name", "Size", "Files", "%"];
            let rows: Vec<Vec<String>> = result
                .children
                .iter()
                .map(|c| {
                    vec![
                        c.name.clone(),
                        c.total_size_human.clone(),
                        c.file_count.to_string(),
                        format!("{:.1}%", c.percentage),
                    ]
                })
                .collect();
            output::print_table(headers, &rows);
        }
    }
    Ok(())
}

pub fn run_top(
    path: Option<String>,
    files_only: bool,
    dirs_only: bool,
    extensions: Option<String>,
    older: Option<u32>,
    limit: u32,
    format: &OutputFormat,
) -> Result<()> {
    let db = open_db()?;
    let item_type = if files_only {
        "files"
    } else if dirs_only {
        "directories"
    } else {
        "both"
    };
    let ext_vec: Option<Vec<String>> = extensions
        .map(|e| e.split(',').map(|s| s.trim().to_string()).collect());
    let result =
        query::query_large_items(&db, path.as_deref(), item_type, 0, limit, ext_vec.as_deref(), older)?;
    match format {
        OutputFormat::Json => output::print_json(&result),
        _ => {
            let headers = &["Path", "Size", "Type", "Modified"];
            let rows: Vec<Vec<String>> = result
                .items
                .iter()
                .map(|item| {
                    vec![
                        item.path.clone(),
                        item.size_human.clone(),
                        item.item_type.clone(),
                        item.modified
                            .and_then(|m| {
                                chrono::DateTime::from_timestamp(m, 0)
                                    .map(|d| d.format("%Y-%m-%d").to_string())
                            })
                            .unwrap_or_default(),
                    ]
                })
                .collect();
            if matches!(format, OutputFormat::Csv) {
                output::print_csv(headers, &rows);
            } else {
                output::print_table(headers, &rows);
            }
        }
    }
    Ok(())
}

pub fn run_search(
    pattern: String,
    path: Option<String>,
    limit: u32,
    format: &OutputFormat,
) -> Result<()> {
    let db = open_db()?;
    let result = query::query_search(&db, Some(&pattern), path.as_deref(), None, None, limit)?;
    match format {
        OutputFormat::Json => output::print_json(&result),
        _ => {
            let headers = &["Path", "Size", "Modified"];
            let rows: Vec<Vec<String>> = result
                .files
                .iter()
                .map(|f| {
                    vec![
                        f.path.clone(),
                        f.size_human.clone(),
                        f.modified
                            .and_then(|m| {
                                chrono::DateTime::from_timestamp(m, 0)
                                    .map(|d| d.format("%Y-%m-%d").to_string())
                            })
                            .unwrap_or_default(),
                    ]
                })
                .collect();
            if matches!(format, OutputFormat::Csv) {
                output::print_csv(headers, &rows);
            } else {
                output::print_table(headers, &rows);
            }
        }
    }
    Ok(())
}

pub fn run_waste(
    path: Option<String>,
    category: Option<String>,
    min_size: Option<u64>,
    format: &OutputFormat,
) -> Result<()> {
    let db = open_db()?;
    let config = crate::config::Config::load()?;
    let categories = category
        .map(|c| vec![c])
        .unwrap_or_else(|| vec!["all".to_string()]);
    let min = min_size.unwrap_or(0);
    let results = crate::waste::detect_waste(
        &db,
        path.as_deref(),
        &categories,
        min,
        &config.waste.disabled_categories,
    )?;

    match format {
        OutputFormat::Json => output::print_json(&results),
        _ => {
            if results.is_empty() {
                println!("No reclaimable space found.");
                return Ok(());
            }
            let mut total_waste: u64 = 0;
            for cat in &results {
                total_waste += cat.total_size;
                println!(
                    "\n{} ({}) - {}",
                    cat.category,
                    cat.safety.as_str(),
                    cat.total_size_human
                );
                println!("  {}", cat.description);
                if let Some(cmd) = &cat.cleanup_command {
                    println!("  Cleanup: {}", cmd);
                }
                let headers = &["Path", "Size"];
                let rows: Vec<Vec<String>> = cat
                    .items
                    .iter()
                    .take(5)
                    .map(|item| {
                        vec![
                            item.path.clone(),
                            crate::models::format_size(item.size_bytes),
                        ]
                    })
                    .collect();
                if !rows.is_empty() {
                    if matches!(format, OutputFormat::Csv) {
                        output::print_csv(headers, &rows);
                    } else {
                        output::print_table(headers, &rows);
                    }
                }
            }
            println!(
                "\nTotal reclaimable: {}",
                crate::models::format_size(total_waste)
            );
        }
    }
    Ok(())
}

pub fn run_trends(
    path: Option<String>,
    period: &str,
    limit: u32,
    format: &OutputFormat,
) -> Result<()> {
    let db = open_db()?;
    let result = crate::query::query_trends(&db, path.as_deref(), period, "absolute_growth", limit)?;

    match format {
        OutputFormat::Json => output::print_json(&result),
        _ => {
            if result.trends.is_empty() {
                println!("No trend data available yet. Run multiple scans to build history.");
                return Ok(());
            }
            println!("Disk usage trends ({})\n", result.period);
            let headers = &["Path", "Current", "Growth", "%"];
            let rows: Vec<Vec<String>> = result
                .trends
                .iter()
                .map(|t| {
                    vec![
                        t.path.clone(),
                        t.current_size_human.clone(),
                        t.growth_human.clone(),
                        format!("{:.1}%", t.growth_percent),
                    ]
                })
                .collect();
            if matches!(format, OutputFormat::Csv) {
                output::print_csv(headers, &rows);
            } else {
                output::print_table(headers, &rows);
            }
        }
    }
    Ok(())
}

pub fn run_duplicates(
    path: Option<String>,
    min_size: u64,
    limit: u32,
    format: &OutputFormat,
) -> Result<()> {
    let db = open_db()?;
    let result = crate::duplicates::find_duplicates(&db, path.as_deref(), min_size, None, limit)?;

    match format {
        OutputFormat::Json => output::print_json(&result),
        _ => {
            if result.is_empty() {
                println!("No duplicates found.");
                return Ok(());
            }
            let mut total_wasted: u64 = 0;
            for group in &result {
                total_wasted += group.wasted_bytes;
            }
            println!(
                "Found {} duplicate groups (wasting {})\n",
                result.len(),
                crate::models::format_size(total_wasted)
            );
            let headers = &["Size", "Wasted", "Copies", "Paths"];
            let rows: Vec<Vec<String>> = result
                .iter()
                .map(|g| {
                    let paths = g
                        .files
                        .iter()
                        .map(|f| f.path.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    vec![
                        crate::models::format_size(g.size_bytes),
                        crate::models::format_size(g.wasted_bytes),
                        g.files.len().to_string(),
                        paths,
                    ]
                })
                .collect();
            if matches!(format, OutputFormat::Csv) {
                output::print_csv(headers, &rows);
            } else {
                output::print_table(headers, &rows);
            }
        }
    }
    Ok(())
}

pub fn run_types(path: Option<String>, limit: u32, format: &OutputFormat) -> Result<()> {
    let db = open_db()?;
    let result = query::query_usage_by_type(&db, path.as_deref(), limit)?;
    match format {
        OutputFormat::Json => output::print_json(&result),
        _ => {
            let headers = &["Extension", "Count", "Total Size", "%"];
            let rows: Vec<Vec<String>> = result
                .types
                .iter()
                .map(|t| {
                    vec![
                        t.extension.clone(),
                        t.file_count.to_string(),
                        t.total_size_human.clone(),
                        format!("{:.1}%", t.percentage),
                    ]
                })
                .collect();
            if matches!(format, OutputFormat::Csv) {
                output::print_csv(headers, &rows);
            } else {
                output::print_table(headers, &rows);
            }
        }
    }
    Ok(())
}
