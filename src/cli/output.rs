use comfy_table::{ContentArrangement, Table};
use serde::Serialize;

pub fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(headers);
    for row in rows {
        table.add_row(row);
    }
    println!("{table}");
}

pub fn print_json<T: Serialize>(value: &T) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

pub fn print_csv(headers: &[&str], rows: &[Vec<String>]) {
    println!("{}", headers.join(","));
    for row in rows {
        println!("{}", row.join(","));
    }
}
