#![feature(str_as_str)]

use anyhow::{Result, bail};
use codecrafters_sqlite::SQLite;

fn main() -> Result<()> {
  // Parse arguments
  let args = std::env::args().collect::<Vec<_>>();
  match args.len() {
    0 | 1 => bail!("Missing <database path> and <command>"),
    2 => bail!("Missing <command>"),
    _ => {}
  }

  handle_command(&args[2], &args)?;

  Ok(())
}

fn handle_command(command: &str, args: &[String]) -> anyhow::Result<()> {
  let mut sqlite = SQLite::open(&args[1])?;
  match command.as_str() {
    ".tables" => sqlite.list_tables(),
    ".dbinfo" => sqlite.print_db_info(),
    // check if the commands starts with `select count(*) from` in any case
    _ if command.to_lowercase().starts_with("select count(*) from") => {
      let count = sqlite.count_table_rows(command)?;
      println!("{count}");
      Ok(())
    }
    _ => anyhow::bail!("Unknown command: {}", command),
  }
}
