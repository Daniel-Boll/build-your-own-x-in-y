#![feature(str_as_str)]

use anyhow::bail;
use codecrafters_sqlite::{SQLite, parser::select};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn setup_tracing() {
  tracing_subscriber::registry()
    .with(
      tracing_subscriber::fmt::layer().with_line_number(true), // .with_thread_ids(true),
    )
    .with(EnvFilter::from_default_env())
    .init();
}

fn main() -> anyhow::Result<()> {
  setup_tracing();

  let args = std::env::args().collect::<Vec<_>>();
  match args.len() {
    0 | 1 => anyhow::bail!("Missing <database path> and <command>"),
    2 => anyhow::bail!("Missing <command>"),
    _ => {}
  }

  handle_command(&args[2], &args)?;

  Ok(())
}

fn handle_command(command: &str, args: &[String]) -> anyhow::Result<()> {
  let mut sqlite = SQLite::open(&args[1])?;

  match command {
    ".tables" => sqlite.list_tables(),
    ".dbinfo" => sqlite.print_db_info(),
    sql => handle_sql(&mut sqlite, sql),
  }
}

fn handle_sql(sqlite: &mut SQLite, command: &str) -> anyhow::Result<()> {
  let stmt = select::parse(command).map_err(|e| anyhow::anyhow!("Invalid SQL: {}", e))?;

  if stmt.where_clause.is_some() {
    bail!("WHERE clause not supported yet");
  }

  sqlite.select_columns(&stmt)
}
