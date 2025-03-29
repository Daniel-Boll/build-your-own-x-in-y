use std::collections::HashMap;

use miette::{Result, SourceSpan};
use nom::{
  IResult, Parser,
  branch::alt,
  bytes::complete::{tag, tag_no_case, take_while1},
  character::complete::multispace0,
  combinator::{map, opt},
  multi::separated_list1,
  sequence::{delimited, preceded},
};

use super::error::SqlError;

#[derive(Debug, PartialEq)]
pub struct SchemaStatement {
  pub table_name: String,
  pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDef {
  pub name: String,
  pub is_rowid: bool,
}

fn ws(input: &str) -> IResult<&str, &str> {
  multispace0(input)
}

fn identifier(input: &str) -> IResult<&str, String> {
  // Handle both quoted and unquoted identifiers
  let quoted = delimited(tag("\""), take_while1(|c: char| c != '"'), tag("\""));
  let unquoted = take_while1(|c: char| c.is_alphanumeric() || c == '_');
  map(alt((quoted, unquoted)), String::from).parse(input)
}

fn column_def(input: &str) -> IResult<&str, ColumnDef> {
  let (input, name) = preceded(ws, identifier).parse(input)?;
  // Capture everything until the next comma or closing parenthesis as type and constraints
  let (input, type_and_constraints) =
    preceded(ws, take_while1(|c: char| c != ',' && c != ')')).parse(input)?;
  let upper_constraints = type_and_constraints.to_uppercase();
  let is_rowid = upper_constraints.contains("INTEGER") && upper_constraints.contains("PRIMARY");
  Ok((input, ColumnDef { name, is_rowid }))
}

fn column_list(input: &str) -> IResult<&str, Vec<ColumnDef>> {
  delimited(
    preceded(ws, tag("(")),
    separated_list1(preceded(ws, tag(",")), column_def),
    preceded(ws, tag(")")),
  )
  .parse(input)
}

fn create_table(input: &str) -> IResult<&str, SchemaStatement> {
  map(
    (
      tag_no_case("CREATE"),
      ws,
      tag_no_case("TABLE"),
      ws,
      opt((
        tag_no_case("IF"),
        ws,
        tag_no_case("NOT"),
        ws,
        tag_no_case("EXISTS"),
        ws,
      )),
      identifier,
      ws,
      column_list,
    ),
    |(_, _, _, _, _, table_name, _, columns)| SchemaStatement {
      table_name,
      columns,
    },
  )
  .parse(input)
}

pub fn parse(sql: &str) -> Result<SchemaStatement> {
  match create_table(sql) {
    Ok((remaining, result)) => {
      if !remaining.trim().is_empty() {
        let offset = sql.len() - remaining.len();
        Err(miette::Report::new(SqlError {
          message: format!("Unparsed input remaining: '{}'", remaining),
          input: sql.to_string(),
          span: SourceSpan::new(offset.into(), remaining.len()),
        }))
      } else {
        Ok(result)
      }
    }
    Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
      let offset = sql.len() - e.input.len();
      Err(miette::Report::new(SqlError {
        message: "Invalid SQL syntax".to_string(),
        input: sql.to_string(),
        span: SourceSpan::new(offset.into(), 1),
      }))
    }
    Err(nom::Err::Incomplete(_)) => Err(miette::miette!("Incomplete input")),
  }
}

impl SchemaStatement {
  pub fn to_column_map(&self) -> (HashMap<String, usize>, Option<String>) {
    let mut column_map = HashMap::new();
    let mut rowid_alias = None;
    let mut payload_idx = 0;

    for column in &self.columns {
      if column.is_rowid {
        rowid_alias = Some(column.name.clone());
      } else {
        column_map.insert(column.name.clone(), payload_idx);
        payload_idx += 1;
      }
    }

    (column_map, rowid_alias)
  }
}
