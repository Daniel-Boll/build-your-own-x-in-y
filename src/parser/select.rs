use miette::{Result, SourceSpan};
use nom::{
  IResult, Parser,
  branch::alt,
  bytes::complete::{tag_no_case, take_while1},
  character::complete::multispace0,
  combinator::{map, opt},
  multi::separated_list1,
  sequence::{delimited, preceded},
};

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
  pub columns: Vec<Column>,
  pub from: String,
  pub where_clause: Option<Condition>,
}

#[derive(Debug, PartialEq)]
pub enum Column {
  All,
  Count,
  Named(String),
}

#[derive(Debug, PartialEq)]
pub struct Condition {
  left: String,
  operator: String,
  right: String,
}

use super::error::SqlError;

fn ws<I, O, E, P>(tag: P) -> impl Parser<I, Output = O, Error = E>
where
  P: Parser<I, Output = O, Error = E>,
  I: nom::Input,
  E: nom::error::ParseError<I>,
  <I as nom::Input>::Item: nom::AsChar,
{
  delimited(multispace0, tag, multispace0)
}

fn identifier(input: &str) -> IResult<&str, String> {
  let (rest, ident) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
  let ident_upper = ident.to_uppercase();
  // Prevent reserved keywords from being used as identifiers
  if matches!(ident_upper.as_str(), "SELECT" | "FROM" | "WHERE" | "COUNT") {
    return Err(nom::Err::Error(nom::error::Error::new(
      input,
      nom::error::ErrorKind::Verify,
    )));
  }
  // Return the original case for display purposes, but we'll compare case-insensitively elsewhere
  Ok((rest, ident.to_string()))
}

fn column(input: &str) -> IResult<&str, Column> {
  alt((
    map(tag_no_case("*"), |_| Column::All),
    map(
      preceded(ws(tag_no_case("COUNT")), ws(tag_no_case("(*)"))),
      |_| Column::Count,
    ),
    map(identifier, Column::Named),
  ))
  .parse(input)
}

fn column_list(input: &str) -> IResult<&str, Vec<Column>> {
  separated_list1(ws(tag_no_case(",")), column).parse(input)
}

fn condition(input: &str) -> IResult<&str, Condition> {
  (
    identifier,
    ws(alt((
      tag_no_case("="),
      tag_no_case("<"),
      tag_no_case(">"),
      tag_no_case("<="),
      tag_no_case(">="),
    ))),
    identifier,
  )
    .parse(input)
    .map(|(rest, (left, operator, right))| {
      (
        rest,
        Condition {
          left,
          operator: operator.to_string(),
          right,
        },
      )
    })
}

fn select_statement(input: &str) -> IResult<&str, SelectStatement> {
  map(
    (
      ws(tag_no_case("SELECT")),
      column_list,
      ws(tag_no_case("FROM")),
      identifier,
      opt(preceded(ws(tag_no_case("WHERE")), condition)),
    ),
    |(_, columns, _, from, where_clause)| SelectStatement {
      columns,
      from,
      where_clause,
    },
  )
  .parse(input)
}

pub fn parse(input: &str) -> Result<SelectStatement> {
  match select_statement(input) {
    Ok((remaining, result)) => {
      if !remaining.trim().is_empty() {
        let offset = input.len() - remaining.len();
        Err(miette::Report::new(SqlError {
          message: format!("Unparsed input remaining: '{}'", remaining),
          input: input.to_string(),
          span: SourceSpan::new(offset.into(), remaining.len()),
        }))
      } else {
        Ok(result)
      }
    }
    Err(nom::Err::Error(e) | nom::Err::Failure(e)) => {
      let offset = input.len() - e.input.len();
      Err(miette::Report::new(SqlError {
        message: "(select) Invalid SQL syntax".to_string(),
        input: input.to_string(),
        span: SourceSpan::new(offset.into(), 1),
      }))
    }
    Err(nom::Err::Incomplete(_)) => Err(miette::miette!("Incomplete input")),
  }
}
