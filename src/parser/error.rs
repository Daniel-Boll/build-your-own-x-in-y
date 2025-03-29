use miette::{Diagnostic, SourceSpan};
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
#[error("SQL Parsing Error: {message}")]
pub struct SqlError {
  pub message: String,
  #[source_code]
  pub input: String,
  #[label("Here")]
  pub span: SourceSpan,
}
