use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::dataframe::{DataFrame as DFDataFrame, DataFrameWriteOptions};
use datafusion::logical_expr::{JoinType, Partitioning as DFPartitioning, SortExpr as DFSortExpr};
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, Expr, ParquetReadOptions, col};
use datafusion::scalar::ScalarValue;
use napi::bindgen_prelude::Uint8Array;
use napi::Status;
use napi_derive::napi;

use crate::sql::to_napi_err;
use crate::sql::FieldInfo;
use crate::sql::QueryResult;
use crate::sql::SessionContext;
use crate::sql::invalid_arg;

/// A lazily-evaluated DataFusion DataFrame.
///
/// Opaque to JS (only `inner` is stored); interact with it through the methods.
#[napi]
pub struct DataFrame {
  inner: DFDataFrame,
}

/// One sort key: a SQL expression plus direction/null-handling.
#[napi(object)]
pub struct SortExpr {
  /// SQL expression to sort by, e.g. `"a"` or `"b * c"`.
  pub expr: String,
  /// Ascending when `true` (default `true`).
  pub asc: Option<bool>,
  /// `true` puts NULLs first (default `false`).
  pub nulls_first: Option<bool>,
}

/// Partitioning scheme for `repartition`.
#[napi(object)]
pub struct Partitioning {
  /// `"round_robin"` or `"hash"`.
  pub kind: String,
  /// Number of partitions.
  pub num: i64,
  /// Column names, required when `kind == "hash"`.
  pub columns: Option<Vec<String>>,
}

impl DataFrame {
  /// Parse a SQL expression string against this DataFrame's schema.
  fn parse_expr(&self, sql: &str) -> napi::Result<Expr> {
    self.inner.parse_sql_expr(sql).map_err(to_napi_err)
  }

  /// Parse a list of `SortExpr` inputs into DataFusion `SortExpr`s.
  ///
  /// `nullsFirst` follows Spark: NULLs sort first ascending, last descending,
  /// so it defaults to the sort direction unless it is given explicitly.
  fn to_df_sort_exprs(&self, sorts: Vec<SortExpr>) -> napi::Result<Vec<DFSortExpr>> {
    sorts
      .into_iter()
      .map(|s| {
        let e = self.parse_expr(&s.expr)?;
        let (asc, nulls_first) = sort_order(s.asc, s.nulls_first);
        Ok(e.sort(asc, nulls_first))
      })
      .collect()
  }
}

/// Resolve sort direction and NULL placement: NULLs first ascending, last
/// descending, matching Spark.
fn sort_order(asc: Option<bool>, nulls_first: Option<bool>) -> (bool, bool) {
  let asc = asc.unwrap_or(true);
  (asc, nulls_first.unwrap_or(asc))
}

/// Validate and convert `limit` arguments, rejecting negatives instead of
/// letting `as usize` wrap them into huge values.
fn limit_bounds(skip: i64, fetch: Option<i64>) -> napi::Result<(usize, Option<usize>)> {
  if skip < 0 {
    return Err(invalid_arg("skip must not be negative"));
  }
  if fetch.is_some_and(|value| value < 0) {
    return Err(invalid_arg("fetch must not be negative"));
  }
  Ok((skip as usize, fetch.map(|value| value as usize)))
}

/// Wrap a DataFusion DataFrame into our napi DataFrame.
fn wrap(df: DFDataFrame) -> DataFrame {
  DataFrame { inner: df }
}

/// Map a join-type string to DataFusion's `JoinType`.
fn to_join_type(s: &str) -> napi::Result<JoinType> {
  Ok(match s.to_ascii_uppercase().as_str() {
    "INNER" => JoinType::Inner,
    "LEFT" => JoinType::Left,
    "RIGHT" => JoinType::Right,
    "FULL" => JoinType::Full,
    "LEFTANTI" | "LEFT_ANTI" => JoinType::LeftAnti,
    "RIGHTANTI" | "RIGHT_ANTI" => JoinType::RightAnti,
    "LEFTSEMI" | "LEFT_SEMI" => JoinType::LeftSemi,
    "RIGHTSEMI" | "RIGHT_SEMI" => JoinType::RightSemi,
    other => {
      return Err(napi::Error::new(
        Status::InvalidArg,
        format!("unknown join type: {other}"),
      ))
    }
  })
}

/// Map our `Partitioning` object to DataFusion's `Partitioning`.
fn to_df_partitioning(p: Partitioning) -> napi::Result<DFPartitioning> {
  match p.kind.to_ascii_uppercase().as_str() {
    "ROUND_ROBIN" | "ROUNDROBIN" => Ok(DFPartitioning::RoundRobinBatch(p.num as usize)),
    "HASH" => {
      let cols = p.columns.ok_or_else(|| {
        napi::Error::new(Status::InvalidArg, "hash partitioning requires 'columns'")
      })?;
      let exprs = cols.into_iter().map(col).collect();
      Ok(DFPartitioning::Hash(exprs, p.num as usize))
    }
    other => Err(napi::Error::new(
      Status::InvalidArg,
      format!("unknown partitioning kind: {other}"),
    )),
  }
}

/// Best-effort inference of a `ScalarValue` from a string (used by `fill_null`
/// /`fill_nan`). Tries int64, then float64, then bool, falling back to Utf8.
///
/// Prefer passing `dataType` when the column type is known: this guesses, and a
/// guess that does not match the column type fails at execution time.
fn infer_scalar(s: &str) -> ScalarValue {
  if let Ok(i) = s.parse::<i64>() {
    ScalarValue::Int64(Some(i))
  } else if let Ok(f) = s.parse::<f64>() {
    ScalarValue::Float64(Some(f))
  } else if s == "true" {
    ScalarValue::Boolean(Some(true))
  } else if s == "false" {
    ScalarValue::Boolean(Some(false))
  } else {
    ScalarValue::Utf8(Some(s.to_string()))
  }
}

/// Resolve the fill value, casting it to `data_type` when one is given so that
/// it matches the target column: `fillNull("0", ["amount"], "Float64")`.
fn resolve_scalar(value: &str, data_type: Option<&str>) -> napi::Result<ScalarValue> {
  let Some(data_type) = data_type else {
    return Ok(infer_scalar(value));
  };
  let parsed_type: datafusion::arrow::datatypes::DataType = data_type
    .parse()
    .map_err(|e| invalid_arg(format!("cannot parse data_type {data_type}: {e}")))?;
  match parsed_type {
    datafusion::arrow::datatypes::DataType::Int8 => Ok(ScalarValue::Int8(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Int16 => Ok(ScalarValue::Int16(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Int32 => Ok(ScalarValue::Int32(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Int64 => Ok(ScalarValue::Int64(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Float32 => Ok(ScalarValue::Float32(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Float64 => Ok(ScalarValue::Float64(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Boolean => Ok(ScalarValue::Boolean(value.parse().ok())),
    datafusion::arrow::datatypes::DataType::Date32 => Ok(ScalarValue::Date32(value.parse().ok())),
    _ => Ok(ScalarValue::Utf8(Some(value.to_string()))),
  }
}

#[napi]
impl DataFrame {
  // ---- Projection / selection -------------------------------------------

  /// Project arbitrary SQL expressions, e.g. `["a", "b * c"]`.
  #[napi]
  pub fn select(&self, exprs: Vec<String>) -> napi::Result<DataFrame> {
    let parsed: Vec<Expr> = exprs
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    self.inner.clone().select(parsed).map(wrap).map_err(to_napi_err)
  }

  /// Keep only the named columns.
  #[napi]
  pub fn select_columns(&self, columns: Vec<String>) -> napi::Result<DataFrame> {
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    self
      .inner
      .clone()
      .select_columns(&refs)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Project a list of SQL expression strings.
  #[napi]
  pub fn select_exprs(&self, exprs: Vec<String>) -> napi::Result<DataFrame> {
    let refs: Vec<&str> = exprs.iter().map(String::as_str).collect();
    self
      .inner
      .clone()
      .select_exprs(&refs)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Add or replace a column. `expr` is a SQL expression string.
  #[napi]
  pub fn with_column(&self, name: String, expr: String) -> napi::Result<DataFrame> {
    let e = self.parse_expr(&expr)?;
    self
      .inner
      .clone()
      .with_column(&name, e)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Rename a column (no-op if the old name doesn't exist).
  #[napi]
  pub fn with_column_renamed(
    &self,
    old_name: String,
    new_name: String,
  ) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .with_column_renamed(old_name.as_str(), &new_name)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Drop the named columns.
  #[napi]
  pub fn drop_columns(&self, columns: Vec<String>) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .drop_columns(&columns)
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Filtering / aggregation / window ---------------------------------

  /// Keep rows where the SQL predicate evaluates to true.
  #[napi]
  pub fn filter(&self, predicate: String) -> napi::Result<DataFrame> {
    let e = self.parse_expr(&predicate)?;
    self.inner.clone().filter(e).map(wrap).map_err(to_napi_err)
  }

  /// Group/aggregate. `group` and `aggr` are SQL expression strings, e.g.
  /// `aggregate(["a"], ["min(b)"])`.
  #[napi]
  pub fn aggregate(
    &self,
    group: Vec<String>,
    aggr: Vec<String>,
  ) -> napi::Result<DataFrame> {
    let g: Vec<Expr> = group
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    let a: Vec<Expr> = aggr
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    self.inner.clone().aggregate(g, a).map(wrap).map_err(to_napi_err)
  }

  /// Add window-function expressions.
  #[napi]
  pub fn window(&self, window_exprs: Vec<String>) -> napi::Result<DataFrame> {
    let w: Vec<Expr> = window_exprs
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    self.inner.clone().window(w).map(wrap).map_err(to_napi_err)
  }

  /// Sort by the given keys (see [`SortExpr`]).
  #[napi]
  pub fn sort(&self, sorts: Vec<SortExpr>) -> napi::Result<DataFrame> {
    let exprs = self.to_df_sort_exprs(sorts)?;
    self.inner.clone().sort(exprs).map(wrap).map_err(to_napi_err)
  }

  /// Sort using default direction for each expression.
  #[napi]
  pub fn sort_by(&self, exprs: Vec<String>) -> napi::Result<DataFrame> {
    let parsed: Vec<Expr> = exprs
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    self
      .inner
      .clone()
      .sort_by(parsed)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Limit output: skip `skip` rows, then return at most `fetch` (or all).
  ///
  /// Both arguments must be non-negative.
  #[napi]
  pub fn limit(&self, skip: i64, fetch: Option<i64>) -> napi::Result<DataFrame> {
    let (skip, fetch) = limit_bounds(skip, fetch)?;
    self
      .inner
      .clone()
      .limit(skip, fetch)
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Distinct ----------------------------------------------------------

  /// Remove all duplicated rows.
  #[napi]
  pub fn distinct(&self) -> napi::Result<DataFrame> {
    self.inner.clone().distinct().map(wrap).map_err(to_napi_err)
  }

  /// `DISTINCT ON`: `on` are the de-duplication keys, `select` the output
  /// expressions, `sort` an optional ordering within each group.
  #[napi]
  pub fn distinct_on(
    &self,
    on: Vec<String>,
    select: Vec<String>,
    sort: Option<Vec<SortExpr>>,
  ) -> napi::Result<DataFrame> {
    let on_e: Vec<Expr> = on
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    let sel_e: Vec<Expr> = select
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    let sort_e = match sort {
      Some(s) => Some(self.to_df_sort_exprs(s)?),
      None => None,
    };
    self
      .inner
      .clone()
      .distinct_on(on_e, sel_e, sort_e)
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Set operations ----------------------------------------------------

  #[napi]
  pub fn union(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .union(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn union_distinct(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .union_distinct(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn union_by_name(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .union_by_name(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn union_by_name_distinct(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .union_by_name_distinct(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn intersect(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .intersect(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn intersect_distinct(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .intersect_distinct(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn except(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .except(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  #[napi]
  pub fn except_distinct(&self, right: &DataFrame) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .except_distinct(right.inner.clone())
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Joins -------------------------------------------------------------

  /// Equijoin on `left_cols`/`right_cols` plus an optional SQL filter.
  ///
  /// The filter is resolved against the *joined* schema, so it may reference
  /// columns from both sides. That costs one extra planning pass when a filter
  /// is given.
  #[napi]
  pub fn join(
    &self,
    right: &DataFrame,
    join_type: String,
    left_cols: Vec<String>,
    right_cols: Vec<String>,
    filter: Option<String>,
  ) -> napi::Result<DataFrame> {
    let jt = to_join_type(&join_type)?;
    let lc: Vec<&str> = left_cols.iter().map(String::as_str).collect();
    let rc: Vec<&str> = right_cols.iter().map(String::as_str).collect();
    let f = match filter {
      Some(sql) => {
        let probe = self
          .inner
          .clone()
          .join(right.inner.clone(), jt, &lc, &rc, None)
          .map_err(to_napi_err)?;
        Some(probe.parse_sql_expr(&sql).map_err(to_napi_err)?)
      }
      None => None,
    };
    self
      .inner
      .clone()
      .join(right.inner.clone(), jt, &lc, &rc, f)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Join on explicit SQL predicate expressions.
  #[napi]
  pub fn join_on(
    &self,
    right: &DataFrame,
    join_type: String,
    on_exprs: Vec<String>,
  ) -> napi::Result<DataFrame> {
    let jt = to_join_type(&join_type)?;
    let on: Vec<Expr> = on_exprs
      .iter()
      .map(|e| self.parse_expr(e))
      .collect::<napi::Result<_>>()?;
    self
      .inner
      .clone()
      .join_on(right.inner.clone(), jt, on)
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Repartition / unnest / fill --------------------------------------

  /// Repartition the DataFrame (see [`Partitioning`]).
  #[napi]
  pub fn repartition(&self, partitioning: Partitioning) -> napi::Result<DataFrame> {
    let p = to_df_partitioning(partitioning)?;
    self
      .inner
      .clone()
      .repartition(p)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Expand the named list/struct columns into rows and new columns.
  #[napi]
  pub fn unnest_columns(&self, columns: Vec<String>) -> napi::Result<DataFrame> {
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    self
      .inner
      .clone()
      .unnest_columns(&refs)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Fill NULLs in the given columns (or all when empty) with `value`.
  ///
  /// `value` is parsed as int64/float64/bool/utf8 (see [`infer_scalar`]), or
  /// cast to `dataType` when that is given.
  #[napi]
  pub fn fill_null(
    &self,
    value: String,
    columns: Vec<String>,
    data_type: Option<String>,
  ) -> napi::Result<DataFrame> {
    let sv = resolve_scalar(&value, data_type.as_deref())?;
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    self
      .inner
      .clone()
      .fill_null(&sv, &refs)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Fill NaN values in floating-point columns (see [`infer_scalar`]).
  #[napi]
  pub fn fill_nan(
    &self,
    value: String,
    columns: Vec<String>,
    data_type: Option<String>,
  ) -> napi::Result<DataFrame> {
    let sv = resolve_scalar(&value, data_type.as_deref())?;
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    self
      .inner
      .clone()
      .fill_nan(&sv, &refs)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Apply an alias, replacing column qualifiers.
  #[napi]
  pub fn alias(&self, alias: String) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .alias(&alias)
      .map(wrap)
      .map_err(to_napi_err)
  }

  // ---- Explain / introspection ------------------------------------------

  /// Return a DataFrame of the plan explanation (`EXPLAIN`).
  #[napi]
  pub fn explain(&self, verbose: bool, analyze: bool) -> napi::Result<DataFrame> {
    self
      .inner
      .clone()
      .explain(verbose, analyze)
      .map(wrap)
      .map_err(to_napi_err)
  }

  /// Human-readable schema string (DataFusion `DFSchema`).
  #[napi]
  pub fn schema(&self) -> String {
    format!("{}", self.inner.schema())
  }

  /// Column names, in order. Same as `schemaFields[].name`.
  #[napi]
  pub fn columns(&self) -> Vec<String> {
    self
      .inner
      .schema()
      .fields()
      .iter()
      .map(|field| field.name().clone())
      .collect()
  }

  /// Name and Arrow type of every column, in order.
  ///
  /// This is the typed view of the schema: `collect()` returns cells as
  /// strings, and this is what tells you what they actually are.
  #[napi]
  pub fn schema_fields(&self) -> Vec<FieldInfo> {
    FieldInfo::from_fields(self.inner.schema().fields())
  }

  /// Human-readable unoptimized logical plan.
  #[napi]
  pub fn logical_plan(&self) -> String {
    format!("{}", self.inner.logical_plan())
  }

  // ---- Execution ---------------------------------------------------------

  /// Cache as an in-memory table, returning a new DataFrame.
  #[napi]
  pub async fn cache(&self) -> napi::Result<DataFrame> {
    let df = self.inner.clone().cache().await.map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Summary statistics (`DESCRIBE`): count, null_count, mean, std, min, max,
  /// median per column. Returns a new DataFrame (call `collect` to read it).
  #[napi]
  pub async fn describe(&self) -> napi::Result<DataFrame> {
    let df = self.inner.clone().describe().await.map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Total number of rows.
  #[napi]
  pub async fn count(&self) -> napi::Result<i64> {
    let c = self.inner.clone().count().await.map_err(to_napi_err)?;
    Ok(c as i64)
  }

  /// Execute and buffer all `RecordBatch`es into a [`QueryResult`].
  ///
  /// Cells are stringified. For anything large, typed, or binary, use
  /// [`DataFrame::to_arrow_ipc`] instead: it keeps the Arrow types and does not
  /// build a JS string per cell.
  #[napi]
  pub async fn collect(&self) -> napi::Result<QueryResult> {
    let batches = self.inner.clone().collect().await.map_err(to_napi_err)?;
    QueryResult::from_batches(batches)
  }

  /// Execute and return the result as an Arrow IPC stream.
  ///
  /// This is the lossless path: types, nested values and binary data survive,
  /// and nothing is allocated per cell on the JavaScript side. Feed the bytes to
  /// `apache-arrow` in Node. Statements that produce no rows (DDL) yield an
  /// empty buffer, since there is no schema to write.
  #[napi]
  pub async fn to_arrow_ipc(&self) -> napi::Result<Uint8Array> {
    let batches = self.inner.clone().collect().await.map_err(to_napi_err)?;
    let Some(first) = batches.first() else {
      return Ok(Uint8Array::new(Vec::new()));
    };

    let mut out = Vec::new();
    {
      let mut writer =
        StreamWriter::try_new(&mut out, &first.schema()).map_err(|e| napi::Error::new(Status::GenericFailure, e.to_string()))?;
      for batch in &batches {
        // `write` checks the schema itself; this message is just clearer.
        if batch.schema() != first.schema() {
          return Err(napi::Error::new(
            Status::GenericFailure,
            "cannot write record batches with different schemas to one IPC stream",
          ));
        }
        writer.write(batch).map_err(|e| napi::Error::new(Status::GenericFailure, e.to_string()))?;
      }
      writer.finish().map_err(|e| napi::Error::new(Status::GenericFailure, e.to_string()))?;
    }
    Ok(Uint8Array::new(out))
  }

  /// Execute, preserving input partitioning; returns one [`QueryResult`] per
  /// partition.
  #[napi]
  pub async fn collect_partitioned(&self) -> napi::Result<Vec<QueryResult>> {
    let parts = self
      .inner
      .clone()
      .collect_partitioned()
      .await
      .map_err(to_napi_err)?;
    parts.into_iter().map(QueryResult::from_batches).collect()
  }

  /// Execute and print the result to stdout.
  #[napi]
  pub async fn show(&self) -> napi::Result<()> {
    self.inner.clone().show().await.map_err(to_napi_err)
  }

  /// Execute and print only the first `num` rows.
  #[napi]
  pub async fn show_limit(&self, num: i64) -> napi::Result<()> {
    self
      .inner
      .clone()
      .show_limit(num as usize)
      .await
      .map_err(to_napi_err)
  }

  /// Execute and return a string rendering of the result.
  #[napi]
  pub async fn to_string(&self) -> napi::Result<String> {
    self.inner.clone().to_string().await.map_err(to_napi_err)
  }

  /// Execute and write results to a CSV file. Returns a [`QueryResult`] with
  /// the number of rows written.
  #[napi]
  pub async fn write_csv(&self, path: String) -> napi::Result<QueryResult> {
    let batches = self
      .inner
      .clone()
      .write_csv(&path, DataFrameWriteOptions::new(), None)
      .await
      .map_err(to_napi_err)?;
    QueryResult::from_batches(batches)
  }

  /// Execute and write results to a Parquet file.
  #[napi]
  pub async fn write_parquet(&self, path: String) -> napi::Result<QueryResult> {
    let batches = self
      .inner
      .clone()
      .write_parquet(&path, DataFrameWriteOptions::new(), None)
      .await
      .map_err(to_napi_err)?;
    QueryResult::from_batches(batches)
  }

  /// Execute and insert results into an existing table.
  #[napi]
  pub async fn write_table(&self, table_name: String) -> napi::Result<QueryResult> {
    let batches = self
      .inner
      .clone()
      .write_table(&table_name, DataFrameWriteOptions::new())
      .await
      .map_err(to_napi_err)?;
    QueryResult::from_batches(batches)
  }
}

// ===========================================================================
// DataFrame-producing factory methods on SessionContext.
//
// Placed here (same crate as `sql::SessionContext`) so a `DataFrame` can be
// obtained from JS without duplicating the SessionContext definition.
// ===========================================================================

#[napi]
impl SessionContext {
  /// Read a CSV file into a DataFrame (`ctx.read_csv(path)`).
  #[napi]
  pub async fn read_csv(&self, path: String) -> napi::Result<DataFrame> {
    let df = self
      .inner
      .read_csv(&path, CsvReadOptions::new())
      .await
      .map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Read a Parquet file into a DataFrame.
  #[napi]
  pub async fn read_parquet(&self, path: String) -> napi::Result<DataFrame> {
    let df = self
      .inner
      .read_parquet(&path, ParquetReadOptions::default())
      .await
      .map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Read an Avro file into a DataFrame.
  #[napi]
  pub async fn read_avro(&self, path: String) -> napi::Result<DataFrame> {
    let df = self
      .inner
      .read_avro(&path, AvroReadOptions::default())
      .await
      .map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Read a previously-registered table into a DataFrame.
  #[napi]
  pub async fn table(&self, name: String) -> napi::Result<DataFrame> {
    let df = self.inner.table(&name).await.map_err(to_napi_err)?;
    Ok(wrap(df))
  }

  /// Run a SQL statement and return the resulting DataFrame (does not collect).
  #[napi]
  pub async fn sql_df(&self, sql: String) -> napi::Result<DataFrame> {
    let df = self.inner.sql(&sql).await.map_err(to_napi_err)?;
    Ok(wrap(df))
  }
}

// Methods intentionally omitted from this draft (see module docs / DataFusion
// source for details):
//   - `new`, `from_columns`                  (require constructing Arrow arrays in JS)
//   - `parse_sql_expr`                        (returns an `Expr`, not representable in JS)
//   - `create_physical_plan`, `task_ctx`,
//     `registry`, `into_parts`,
//     `into_unoptimized_plan`, `into_optimized_plan`,
//     `into_view`, `into_temporary_view`      (return non-JS Rust objects)
//   - `with_param_values`                     (parameter binding needs typed `ScalarValue`s)
//   - `find_qualified_columns`                (returns qualified column refs)
//   - `execute_stream`, `execute_stream_partitioned`
//     (async streaming is not modeled as a single JS value here)
//   - `explain_with_options`                  (`ExplainOption` struct; `explain` covers common cases)
//   - `unnest_columns_with_options`           (`UnnestOptions` struct; `unnest_columns` covers defaults)

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn sort_order_defaults_to_spark_null_placement() {
    // Unset means ascending, NULLs first (also DataFusion's own default).
    assert_eq!(sort_order(None, None), (true, true));
    assert_eq!(sort_order(Some(false), None), (false, false));
    // An explicit value always wins.
    assert_eq!(sort_order(Some(false), Some(true)), (false, true));
    assert_eq!(sort_order(Some(true), Some(false)), (true, false));
  }

  #[test]
  fn limit_bounds_rejects_negative_arguments() {
    assert!(limit_bounds(-1, None).is_err());
    assert!(limit_bounds(0, Some(-1)).is_err());
    assert_eq!(limit_bounds(0, None).unwrap(), (0, None));
    assert_eq!(limit_bounds(2, Some(10)).unwrap(), (2, Some(10)));
  }

  #[test]
  fn infer_scalar_picks_a_plausible_type() {
    assert!(matches!(infer_scalar("42"), ScalarValue::Int64(Some(42))));
    assert!(matches!(infer_scalar("4.5"), ScalarValue::Float64(Some(_))));
    assert!(matches!(infer_scalar("true"), ScalarValue::Boolean(Some(true))));
    assert!(matches!(infer_scalar("hi"), ScalarValue::Utf8(Some(_))));
  }

  #[test]
  fn resolve_scalar_casts_to_the_requested_type() {
    // The guessed type would be Int64, which does not fit a float column.
    let scalar = resolve_scalar("0", Some("Float64")).unwrap();
    assert!(matches!(scalar, ScalarValue::Float64(Some(_))));
    let literal = resolve_scalar("2024-01-02", Some("Date32")).unwrap();
    assert!(matches!(literal, ScalarValue::Date32(_)), "{literal:?}");
    // Quotes in the value are escaped rather than terminating the literal.
    assert!(resolve_scalar("it's", Some("Utf8")).is_ok());
    // An unknown type is a bad argument, not a panic.
    assert!(resolve_scalar("1", Some("NotAType")).is_err());
  }

  #[test]
  fn join_types_are_case_insensitive() {
    assert!(matches!(to_join_type("inner"), Ok(JoinType::Inner)));
    assert!(matches!(to_join_type("LEFT_SEMI"), Ok(JoinType::LeftSemi)));
    assert!(to_join_type("sideways").is_err());
  }

  #[test]
  fn partitioning_requires_columns_for_hash() {
    let round_robin = Partitioning {
      kind: "round_robin".to_string(),
      num: 4,
      columns: None,
    };
    assert!(to_df_partitioning(round_robin).is_ok());

    let hash_without_columns = Partitioning {
      kind: "hash".to_string(),
      num: 4,
      columns: None,
    };
    assert!(to_df_partitioning(hash_without_columns).is_err());
  }
}
