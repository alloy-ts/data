use napi_derive::napi;

/// Options for configuring the Spark Connect server.
#[napi(object)]
pub struct SparkConnectOptions {
  /// IP address to bind (e.g. "127.0.0.1" or "0.0.0.0").
  pub ip: Option<String>,
  /// Port to bind (defaults to 50051 or 15002 as configured).
  pub port: Option<u16>,
  /// Working directory for the server session.
  pub directory: Option<String>,
}

#[napi]
pub fn spark_connect_version() -> String {
  env!("CARGO_PKG_VERSION").to_string()
}
