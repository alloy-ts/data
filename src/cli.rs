use napi::{Error, Result, Status};
use napi_derive::napi;
use pyo3::Python;

/// Program name reported in clap's help and usage output.
const PROGRAM_NAME: &str = "sail";

/// Options for the `sail spark server` command.
#[napi(object)]
pub struct SailSparkServerOptions {
  /// IP address to bind. Defaults to `127.0.0.1`, as in the CLI.
  pub ip: Option<String>,
  /// Port to bind. Defaults to `50051`, as in the CLI. Pass `0` for an
  /// ephemeral port.
  pub port: Option<u16>,
  /// Working directory for the server (`--directory`).
  pub directory: Option<String>,
}

/// Options for the `sail spark run` command.
#[napi(object)]
pub struct SailSparkRunOptions {
  /// PySpark file to run. Defaults to `-`, which reads stdin.
  pub file: Option<String>,
  /// Working directory for the job (`--directory`).
  pub directory: Option<String>,
}
#[napi]
pub fn sail(args: Vec<String>) -> Result<i32> {
  // clap reads argv[0] as the program name.
  let mut argv = Vec::with_capacity(args.len() + 1);
  argv.push(PROGRAM_NAME.to_string());
  argv.extend(args);

  // Upstream's binary initializes CPython before dispatching, which `spark
  // shell` and `spark run` need. Doing it here keeps the interpreter on the
  // calling thread: CPython installs its signal handlers on the thread that
  // initializes it, which is what makes Ctrl-C work in the shell. A NAPI call
  // made from Node's main thread runs there.
  Python::initialize();

  match sail_cli::runner::main(argv) {
    Ok(()) => Ok(0),
    Err(error) => Err(Error::new(Status::GenericFailure, error.to_string())),
  }
}
#[napi]
pub fn sail_spark_server(options: Option<SailSparkServerOptions>) -> Result<i32> {
  let options = options.unwrap_or_default();
  let mut args = vec!["spark".to_string(), "server".to_string()];
  if let Some(ip) = options.ip {
    args.push("--ip".to_string());
    args.push(ip);
  }
  if let Some(port) = options.port {
    args.push("--port".to_string());
    args.push(port.to_string());
  }
  if let Some(directory) = options.directory {
    args.push("--directory".to_string());
    args.push(directory);
  }
  sail(args)
}

#[napi]
pub fn sail_spark_shell() -> Result<i32> {
  let args = vec!["spark".to_string(), "shell".to_string()];
  sail(args)
}
#[napi]
pub fn sail_spark_run(options: Option<SailSparkRunOptions>) -> Result<i32> {
  let options = options.unwrap_or_default();
  let mut args = vec!["spark".to_string(), "run".to_string()];
  if let Some(file) = options.file {
    args.push("--file".to_string());
    args.push(file);
  }
  if let Some(directory) = options.directory {
    args.push("--directory".to_string());
    args.push(directory);
  }
  sail(args)
}
