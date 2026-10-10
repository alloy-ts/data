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

use std::sync::Arc;
use std::time::Duration;

use crate::error::{RpcStatus, SailError};
use crate::exec::ExecOptions;
use crate::sailbox::object::Sailbox;

/// Options for [`Sailbox::shell`].
#[derive(Debug, Clone, Default)]
pub struct ShellOptions {
    /// Login shell to run when no command is given (default: the guest's
    /// `$SHELL`, else `/bin/bash`). Ignored when a command is given.
    pub shell: Option<String>,
    /// `$TERM` for the remote pty (default: the local `$TERM`).
    pub term: Option<String>,
    /// Working directory for the session. `None` starts it in the image's
    /// working directory, or `/` when the image does not set one.
    pub cwd: Option<String>,
    /// Run the session as this Sailbox user, with Docker `USER` semantics
    /// (see [`ExecOptions::user`]). `None` runs as the image's `USER` when
    /// the image sets one, root otherwise; pass `"0:0"` to force root.
    pub user: Option<String>,
    /// Wall-clock limit for the session; `None` means no limit.
    pub timeout: Option<Duration>,
    /// Extra environment for the session, with the precedence and reserved
    /// variables of [`ExecOptions::env`].
    pub env: Vec<(String, String)>,
    /// Turn off all local forwarding for the session (on by default): the
    /// browser opens and localhost servers, plus paste, drag-and-drop, and
    /// clipboard bridging. Every byte then passes through verbatim.
    pub no_forward: bool,
}

/// True when stdin and stdout are both TTYs, required for an interactive PTY.
#[doc(hidden)]
pub fn stdio_is_tty() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn tty_required() -> SailError {
    SailError::Execution {
        code: RpcStatus::FailedPrecondition,
        detail: "shell requires an interactive terminal (stdin and stdout must be TTYs)"
            .to_string(),
    }
}

impl Sailbox {
    /// Open an interactive pty session on the Sailbox, driving the local
    /// terminal. With no `command`, runs a login shell; pass a command to run
    /// that under a pty instead (e.g. a REPL or an editor). Raw-mode
    /// keystrokes (including Ctrl-C, Ctrl-Z, and Ctrl-D) reach the remote
    /// process, its output renders locally, and terminal resizes propagate.
    /// Blocks until the remote process exits and returns its exit code.
    /// Requires an interactive local terminal (stdin and stdout TTYs).
    ///
    /// A dropped connection does not end the session. The shell prints a
    /// notice, keeps reconnecting, and redraws the screen once the Sailbox is
    /// reachable again. If the Sailbox goes to sleep, the next key pressed
    /// wakes it and resumes the same session. If it is paused, the shell says
    /// so when a key is pressed and sends the keys once the Sailbox is
    /// resumed. Pressing Ctrl-D while disconnected or paused leaves the
    /// session. The session ends if the Sailbox is terminated.
    ///
    /// While
    /// the session is open, browser opens, localhost servers, paste, and
    /// drag-and-drop are forwarded to the local machine, and Ctrl+V forwards
    /// your clipboard (a two-way clipboard on devbox images, upload-and-paste
    /// elsewhere); see [`ShellOptions::no_forward`].
    ///
    /// Runs on the local machine, which must be Unix (it needs Unix TTY and
    /// signal APIs).
    ///
    /// This is the one process-global API in the crate: for the session's
    /// duration it owns stdin/stdout, switches the terminal to raw mode, and
    /// installs a signal handler, restoring them when the session ends. The
    /// bridge runs on a blocking thread, so cancelling this future does not
    /// end the session; stop it by exiting the remote process.
    pub async fn shell(
        &self,
        command: Option<&str>,
        options: ShellOptions,
    ) -> Result<i32, SailError> {
        if !stdio_is_tty() {
            return Err(tty_required());
        }
        let command = match command {
            Some(command) => command.to_string(),
            None => login_shell_command(options.shell.as_deref()),
        };
        let (cols, rows) = terminal_size();
        // An interactive shell forwards the session's localhost servers, browser
        // opens, and clipboard/paste to the user's machine unless opted out.
        let (forward_ports, forward_browser, forward_clipboard) =
            crate::exec::forward_flags(options.no_forward);
        let proc = self
            .client()
            .exec_shell(
                self.sailbox_id(),
                &command,
                ExecOptions {
                    timeout: options.timeout,
                    pty: Some(crate::exec::PtyConfig {
                        term: options
                            .term
                            .or_else(|| std::env::var("TERM").ok())
                            .unwrap_or_default(),
                        cols,
                        rows,
                    }),
                    cwd: options.cwd,
                    user: options.user,
                    env: options.env,
                    forward_ports,
                    forward_browser,
                    forward_clipboard,
                    retry_timeout: crate::exec::RetryBudget::Forever,
                    wake_on_input: true,
                    ..Default::default()
                },
            )
            .await?;
        let proc = Arc::new(proc);
        tokio::task::spawn_blocking(move || run_interactive(proc))
            .await
            .map_err(|err| SailError::Internal {
                message: format!("shell bridge task failed: {err}"),
            })?
    }
}

/// The command for an interactive login session: `exec` the login shell so
/// `$0` and login semantics match ssh. An explicit shell is quoted so a path
/// with spaces runs as a literal program; the default stays unquoted so the
/// guest shell expands `$SHELL`.
fn login_shell_command(shell: Option<&str>) -> String {
    match shell {
        Some(shell) => format!("exec {} -l", crate::exec::sh_quote(shell)),
        None => "exec ${SHELL:-/bin/bash} -l".to_string(),
    }
}

/// The local terminal size as (cols, rows), defaulting to 80x24.
#[cfg(unix)]
#[doc(hidden)]
pub fn terminal_size() -> (u32, u32) {
    let mut size = libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &raw mut size) } == 0;
    if ok && size.ws_col > 0 && size.ws_row > 0 {
        (u32::from(size.ws_col), u32::from(size.ws_row))
    } else {
        (80, 24)
    }
}

/// The local terminal size as (cols, rows), defaulting to 80x24.
#[cfg(not(unix))]
#[doc(hidden)]
pub fn terminal_size() -> (u32, u32) {
    (80, 24)
}

/// Interactive PTY sessions need Unix TTY and signal APIs.
#[cfg(not(unix))]
#[doc(hidden)]
pub fn run_interactive(_proc: Arc<crate::exec::ExecProcess>) -> Result<i32, SailError> {
    Err(SailError::Execution {
        code: RpcStatus::Unimplemented,
        detail: "interactive PTY sessions are not supported on this platform".to_string(),
    })
}

#[cfg(unix)]
#[doc(hidden)]
pub use unix::run_interactive;

#[cfg(unix)]
#[doc(hidden)]
pub use unix::{drive_output_pump, RenderControl};

#[cfg(unix)]
mod unix {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::terminal_size;
    use crate::error::{RpcStatus, SailError};
    use crate::exec::{ExecProcess, ForwardEvent, LinkEnd, LinkState, OutputStream, ReadStep};
    use crate::shell_input::{
        dropped_local_files, find, partial_suffix_len, sanitize_drop_name, InputEvent,
        InputScanner, PASTE_END, PASTE_START,
    };

    /// Drive a future to completion from this bridge's dedicated thread. On
    /// the shared runtime's blocking pool (the [`Sailbox::shell`] path) an
    /// ambient handle exists and `Handle::block_on` is the correct, safe
    /// call; on a plain thread (the CLI's direct `run_interactive` use) fall
    /// back to the crate's shared-runtime `block_on`.
    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(future),
            Err(_) => crate::runtime::block_on(future),
        }
    }

    /// Set by the SIGWINCH handler; drained by the input loop to issue a resize.
    static RESIZE_PENDING: AtomicBool = AtomicBool::new(false);

    extern "C" fn on_sigwinch(_signum: libc::c_int) {
        RESIZE_PENDING.store(true, Ordering::Relaxed);
    }

    /// Drive the local terminal against a PTY exec until the remote process
    /// exits, returning its exit code. Raw mode and the SIGWINCH handler are
    /// always restored, even on error. When the session forwards its clipboard
    /// (`proc.forward_clipboard()`), dragged files and Ctrl+V pastes forward
    /// into the guest and in-guest copies mirror back to the local clipboard;
    /// without it every byte passes through verbatim.
    pub fn run_interactive(proc: Arc<ExecProcess>) -> Result<i32, SailError> {
        let saved = enter_raw_mode()?;
        let prev_winch = install_sigwinch();
        let prev_in_flags = set_stdin_nonblocking();
        // Non-blocking stdout so the output pump is never parked in a write to a
        // slow terminal: it must stay free to notice the ring dropped and repaint.
        let prev_out_flags = set_stdout_nonblocking();

        // Seed the remote PTY with the current size.
        let (cols, rows) = terminal_size();
        block_on(proc.resize(cols, rows));

        let stop = Arc::new(AtomicBool::new(false));
        let render = Arc::new(RenderControl::default());
        let input = Arc::new(LocalInput::default());
        let reader = spawn_input_reader(Arc::clone(&proc), Arc::clone(&input), Arc::clone(&stop));
        let output = spawn_output_pump(Arc::clone(&proc), Arc::clone(&stop), Arc::clone(&render));
        // Browser-open and localhost-port forwarding (guest-gated by the launch
        // flags); idles harmlessly when the session opted out.
        let forward = spawn_forward_consumer(Arc::clone(&proc));
        // The clipboard bridge, both directions, rides the same opt-in as the
        // clipboard launch flag: mirror in-guest copies onto the local
        // clipboard, and scan stdin for pastes/drags to send the other way.
        let forward_clipboard = proc.forward_clipboard();
        let clipboard = forward_clipboard.then(|| spawn_clipboard_consumer(Arc::clone(&proc)));

        if forward_clipboard {
            drive_input_forwarding(
                PasteBridge::new(Arc::clone(&proc), Arc::clone(&render), Arc::clone(&input)),
                &stop,
            );
        } else {
            drive_input(&proc, &input, &stop);
        }
        // A failed keystroke write ends the input loop. While the Sailbox is
        // asleep the stream waits for a keystroke that can no longer come, so
        // end it; otherwise let the stream finish on its own (the command's
        // Exit is usually right behind a write refused because it exited).
        while !stop.load(Ordering::Relaxed) {
            if proc.link_state() == LinkState::Asleep {
                proc.close();
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }

        // Tear down in reverse order so the terminal is always usable afterwards.
        let _ = output.join();
        let _ = reader.join();
        let _ = forward.join();
        if let Some(consumer) = clipboard {
            let _ = consumer.join();
        }
        restore_stdout_flags(prev_out_flags);
        restore_stdin_flags(prev_in_flags);
        restore_sigwinch(prev_winch);
        restore_terminal(&saved);

        // A witnessed Exit is the command's real result. When the stream ended
        // without one, the command did not exit; the box was parked (put to
        // sleep) or otherwise became unreachable mid-session. Report that instead
        // of calling wait(), which would block forever on an Exit an interactive
        // shell never emits; the box's session stays intact for a fresh reconnect.
        match proc.try_wait() {
            Some(result) => result,
            None if input.quit.load(Ordering::Relaxed) => Err(SailError::Execution {
                code: RpcStatus::Unavailable,
                detail: format!(
                    "left the session while disconnected; \
                     open a new one with `sail box shell {}`",
                    proc.sailbox_id(),
                ),
            }),
            None => match proc.link_end() {
                Some(LinkEnd::Denied(message)) => Err(SailError::PermissionDenied {
                    message: message.clone(),
                }),
                Some(end) => Err(SailError::Execution {
                    code: RpcStatus::Unavailable,
                    detail: format!(
                        "the Sailbox {}, so the shell session ended",
                        if *end == LinkEnd::Terminated {
                            "was terminated"
                        } else {
                            "failed"
                        },
                    ),
                }),
                None => Err(SailError::Execution {
                    code: RpcStatus::Unavailable,
                    detail: format!(
                        "the box became unavailable and the shell session ended; \
                     reconnect with `sail box shell {}`",
                        proc.sailbox_id(),
                    ),
                }),
            },
        }
    }

    /// Keystrokes held for the session before the reader stops taking more
    /// from the terminal, so input during a long disconnection cannot grow
    /// without limit.
    const LOCAL_INPUT_CAP: usize = 1 << 20;

    /// First key of the local quit escape, Ctrl-^ (as in mosh). Followed by
    /// `.` it ends a disconnected session; see [`QuitEscape`].
    const QUIT_PREFIX: u8 = 0x1e;

    /// Ctrl-D as the terminal sends it in raw mode. It also ends a
    /// disconnected session; see [`QuitEscape`].
    const CTRL_D: u8 = 0x04;

    /// Keystrokes read from the local terminal by their own thread, so the
    /// quit escape still works while a keystroke write is stuck behind a lost
    /// connection. The input loops read from here instead of the terminal.
    #[derive(Default)]
    struct LocalInput {
        queue: Mutex<VecDeque<u8>>,
        eof: AtomicBool,
        failed: AtomicBool,
        /// The person typed the quit escape.
        quit: AtomicBool,
    }

    /// One read from [`LocalInput`], shaped like a non-blocking `read(2)`.
    enum InputRead {
        Data(usize),
        Empty,
        Eof,
        Failed,
    }

    impl LocalInput {
        fn read(&self, buf: &mut [u8]) -> InputRead {
            let mut queue = self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !queue.is_empty() {
                let n = queue.len().min(buf.len());
                for (slot, byte) in buf.iter_mut().zip(queue.drain(..n)) {
                    *slot = byte;
                }
                return InputRead::Data(n);
            }
            if self.failed.load(Ordering::Relaxed) {
                InputRead::Failed
            } else if self.eof.load(Ordering::Relaxed) {
                InputRead::Eof
            } else {
                InputRead::Empty
            }
        }

        fn push(&self, bytes: &[u8]) {
            self.queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(bytes);
        }

        fn is_full(&self) -> bool {
            self.queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                >= LOCAL_INPUT_CAP
        }
    }

    /// The local quit escape: Ctrl-D, or Ctrl-^ then `.`, ends the session,
    /// but only while it is disconnected at every key, so an application that
    /// binds either keeps it the rest of the time. Ctrl-^ twice sends one Ctrl-^, and
    /// Ctrl-^ then any other key, or then `.` after reconnecting, sends both.
    #[derive(Default)]
    struct QuitEscape {
        armed: bool,
    }

    impl QuitEscape {
        /// Append the bytes to forward to `out`; true when the escape completed.
        fn filter(&mut self, bytes: &[u8], disconnected: bool, out: &mut Vec<u8>) -> bool {
            for &byte in bytes {
                if self.armed {
                    self.armed = false;
                    match byte {
                        b'.' | CTRL_D if disconnected => return true,
                        QUIT_PREFIX => out.push(QUIT_PREFIX),
                        other => out.extend_from_slice(&[QUIT_PREFIX, other]),
                    }
                } else if byte == CTRL_D && disconnected {
                    return true;
                } else if byte == QUIT_PREFIX && disconnected {
                    self.armed = true;
                } else {
                    out.push(byte);
                }
            }
            false
        }

        /// Once reconnected, hand a held Ctrl-^ to the application.
        fn release(&mut self, disconnected: bool, out: &mut Vec<u8>) {
            if self.armed && !disconnected {
                self.armed = false;
                out.push(QUIT_PREFIX);
            }
        }
    }

    /// Spawn the thread that reads the local terminal into `input`, watching
    /// for the quit escape. Once `input` holds [`LOCAL_INPUT_CAP`] bytes it
    /// pauses while attached, and while disconnected it keeps reading for the
    /// escape but discards the rest. It stops at local EOF, a read error, the
    /// escape, or once the session ends.
    fn spawn_input_reader(
        proc: Arc<ExecProcess>,
        input: Arc<LocalInput>,
        stop: Arc<AtomicBool>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            let mut escape = QuitEscape::default();
            let mut forward = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                forward.clear();
                escape.release(proc.link_state() != LinkState::Attached, &mut forward);
                if !forward.is_empty() {
                    input.push(&forward);
                }
                if input.is_full() && proc.link_state() == LinkState::Attached {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                let n = unsafe {
                    libc::read(
                        libc::STDIN_FILENO,
                        buf.as_mut_ptr().cast::<libc::c_void>(),
                        buf.len(),
                    )
                };
                match n.cmp(&0) {
                    std::cmp::Ordering::Greater => {
                        forward.clear();
                        let disconnected = proc.link_state() != LinkState::Attached;
                        let quit = escape.filter(&buf[..n as usize], disconnected, &mut forward);
                        if !(disconnected && input.is_full()) {
                            input.push(&forward);
                        }
                        if quit {
                            input.quit.store(true, Ordering::Relaxed);
                            // Closing ends the stream and cancels a keystroke
                            // write still waiting on the connection, which
                            // unwinds the input and output loops.
                            proc.close();
                            return;
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        input.eof.store(true, Ordering::Relaxed);
                        return;
                    }
                    std::cmp::Ordering::Less => {
                        // A nonblocking read with no data yet (WouldBlock), or
                        // one a handled signal such as SIGWINCH interrupted
                        // (Interrupted), is transient: back off briefly and
                        // retry.
                        let err = std::io::Error::last_os_error();
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) {
                            thread::sleep(Duration::from_millis(10));
                        } else {
                            input.failed.store(true, Ordering::Relaxed);
                            return;
                        }
                    }
                }
            }
        })
    }

    /// Shared switches between the input side and the output pump: `paused`
    /// stops the pump writing to the terminal while an upload progress line
    /// owns it, and `bracketed_paste` tracks whether the guest application has
    /// paste bracketing (DEC mode 2004) on, so injected pastes are framed the
    /// way the terminal would frame a real one. Public only because the pump
    /// is driven directly by integration tests.
    #[doc(hidden)]
    #[derive(Default)]
    pub struct RenderControl {
        paused: AtomicBool,
        bracketed_paste: AtomicBool,
    }

    /// Least time between screen-repaint requests while the local terminal is
    /// too slow to keep up: without a bound a persistently-behind reader would
    /// ask on every drop and flood the guest with resync requests. Capping repaint
    /// requests to one per 100 ms is plenty to keep the screen current.
    const RESYNC_MIN_INTERVAL: Duration = Duration::from_millis(100);

    /// Most backlog the pump buffers toward the terminal before it stops draining
    /// the ring. Holding the cap small means a slow terminal quickly lets the
    /// ring back up and drop-oldest, which the reader reports as a drop — the
    /// signal that triggers a repaint. Larger would just make the terminal crawl
    /// further through stale frames before recovering.
    const OUTPUT_PENDING_CAP: usize = 256 * 1024;

    /// Spawn the thread that renders merged PTY output to the terminal, then
    /// signals stop when the stream ends. The terminal fd is already non-blocking
    /// (set by [`run_interactive`]).
    fn spawn_output_pump(
        proc: Arc<ExecProcess>,
        stop: Arc<AtomicBool>,
        render: Arc<RenderControl>,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut reader = proc
                .reader(OutputStream::Stdout)
                .expect("interactive stdout reader is taken once");
            let mut sink = RawFdWriter(libc::STDOUT_FILENO);
            drive_output_pump(&mut reader, &mut sink, &proc, &render);
            stop.store(true, Ordering::Relaxed);
        })
    }

    /// Least time between retries of a port that could not be forwarded because
    /// its local port was busy. The port watcher only re-reports the guest's
    /// listeners when the set changes, so this retry covers a local port freeing
    /// up while the guest server keeps running.
    const FORWARD_RETRY_INTERVAL: Duration = Duration::from_secs(3);

    /// Spawn the thread that acts on the session's local-forwarding events. It
    /// drains until the stream ends (the accessor then returns `None`). Active
    /// port forwards are held for the life of the session and dropped on exit.
    fn spawn_forward_consumer(proc: Arc<ExecProcess>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut forwards: HashMap<u16, crate::forward::PortForward> = HashMap::new();
            // Ports whose local port was busy, so the forward could not bind. Kept
            // so the bind is retried on the interval below in case the local port
            // frees up.
            let mut conflicts: HashSet<u16> = HashSet::new();
            loop {
                // Wait for the next event, but only until the retry interval when
                // there are conflicts to re-attempt; otherwise wait indefinitely.
                let next: Result<Option<ForwardEvent>, tokio::time::error::Elapsed> =
                    if conflicts.is_empty() {
                        Ok(block_on(proc.next_forward_event()))
                    } else {
                        block_on(async {
                            tokio::time::timeout(FORWARD_RETRY_INTERVAL, proc.next_forward_event())
                                .await
                        })
                    };
                match next {
                    Ok(Some(ForwardEvent::OpenUrl(url))) => {
                        let open = if !is_openable_scheme(&url) {
                            // open_local_url only opens http(s); skip building a
                            // forward for a URL it would refuse anyway.
                            false
                        } else if let Some(port) = crate::forward::forwardable_local_port(&url) {
                            // A URL for a server in the box: forward its port, then
                            // open the bound local address (rewritten below). If it
                            // can't be forwarded, don't open it against the user's
                            // own machine. A login's localhost callback is a server
                            // too, so the port watcher forwards it the same way.
                            if ensure_forward(&proc, &mut forwards, &mut conflicts, port) {
                                true
                            } else {
                                notify_local_port_busy(&url, port);
                                false
                            }
                        } else if crate::forward::is_unforwardable_loopback_url(&url) {
                            // A loopback URL the tunnel can't reach: opening it would
                            // hit the user's own machine, not the sandbox.
                            notify_loopback_unreachable(&url);
                            false
                        } else if let Some(callback) = crate::forward::redirect_callback(&url) {
                            // An external login URL whose redirect returns to a
                            // loopback callback. Don't start a login whose redirect,
                            // carrying the auth code, would hit the user's machine
                            // rather than the sandbox.
                            match callback {
                                // Open only once the callback port is actually
                                // forwarded. The snapshot precedes this URL, so a
                                // listening forwardable callback is already in
                                // `forwards`; a port not there is one whose local
                                // port is busy or whose server is not listening on a
                                // reachable address, and an immediate redirect would
                                // hit the user's own machine.
                                crate::forward::RedirectCallback::Forwardable(port)
                                    if forwards.contains_key(&port) =>
                                {
                                    true
                                }
                                crate::forward::RedirectCallback::Forwardable(port) => {
                                    notify_callback_unforwarded(&url, port);
                                    false
                                }
                                // A loopback the tunnel cannot dial at all.
                                crate::forward::RedirectCallback::Unreachable => {
                                    notify_callback_unreachable(&url);
                                    false
                                }
                            }
                        } else {
                            // An external URL with no localhost callback: open it.
                            true
                        };
                        if open {
                            let url = crate::forward::rewrite_loopback_url(&url, |remote| {
                                forwards
                                    .get(&remote)
                                    .map(crate::forward::PortForward::local_port)
                            });
                            open_local_url(&url);
                        }
                    }
                    Ok(Some(ForwardEvent::PortSnapshot(ports))) => {
                        // Reconcile against the authoritative set: drop forwards and
                        // conflicts for servers that are gone, then forward the rest.
                        let listening: HashSet<u16> = ports.iter().copied().collect();
                        forwards.retain(|port, _| listening.contains(port));
                        conflicts.retain(|port| listening.contains(port));
                        for port in ports {
                            ensure_forward(&proc, &mut forwards, &mut conflicts, port);
                        }
                    }
                    // The stream ended.
                    Ok(None) => break,
                    // No event within the interval: retry any port whose local port
                    // was busy, in case it has since freed up.
                    Err(_) => {
                        for port in conflicts.iter().copied().collect::<Vec<_>>() {
                            ensure_forward(&proc, &mut forwards, &mut conflicts, port);
                        }
                    }
                }
            }
        })
    }

    /// Forward `port` (guest to the same local port) if it is not already
    /// forwarded. Returns whether the port is now forwarded. The local port
    /// always matches the guest port and is never remapped: a login callback
    /// redirect targets that exact port, so binding elsewhere would send the
    /// browser to whatever already holds the local port rather than the sandbox.
    /// A busy local port is recorded in `conflicts` and retried on the interval.
    fn ensure_forward(
        proc: &Arc<ExecProcess>,
        forwards: &mut HashMap<u16, crate::forward::PortForward>,
        conflicts: &mut HashSet<u16>,
        port: u16,
    ) -> bool {
        if forwards.contains_key(&port) {
            return true;
        }
        if let Ok(forward) = block_on(proc.forward_port(port, port)) {
            forwards.insert(port, forward);
            conflicts.remove(&port);
            true
        } else {
            conflicts.insert(port);
            false
        }
    }

    /// Notify that a URL the Sailbox asked to open targets a loopback address the
    /// sandbox cannot reach, so it was not opened against the user's own machine.
    fn notify_loopback_unreachable(url: &str) {
        let notice = format!(
            "\r\n[sail] not opening {url}: it targets a loopback address the sandbox cannot reach\r\n"
        );
        let _ = RawFdWriter(libc::STDERR_FILENO).write_all(notice.as_bytes());
    }

    /// Notify that a Sailbox server was not opened because its port is already in use
    /// on the local machine, so the forward could not bind it.
    fn notify_local_port_busy(url: &str, port: u16) {
        let notice = format!("\r\n[sail] not opening {url}: local port {port} is in use\r\n");
        let _ = RawFdWriter(libc::STDERR_FILENO).write_all(notice.as_bytes());
    }

    /// Whether `open_local_url` would open this URL (it opens only http(s)).
    fn is_openable_scheme(url: &str) -> bool {
        url.starts_with("http://") || url.starts_with("https://")
    }

    /// Notify that a login was not opened because its localhost callback port is
    /// not forwarded (its local port is busy, or its server is not listening on a
    /// reachable address), so the provider's redirect could not reach the sandbox.
    fn notify_callback_unforwarded(url: &str, port: u16) {
        let notice = format!(
            "\r\n[sail] not opening {url}: its login callback port {port} is not forwarded\r\n"
        );
        let _ = RawFdWriter(libc::STDERR_FILENO).write_all(notice.as_bytes());
    }

    /// Notify that a login was not opened because its callback is a loopback
    /// address the sandbox cannot reach, so the redirect would hit the user's
    /// own machine.
    fn notify_callback_unreachable(url: &str) {
        let notice = format!(
            "\r\n[sail] not opening {url}: its login callback is a loopback address the sandbox cannot reach\r\n"
        );
        let _ = RawFdWriter(libc::STDERR_FILENO).write_all(notice.as_bytes());
    }

    /// Open a URL in the user's local browser, best-effort. Only http(s) URLs
    /// are opened, so a sandbox process cannot drive arbitrary local handlers.
    /// The child inherits no terminal, so an opener's own output can't corrupt
    /// the session. Silent on success: the browser tab appearing is the signal.
    fn open_local_url(url: &str) {
        if !is_openable_scheme(url) {
            return;
        }
        let _ = local_browser_command(url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }

    /// The platform command that opens a URL in the default browser. This
    /// module is Unix-only, so the choice is macOS `open` or Linux `xdg-open`.
    fn local_browser_command(url: &str) -> std::process::Command {
        let program = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let mut command = std::process::Command::new(program);
        command.arg(url);
        command
    }

    /// Shown once a lost connection has not come back within this long, so
    /// a brief blip reattaches silently.
    const RECONNECT_NOTICE_AFTER: Duration = Duration::from_secs(1);
    const RECONNECTING_NOTICE: &[u8] =
        b"\r\n[sail] Connection lost. Reconnecting... (press Ctrl-D to quit)\r\n";
    const ASLEEP_NOTICE: &[u8] =
        b"\r\n[sail] The Sailbox went to sleep. Press any key to wake it (Ctrl-D to quit).\r\n";
    const PAUSED_NOTICE: &[u8] =
        b"\r\n[sail] The Sailbox is paused. Your input is sent once it is resumed (Ctrl-D to quit).\r\n";

    /// Decides when to print a status line about the connection. Each line is
    /// printed once per disconnection; the screen repaint sent on reattach
    /// replaces it.
    #[derive(Default)]
    struct LinkNotice {
        /// When the current disconnection began, if one is underway.
        lost_at: Option<Instant>,
        shown: Option<LinkState>,
    }

    impl LinkNotice {
        fn update(&mut self, state: LinkState, now: Instant) -> Option<&'static [u8]> {
            match state {
                LinkState::Attached => {
                    self.lost_at = None;
                    self.shown = None;
                    None
                }
                LinkState::Reconnecting => {
                    let lost_at = *self.lost_at.get_or_insert(now);
                    if self.shown.is_some() || now.duration_since(lost_at) < RECONNECT_NOTICE_AFTER
                    {
                        return None;
                    }
                    self.shown = Some(LinkState::Reconnecting);
                    Some(RECONNECTING_NOTICE)
                }
                LinkState::Asleep => {
                    self.lost_at.get_or_insert(now);
                    // A paused box that was resumed may read asleep until
                    // the write that landed wakes it.
                    if matches!(self.shown, Some(LinkState::Asleep | LinkState::Paused)) {
                        return None;
                    }
                    self.shown = Some(LinkState::Asleep);
                    Some(ASLEEP_NOTICE)
                }
                LinkState::Paused => {
                    self.lost_at.get_or_insert(now);
                    if self.shown == Some(LinkState::Paused) {
                        return None;
                    }
                    self.shown = Some(LinkState::Paused);
                    Some(PAUSED_NOTICE)
                }
            }
        }
    }

    /// Render one live output stream onto a terminal `sink` until the stream
    /// ends, favoring a current screen over a faithful replay.
    ///
    /// The terminal writer must never block the loop: a slow terminal has to keep
    /// the pump free to notice the ring dropped and ask the guest to repaint the
    /// current screen ([`ExecProcess::resync`]). So `sink` is written
    /// non-blockingly, backlog is held to [`OUTPUT_PENDING_CAP`] so the ring
    /// backs up and drops-oldest when the terminal falls behind, and a reported
    /// drop discards the torn backlog and requests a repaint rather than crawling
    /// the slow terminal through stale frames it will never catch. The command is
    /// detached on the server, so none of this ever blocks it.
    ///
    /// Generic over the sink so the drop-to-repaint behavior is testable against a
    /// deliberately slow writer without a real terminal.
    #[doc(hidden)]
    pub fn drive_output_pump<W: Write>(
        reader: &mut crate::exec::StreamReader,
        sink: &mut W,
        proc: &Arc<ExecProcess>,
        render: &RenderControl,
    ) {
        let mut pending: Vec<u8> = Vec::new();
        let mut last_resync: Option<Instant> = None;
        // Hold an observed drop until a repaint is actually requested. resync_due
        // only fires once per RESYNC_MIN_INTERVAL, so a drop seen during that
        // cooldown would otherwise be forgotten, leaving the screen showing a
        // torn, partial frame.
        let mut resync_pending = false;
        let mut modes = BracketedPasteTracker::default();
        let mut copies = proc.forward_clipboard().then(Osc52Copies::default);
        // Kept for the session: on X11 a selection lives only as long as the
        // handle that set it.
        let mut clipboard: Option<arboard::Clipboard> = None;
        // Copies waiting for the terminal to take the output they were in,
        // each with the count of bytes written to the terminal once its
        // sequence has been. A copy in output discarded unwritten is not made.
        let mut queued: VecDeque<(usize, String)> = VecDeque::new();
        // Bytes written to the terminal so far; `pending` follows them.
        let mut written_total = 0usize;
        // A drop seen while reading chunks, handled once they are read.
        let mut dropped = false;
        // `reset` is set when `bytes` begins a repaint, which supersedes
        // what came before, and `gap` when output before `bytes` is missing
        // or superseded, so no sequence begun before it continues in it.
        // Returns the texts `bytes` copies, each with the length `pending`
        // has up to the end of its sequence.
        let mut take = |bytes: &[u8], reset: bool, gap: bool, pending: &mut Vec<u8>| {
            if reset {
                pending.clear();
            }
            if gap {
                modes.tail.clear();
            }
            modes.scan(bytes, render);
            let base = pending.len();
            pending.extend_from_slice(bytes);
            let Some(copies) = copies.as_mut() else {
                return Vec::new();
            };
            if gap {
                copies.clear();
            }
            let mut copied = copies.scan(bytes);
            for (end, _) in &mut copied {
                *end += base;
            }
            copied
        };
        let mut notice = LinkNotice::default();
        loop {
            // An upload progress line owns the terminal: stop rendering (and
            // stop draining the ring, which then backs up and drops-oldest just
            // like a slow terminal — the existing repaint path heals it).
            if render.paused.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            if let Some(line) = notice.update(proc.link_state(), Instant::now()) {
                pending.extend_from_slice(line);
            }
            // Push as much backlog as the terminal accepts right now, without
            // blocking on it.
            let mut flushed = false;
            if !pending.is_empty() {
                let written = write_nonblocking(sink, &pending);
                if written > 0 {
                    pending.drain(..written);
                    written_total += written;
                    make_written_copies(&mut queued, written_total, &mut clipboard);
                    flushed = true;
                }
            }
            // Refill from the ring, but only up to the cap: leaving the rest in
            // the ring lets it back up and drop-oldest when the terminal is slow.
            let mut progressed = false;
            if pending.len() < OUTPUT_PENDING_CAP {
                // Don't wait for new data while there is still backlog to push.
                let wait = if pending.is_empty() {
                    Duration::from_millis(50)
                } else {
                    Duration::ZERO
                };
                match reader.next(wait) {
                    ReadStep::Chunk(bytes) => {
                        // A Snapshot reset the ring: `bytes` is the repaint, and
                        // it supersedes the stale backlog buffered toward the
                        // terminal. Drop that backlog before queuing the repaint
                        // so the finished screen renders at once instead of stuck
                        // behind bytes the slow terminal will never finish
                        // draining (the bounded end-of-stream flush would give up
                        // before reaching it).
                        let (reset, gap) = reader.took_gap_before_chunk();
                        // A repaint supersedes what was dropped before it, and
                        // the copies in the backlog it replaces.
                        dropped = (dropped && !reset) || gap;
                        if reset {
                            queued.clear();
                        }
                        let copied = take(&bytes, reset, reset || gap, &mut pending);
                        queued.extend(
                            copied
                                .into_iter()
                                .map(|(end, text)| (written_total + end, text)),
                        );
                        progressed = true;
                    }
                    ReadStep::Eof => {
                        // An empty repaint at the end still supersedes the
                        // backlog.
                        if reader.took_reset() {
                            pending.clear();
                            queued.clear();
                        }
                        written_total += flush_blocking(sink, &pending);
                        make_written_copies(&mut queued, written_total, &mut clipboard);
                        return;
                    }
                    ReadStep::Pending => {}
                }
                while pending.len() < OUTPUT_PENDING_CAP {
                    match reader.try_next() {
                        // Honor a reset here too: the repaint can land in this
                        // batch drain when the Snapshot arrives after next()
                        // above already returned a stale chunk this iteration.
                        Some(more) => {
                            let (reset, gap) = reader.took_gap_before_chunk();
                            dropped = (dropped && !reset) || gap;
                            if reset {
                                queued.clear();
                            }
                            let copied = take(&more, reset, reset || gap, &mut pending);
                            queued.extend(
                                copied
                                    .into_iter()
                                    .map(|(end, text)| (written_total + end, text)),
                            );
                        }
                        None => break,
                    }
                }
            }
            // A repaint after the last chunk read, which may have no bytes of
            // its own to come with, supersedes the backlog, and drops seen
            // with this batch's chunks. The ring evicting output we had not
            // shown leaves the backlog a torn tail, so it is dropped and the
            // current screen repainted instead. A reset clears the ring's
            // drop flag, so a drop it still reports came after the repaint.
            // Either way no sequence begun in the dropped backlog continues
            // afterwards.
            let reset = reader.took_reset();
            let torn = (std::mem::take(&mut dropped) && !reset) | reader.took_drop();
            if reset || torn {
                pending.clear();
                queued.clear();
                take(&[], false, true, &mut pending);
                resync_pending |= torn;
            }
            if resync_pending && resync_due(&mut last_resync) {
                resync_pending = false;
                let handle = Arc::clone(proc);
                crate::runtime::runtime().spawn(async move { handle.resync().await });
            }
            // Yield when no new ring data was read and bytes are still queued,
            // either because the backlog is at the cap (so the ring can back up
            // and drop-oldest for a slow terminal) or because the terminal is
            // back-pressured and accepted nothing (so the loop does not spin).
            // A terminal actively draining a partial backlog is making progress,
            // so it keeps looping.
            if !progressed
                && !pending.is_empty()
                && (pending.len() >= OUTPUT_PENDING_CAP || !flushed)
            {
                thread::sleep(Duration::from_millis(5));
            }
        }
    }

    /// Write what the terminal will take right now, returning the bytes accepted.
    /// A full terminal (`WouldBlock`), or any transient error, accepts zero and
    /// the caller keeps the rest rather than propagating a terminal write error.
    fn write_nonblocking<W: Write>(sink: &mut W, buf: &[u8]) -> usize {
        sink.write(buf).unwrap_or(0)
    }

    /// End of stream: land the final bytes even against a non-blocking terminal,
    /// but bounded so a wedged terminal cannot hang the exit.
    /// Write `buf`, giving up on a terminal that stays full, and return how
    /// much was written.
    fn flush_blocking<W: Write>(sink: &mut W, buf: &[u8]) -> usize {
        let mut off = 0;
        for _ in 0..2000 {
            if off >= buf.len() {
                break;
            }
            match sink.write(&buf[off..]) {
                Ok(0) => thread::sleep(Duration::from_millis(1)),
                Ok(n) => off += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => break,
            }
        }
        let _ = sink.flush();
        off
    }

    /// A `Write` over a raw fd. On a non-blocking fd a full pipe surfaces as a
    /// `WouldBlock` error rather than parking the thread.
    struct RawFdWriter(libc::c_int);

    impl Write for RawFdWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let n = unsafe { libc::write(self.0, buf.as_ptr().cast(), buf.len()) };
            if n < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Whether enough time has passed since the last repaint request to send
    /// another, stamping the clock when it returns true.
    fn resync_due(last: &mut Option<Instant>) -> bool {
        let now = Instant::now();
        if last.is_none_or(|t| now.duration_since(t) >= RESYNC_MIN_INTERVAL) {
            *last = Some(now);
            true
        } else {
            false
        }
    }

    /// Forward raw stdin bytes to the guest, draining pending resizes, until the
    /// output stream ends or local stdin closes.
    fn drive_input(proc: &Arc<ExecProcess>, input: &LocalInput, stop: &AtomicBool) {
        let mut buf = [0u8; 4096];
        let mut stdin_open = true;
        while !stop.load(Ordering::Relaxed) {
            if RESIZE_PENDING.swap(false, Ordering::Relaxed) {
                let (cols, rows) = terminal_size();
                block_on(proc.resize(cols, rows));
            }
            if !stdin_open {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            match input.read(&mut buf) {
                InputRead::Data(n) => {
                    if block_on(proc.write_stdin(&buf[..n])).is_err() {
                        break; // remote closed stdin or exec ended
                    }
                }
                InputRead::Eof => {
                    // Local stdin reached EOF: send EOF and stop reading it, but
                    // keep draining output until the remote process exits.
                    let _ = block_on(proc.close_stdin());
                    stdin_open = false;
                }
                InputRead::Empty => thread::sleep(Duration::from_millis(10)),
                InputRead::Failed => break,
            }
        }
    }

    // --- local paste and drag-and-drop forwarding ---

    /// Parent of the per-session directory where forwarded drops and pastes
    /// land. Each session gets its own subdirectory (see PasteBridge::new) so
    /// two shells' drops of the same name never collide, and a cancel/failure
    /// rollback only ever deletes files this session uploaded.
    const GUEST_DROPS_ROOT: &str = "/tmp/sail-drops";
    /// Wait this long before drawing the upload progress line: most drops are
    /// small images that land invisibly fast, and flashing a progress line for
    /// them would just flicker the screen.
    const UPLOAD_UI_DELAY: Duration = Duration::from_millis(400);
    /// Largest content pushed onto the guest clipboard in one request (the
    /// service rejects encoded messages over 4 MiB). Larger images upload as
    /// files and paste as a guest path instead.
    const CLIPBOARD_PUSH_MAX: usize = 3 * 1024 * 1024;
    /// Longest a clipboard push may hold the input thread. The push must
    /// complete before the Ctrl+V chord is forwarded (or the guest
    /// application would paste the clipboard's previous content), and the
    /// input thread is what sequences both, so a slow guest stalls keystroke
    /// forwarding for the push's duration. This deadline bounds that stall
    /// well under the request's own; on expiry the caller falls back exactly as
    /// for any other failed push. It must exceed the guest agent's own 3s
    /// serve-verification budget (saild's guestClipboardCmdTimeout) with
    /// round-trip slack, or a push the guest completed near its budget would
    /// be misclassified as failed.
    const CLIPBOARD_PUSH_DEADLINE: Duration = Duration::from_secs(5);
    /// Upload stream granularity: small enough for responsive progress, large
    /// enough that per-message overhead is noise.
    const UPLOAD_CHUNK_BYTES: usize = 256 * 1024;
    /// How long an ambiguous escape-sequence prefix waits for its remaining
    /// bytes before being forwarded as a real keypress. Terminals send
    /// sequences in one burst, so only a human typing a lone ESC waits this out.
    const CARRY_FLUSH_AFTER: Duration = Duration::from_millis(25);

    /// Tracks DEC private mode 2004 (bracketed paste) in the guest's output so
    /// injected pastes are framed exactly as the terminal would frame a real
    /// one. Snapshot repaints re-assert tracked modes, so the flag survives
    /// reattach and heals after any dropped chunk.
    #[derive(Default)]
    struct BracketedPasteTracker {
        tail: Vec<u8>,
    }

    impl BracketedPasteTracker {
        fn scan(&mut self, bytes: &[u8], render: &RenderControl) {
            const INTRO: &[u8] = b"\x1b[?";
            let mut buf = std::mem::take(&mut self.tail);
            buf.extend_from_slice(bytes);
            let mut i = 0;
            while i < buf.len() {
                let Some(at) = find(&buf[i..], INTRO) else {
                    break;
                };
                let start = i + at;
                let mut j = start + INTRO.len();
                while j < buf.len() && (buf[j].is_ascii_digit() || buf[j] == b';') {
                    j += 1;
                }
                let Some(&fin) = buf.get(j) else {
                    // Split across chunks: carry the partial sequence, bounded —
                    // a parameter run longer than any real mode list is not one.
                    if buf.len() - start <= 24 {
                        self.tail = buf[start..].to_vec();
                    }
                    return;
                };
                if fin == b'h' || fin == b'l' {
                    let in_params = buf[start + INTRO.len()..j]
                        .split(|&b| b == b';')
                        .any(|param| param == b"2004");
                    if in_params {
                        render.bracketed_paste.store(fin == b'h', Ordering::Relaxed);
                    }
                }
                i = j;
            }
            let keep = partial_suffix_len(&buf, INTRO);
            if keep > 0 {
                self.tail = buf[buf.len() - keep..].to_vec();
            }
        }
    }

    /// Largest OSC 52 sequence read. The guest mirrors at most 256 KiB of
    /// copied text, which base64 makes about 342 KiB; a copy far beyond that
    /// is not one the user means to paste.
    const OSC52_MAX_BYTES: usize = 1 << 20;

    /// Copies a guest program asked the terminal to make with OSC 52
    /// (`ESC ] 52 ; selections ; base64` ended by BEL or ST), as tmux, vim,
    /// Codex and others do. They are made on the local clipboard too, since
    /// many terminals ignore OSC 52 or ask before honoring it. The sequence
    /// still reaches the terminal.
    #[derive(Default)]
    struct Osc52Copies {
        /// A sequence begun in an earlier chunk.
        tail: Vec<u8>,
        /// How much of `tail` is known to hold no terminator.
        searched: usize,
    }

    impl Osc52Copies {
        /// The texts copied by the sequences that end in `bytes`, each with
        /// the offset in `bytes` just past its end.
        fn scan(&mut self, bytes: &[u8]) -> Vec<(usize, String)> {
            const INTRO: &[u8] = b"\x1b]52;";
            let mut buf = std::mem::take(&mut self.tail);
            let carried = buf.len();
            let searched = std::mem::take(&mut self.searched);
            buf.extend_from_slice(bytes);
            let mut copied = Vec::new();
            let mut i = 0;
            while let Some(at) = find(&buf[i..], INTRO) {
                let start = i + at;
                let body = start + INTRO.len();
                let from = if start == 0 { body.max(searched) } else { body };
                // The sequence ends at BEL or ST. CAN, SUB, and an ESC that
                // does not begin ST cancel it; the ESC may begin another
                // sequence. An ESC at the end may be the start of ST.
                let end = buf[from..].iter().enumerate().find_map(|(n, &b)| match b {
                    0x07 => Some((from + n, 1)),
                    0x18 | 0x1a => Some((from + n, 0)),
                    0x1b => match buf.get(from + n + 1) {
                        Some(&b'\\') => Some((from + n, 2)),
                        Some(_) => Some((from + n, 0)),
                        None => None,
                    },
                    _ => None,
                });
                let Some((end, terminator)) = end else {
                    if buf.len() - start <= OSC52_MAX_BYTES {
                        buf.drain(..start);
                        // The last byte may be the ESC of an ST.
                        self.searched = buf.len() - 1;
                        self.tail = buf;
                    }
                    return copied;
                };
                if terminator == 0 {
                    // Cancelled: copies nothing, and an ESC is read again.
                    i = if buf[end] == 0x1b { end } else { end + 1 };
                    continue;
                }
                if end + terminator - start <= OSC52_MAX_BYTES {
                    if let Some(text) = osc52_text(&buf[body..end]) {
                        copied.push((end + terminator - carried, text));
                    }
                }
                i = end + terminator;
            }
            let keep = partial_suffix_len(&buf[i..], INTRO);
            buf.drain(..buf.len() - keep);
            self.tail = buf;
            copied
        }

        /// Forget any sequence begun in an earlier chunk.
        fn clear(&mut self) {
            self.tail.clear();
            self.searched = 0;
        }
    }

    /// `written_total` bytes have reached the terminal: make the last copy
    /// whose sequence they complete.
    fn make_written_copies(
        queued: &mut VecDeque<(usize, String)>,
        written_total: usize,
        clipboard: &mut Option<arboard::Clipboard>,
    ) {
        let mut made = None;
        while queued.front().is_some_and(|copy| copy.0 <= written_total) {
            made = queued.pop_front().map(|(_, text)| text);
        }
        if let Some(text) = made {
            set_local_clipboard(clipboard, text);
        }
    }

    fn set_local_clipboard(clipboard: &mut Option<arboard::Clipboard>, text: String) {
        if clipboard.is_none() {
            *clipboard = arboard::Clipboard::new().ok();
        }
        if let Some(clipboard) = clipboard.as_mut() {
            let _ = clipboard.set_text(text);
        }
    }

    /// The text an OSC 52 body (`selections ; base64`) copies to the
    /// clipboard. A query (`?`), a copy to another selection only, or data
    /// that is not base64 of UTF-8 text copies nothing.
    fn osc52_text(body: &[u8]) -> Option<String> {
        use base64::Engine as _;
        let at = body.iter().position(|&b| b == b';')?;
        let (selections, data) = (&body[..at], &body[at + 1..]);
        if !(selections.is_empty() || selections.contains(&b'c')) || data == b"?" {
            return None;
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(data)
            .ok()?;
        String::from_utf8(decoded).ok()
    }

    /// Applies guest-clipboard updates to the local clipboard, so a copy made
    /// inside the guest is pasteable locally. Exits when the stream ends. The
    /// clipboard handle stays alive for the whole session: on X11 the
    /// selection lives only as long as the handle that set it.
    fn spawn_clipboard_consumer(proc: Arc<ExecProcess>) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let mut clipboard: Option<arboard::Clipboard> = None;
            while let Some((mime, data)) = block_on(proc.next_clipboard_update()) {
                if mime != "text/plain" {
                    continue;
                }
                let Ok(text) = String::from_utf8(data) else {
                    continue;
                };
                if clipboard.is_none() {
                    clipboard = arboard::Clipboard::new().ok();
                }
                if let Some(clipboard) = clipboard.as_mut() {
                    let _ = clipboard.set_text(text);
                }
            }
        })
    }

    /// The forwarding twin of [`drive_input`]: stdin is scanned for bracketed
    /// pastes and the Ctrl+V chord (see [`crate::shell_input`]), which the
    /// [`PasteBridge`] turns into uploads, clipboard pushes, or verbatim
    /// forwards; every other byte passes through untouched.
    fn drive_input_forwarding(mut bridge: PasteBridge, stop: &AtomicBool) {
        let mut scanner = InputScanner::new();
        let mut buf = [0u8; 4096];
        let mut stdin_open = true;
        let mut carry_deadline: Option<Instant> = None;
        while !stop.load(Ordering::Relaxed) {
            if RESIZE_PENDING.swap(false, Ordering::Relaxed) {
                let (cols, rows) = terminal_size();
                block_on(bridge.proc.resize(cols, rows));
            }
            if !stdin_open {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            // Keystrokes read while an upload owned stdin replay first, in
            // order. Refresh the idle deadline like the read path: a replayed
            // bare ESC lands in the scanner carry and must still flush.
            if !bridge.stashed_input.is_empty() {
                let stashed = std::mem::take(&mut bridge.stashed_input);
                if bridge.handle_events(scanner.scan(&stashed)).is_err() {
                    break;
                }
                carry_deadline = scanner
                    .has_idle_carry()
                    .then(|| Instant::now() + CARRY_FLUSH_AFTER);
            }
            match bridge.input.read(&mut buf) {
                InputRead::Data(n) => {
                    if bridge.handle_events(scanner.scan(&buf[..n])).is_err() {
                        break;
                    }
                    carry_deadline = scanner
                        .has_idle_carry()
                        .then(|| Instant::now() + CARRY_FLUSH_AFTER);
                }
                InputRead::Eof => {
                    // Local stdin reached EOF: flush whatever the scanner
                    // still held verbatim, then send EOF and keep draining
                    // output until the remote process exits.
                    let held = scanner.flush_all();
                    if !held.is_empty() && bridge.forward(&held).is_err() {
                        break;
                    }
                    let _ = block_on(bridge.proc.close_stdin());
                    stdin_open = false;
                }
                InputRead::Failed => break,
                InputRead::Empty => {
                    // Idle: a carried escape prefix past its deadline was a
                    // real keypress (e.g. a lone ESC), so forward it now. A
                    // mid-paste hold is exempt (has_idle_carry): the paste's
                    // remaining bytes are still coming.
                    if carry_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        carry_deadline = None;
                        if scanner.has_idle_carry() {
                            let carry = scanner.take_carry();
                            if !carry.is_empty() && bridge.forward(&carry).is_err() {
                                break;
                            }
                        }
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }

    /// What the local clipboard holds, in the order pasting cares about:
    /// copied files, then an image (encoded to PNG), then text.
    enum LocalClipboard {
        Files(Vec<PathBuf>),
        Image(Vec<u8>),
        Text(String),
        Empty,
    }

    fn read_local_clipboard() -> LocalClipboard {
        let Ok(mut clipboard) = arboard::Clipboard::new() else {
            return LocalClipboard::Empty;
        };
        if let Ok(files) = clipboard.get().file_list() {
            if !files.is_empty() && files.iter().all(|path| path.is_file()) {
                return LocalClipboard::Files(files);
            }
        }
        if let Ok(image) = clipboard.get_image() {
            if let Some(png) = encode_png(&image) {
                return LocalClipboard::Image(png);
            }
        }
        match clipboard.get_text() {
            Ok(text) if !text.is_empty() => LocalClipboard::Text(text),
            _ => LocalClipboard::Empty,
        }
    }

    /// Encode arboard's raw RGBA image as PNG, the type both the guest
    /// clipboard and the coding agents (claude, codex) inside it expect. The
    /// encoder itself rejects a byte buffer that does not match the
    /// dimensions.
    fn encode_png(image: &arboard::ImageData) -> Option<Vec<u8>> {
        let width = u32::try_from(image.width).ok()?;
        let height = u32::try_from(image.height).ok()?;
        let mut out = Vec::new();
        let mut encoder = png::Encoder::new(&mut out, width, height);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().ok()?;
        writer.write_image_data(&image.bytes).ok()?;
        writer.finish().ok()?;
        Some(out)
    }

    /// How an upload ended: the guest paths to paste, a user cancel (paste
    /// nothing), or a failure (the caller falls back to forwarding the
    /// original bytes where that makes sense).
    enum UploadOutcome {
        Done(Vec<String>),
        Cancelled,
        Failed,
    }

    /// One file (or in-memory blob) headed for the guest drops directory.
    struct UploadSource {
        guest_path: String,
        size: u64,
        data: UploadData,
    }

    enum UploadData {
        File(PathBuf),
        Memory(Vec<u8>),
    }

    /// Turns scanned paste events into guest activity: dragged files upload
    /// and paste as guest paths, Ctrl+V forwards the local clipboard (guest
    /// clipboard when supported, file upload otherwise), everything else
    /// forwards verbatim.
    struct PasteBridge {
        proc: Arc<ExecProcess>,
        render: Arc<RenderControl>,
        input: Arc<LocalInput>,
        /// This session's private drop directory under GUEST_DROPS_ROOT, keyed
        /// by the exec id so concurrent shells never share a path.
        drops_dir: String,
        /// Guest file names already used this session, so a re-dropped name
        /// gets a numbered variant instead of clobbering a different file.
        used_names: std::collections::HashSet<String>,
        /// Whether this guest accepts clipboard writes. `None` until the first
        /// attempt; latches `Some(false)` on Unimplemented (the guest has no
        /// clipboard) so later pastes skip straight to the file fallback.
        guest_clipboard: Option<bool>,
        /// Keystrokes read while an upload owned stdin (watching for cancel),
        /// replayed in order afterwards.
        stashed_input: Vec<u8>,
    }

    impl PasteBridge {
        fn new(
            proc: Arc<ExecProcess>,
            render: Arc<RenderControl>,
            input: Arc<LocalInput>,
        ) -> PasteBridge {
            // Name the per-session directory by the hash of the exec id, so it
            // is always path-safe, never `.`/`..`, and collision-free whatever
            // shape the server's id takes: two concurrent sessions never share
            // a path.
            use sha2::Digest as _;
            use std::fmt::Write as _;
            let digest = sha2::Sha256::digest(proc.exec_request_id_internal().as_bytes());
            let mut session = String::with_capacity(16);
            for byte in &digest[..8] {
                let _ = write!(session, "{byte:02x}");
            }
            let drops_dir = format!("{GUEST_DROPS_ROOT}/{session}");
            PasteBridge {
                proc,
                render,
                input,
                drops_dir,
                used_names: std::collections::HashSet::new(),
                guest_clipboard: None,
                stashed_input: Vec::new(),
            }
        }

        fn handle_events(&mut self, events: Vec<InputEvent>) -> Result<(), ()> {
            for event in events {
                match event {
                    InputEvent::Bytes(bytes) => self.forward(&bytes)?,
                    InputEvent::Paste(body) => self.handle_paste(&body)?,
                    InputEvent::PasteChord(chord) => self.handle_chord(&chord)?,
                }
            }
            Ok(())
        }

        /// Forward bytes to the guest pty. Err means the exec ended and the
        /// input loop should stop, matching [`drive_input`].
        fn forward(&self, bytes: &[u8]) -> Result<(), ()> {
            block_on(self.proc.write_stdin(bytes)).map_err(|_| ())
        }

        /// A completed bracketed paste: a drag-and-drop of local files uploads
        /// them and pastes the guest paths; any other paste (or a failed
        /// upload) forwards byte-identically.
        fn handle_paste(&mut self, body: &[u8]) -> Result<(), ()> {
            if let Some(files) = dropped_local_files(body) {
                match self.upload_files(&files) {
                    UploadOutcome::Done(paths) => {
                        // Keep the trailing separator the drag arrived with,
                        // so typing right after the drop stays a separate
                        // argument exactly as it would locally.
                        let text = format!("{} ", paths.join(" "));
                        return self.inject_uploaded(&text, &paths);
                    }
                    UploadOutcome::Cancelled => return Ok(()),
                    UploadOutcome::Failed => {} // fall through to the original paste
                }
            }
            let mut raw = Vec::with_capacity(body.len() + PASTE_START.len() + PASTE_END.len());
            raw.extend_from_slice(PASTE_START);
            raw.extend_from_slice(body);
            raw.extend_from_slice(PASTE_END);
            self.forward(&raw)
        }

        /// A Ctrl+V press: make the local clipboard available in the guest,
        /// then (except for uploads that paste a path themselves) deliver the
        /// keypress so the guest application reacts to it as usual.
        fn handle_chord(&mut self, chord: &[u8]) -> Result<(), ()> {
            match read_local_clipboard() {
                LocalClipboard::Files(files) => {
                    let outcome = self.upload_files(&files);
                    self.inject_outcome(outcome)
                }
                LocalClipboard::Image(png) => {
                    if png.len() <= CLIPBOARD_PUSH_MAX && self.push_clipboard("image/png", &png) {
                        return self.forward(chord);
                    }
                    // No guest clipboard took the image (a non-devbox guest, or
                    // one over the push cap): upload it and inject the path, the
                    // only way a paste-reading agent gets an image here. The
                    // chord is deliberately not forwarded — doing so would paste
                    // whatever the guest clipboard last held. Unlike text, which
                    // is on the clipboard almost always (so the text path never
                    // injects, to keep vim visual-block / quoted-insert intact),
                    // an image on the clipboard is almost always an intended
                    // paste, so injecting wins over preserving a Ctrl+V binding.
                    let name = self.reserve_name(std::path::Path::new("clipboard.png"));
                    let outcome = self.upload_bytes(name, png);
                    self.inject_outcome(outcome)
                }
                LocalClipboard::Text(text) => {
                    if text.len() <= CLIPBOARD_PUSH_MAX {
                        // The chord is forwarded whether or not the push
                        // lands. Ctrl+V is not only "paste": vim binds it to
                        // visual-block and readline to quoted-insert, and text
                        // sits on the clipboard almost always, so replacing a
                        // failed push with injected text or a file would fire
                        // inside those apps constantly. On a guest with no
                        // clipboard the app's paste read finds nothing, which
                        // is exactly how these sessions behaved before
                        // clipboard forwarding existed; terminal-level paste
                        // (Cmd+V) remains the text path there.
                        self.push_clipboard("text/plain", text.as_bytes());
                        return self.forward(chord);
                    }
                    // Too big for a clipboard push. Forwarding the keypress
                    // anyway would paste whatever the guest clipboard last
                    // held, so deliver the text as a file like an oversized
                    // image.
                    let name = self.reserve_name(std::path::Path::new("clipboard.txt"));
                    let outcome = self.upload_bytes(name, text.into_bytes());
                    self.inject_outcome(outcome)
                }
                LocalClipboard::Empty => self.forward(chord),
            }
        }

        /// Try to place content on the guest clipboard, reporting success,
        /// waiting at most [`CLIPBOARD_PUSH_DEADLINE`]. Unimplemented latches
        /// the fallback: this guest has no clipboard (its image ships none,
        /// or it predates the feature), and that never changes mid-session.
        /// Other failures, the deadline included, just skip the push this
        /// time.
        fn push_clipboard(&mut self, mime: &str, data: &[u8]) -> bool {
            if self.guest_clipboard == Some(false) {
                return false;
            }
            let pushed = block_on(async {
                tokio::time::timeout(CLIPBOARD_PUSH_DEADLINE, self.proc.set_clipboard(mime, data))
                    .await
            });
            match pushed {
                Ok(Ok(())) => {
                    self.guest_clipboard = Some(true);
                    true
                }
                Ok(Err(SailError::Execution {
                    code: RpcStatus::Unimplemented,
                    ..
                })) => {
                    self.guest_clipboard = Some(false);
                    false
                }
                Ok(Err(_)) | Err(_) => false,
            }
        }

        /// Paste an upload's guest paths, or forward nothing when the upload was
        /// cancelled or failed (the original chord/paste was already handled).
        fn inject_outcome(&mut self, outcome: UploadOutcome) -> Result<(), ()> {
            match outcome {
                UploadOutcome::Done(paths) => self.inject_uploaded(&paths.join(" "), &paths),
                UploadOutcome::Cancelled | UploadOutcome::Failed => Ok(()),
            }
        }

        /// Paste the uploaded files' guest paths, deleting the uploads if the
        /// session ends before the paste can land, so nothing stays
        /// unreferenced in the guest.
        fn inject_uploaded(&mut self, text: &str, paths: &[String]) -> Result<(), ()> {
            if self.inject(text).is_err() {
                let _ = block_on(self.proc.remove_guest_files(paths));
                return Err(());
            }
            Ok(())
        }

        /// Paste text into the guest exactly as the terminal would: bracketed
        /// while the application has mode 2004 on, plain keystrokes otherwise.
        fn inject(&self, text: &str) -> Result<(), ()> {
            let bracketed = self.render.bracketed_paste.load(Ordering::Relaxed);
            let mut bytes = Vec::with_capacity(text.len() + 16);
            if bracketed {
                bytes.extend_from_slice(PASTE_START);
            }
            bytes.extend_from_slice(text.as_bytes());
            if bracketed {
                bytes.extend_from_slice(PASTE_END);
            }
            self.forward(&bytes)
        }

        /// Reserve a guest-side name for an upload, numbering repeats
        /// (photo.png, photo-2.png, ...) within the session.
        fn reserve_name(&mut self, path: &std::path::Path) -> String {
            let base = sanitize_drop_name(path);
            let mut name = base.clone();
            let mut n = 1;
            while !self.used_names.insert(name.clone()) {
                n += 1;
                name = match base.rsplit_once('.') {
                    Some((stem, ext)) if !stem.is_empty() => format!("{stem}-{n}.{ext}"),
                    _ => format!("{base}-{n}"),
                };
            }
            name
        }

        fn upload_files(&mut self, files: &[PathBuf]) -> UploadOutcome {
            let mut sources = Vec::with_capacity(files.len());
            for path in files {
                let Ok(meta) = std::fs::metadata(path) else {
                    return UploadOutcome::Failed;
                };
                let name = self.reserve_name(path);
                sources.push(UploadSource {
                    guest_path: format!("{}/{name}", self.drops_dir),
                    size: meta.len(),
                    data: UploadData::File(path.clone()),
                });
            }
            self.run_upload(sources)
        }

        /// Upload one in-memory blob under a name already reserved via
        /// [`reserve_name`](Self::reserve_name).
        fn upload_bytes(&mut self, name: String, bytes: Vec<u8>) -> UploadOutcome {
            self.run_upload(vec![UploadSource {
                guest_path: format!("{}/{name}", self.drops_dir),
                size: bytes.len() as u64,
                data: UploadData::Memory(bytes),
            }])
        }

        /// Stream the sources to the guest while this thread keeps the
        /// terminal responsive: a progress line appears for slow uploads, Esc
        /// or Ctrl+C cancels (aborting the write streams, which the guest
        /// discards uncommitted), and other keystrokes are stashed for replay.
        /// Guest output rendering is paused throughout; if anything was drawn
        /// over the screen, a resync repaints it from the authoritative guest
        /// screen state.
        fn run_upload(&mut self, sources: Vec<UploadSource>) -> UploadOutcome {
            let total: u64 = sources.iter().map(|s| s.size).sum();
            let guest_paths: Vec<String> = sources.iter().map(|s| s.guest_path.clone()).collect();
            let label = if sources.len() == 1 {
                guest_paths[0]
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .to_string()
            } else {
                format!("{} files", sources.len())
            };
            self.render.paused.store(true, Ordering::Relaxed);
            let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let committed: Arc<std::sync::Mutex<Vec<String>>> =
                Arc::new(std::sync::Mutex::new(Vec::new()));
            // Spawn where the exec stream lives (see block_on): an embedding
            // runtime's channels must not be redialed on the crate's own
            // reactor.
            let handle = tokio::runtime::Handle::try_current()
                .unwrap_or_else(|_| crate::runtime::runtime().handle().clone());
            let mut task = handle.spawn(upload_task(
                Arc::clone(&self.proc),
                sources,
                Arc::clone(&progress),
                Arc::clone(&committed),
            ));
            let started = Instant::now();
            let mut ui = ProgressLine::new(label, total);
            let mut esc_at: Option<Instant> = None;
            let outcome = loop {
                if task.is_finished() {
                    // A bare ESC still inside its disambiguation window was a
                    // real keypress after all; replay it with the other
                    // stashed input instead of dropping it.
                    if esc_at.take().is_some() {
                        self.stashed_input.push(0x1b);
                    }
                    break match block_on(&mut task) {
                        Ok(Ok(())) => UploadOutcome::Done(guest_paths.clone()),
                        Ok(Err(err)) => {
                            self.roll_back_upload(&committed, &guest_paths);
                            ui.flash(&format!("[sail] upload failed: {err}"));
                            UploadOutcome::Failed
                        }
                        Err(_) => {
                            self.roll_back_upload(&committed, &guest_paths);
                            UploadOutcome::Failed
                        }
                    };
                }
                if self.input.quit.load(Ordering::Relaxed) {
                    // The quit escape ended the session while the link was
                    // down, so there is no guest to roll back or repaint.
                    task.abort();
                    let _ = block_on(&mut task);
                    ui.clear();
                    self.render.paused.store(false, Ordering::Relaxed);
                    return UploadOutcome::Cancelled;
                }
                if self.poll_cancel(&mut esc_at) {
                    // Aborting drops the in-flight writer mid-stream; the
                    // guest treats the torn stream as uncommitted and discards
                    // it.
                    task.abort();
                    let _ = block_on(&mut task);
                    self.roll_back_upload(&committed, &guest_paths);
                    ui.flash("[sail] upload canceled");
                    break UploadOutcome::Cancelled;
                }
                ui.tick(started, progress.load(Ordering::Relaxed));
                thread::sleep(Duration::from_millis(30));
            };
            ui.clear();
            self.render.paused.store(false, Ordering::Relaxed);
            if ui.wrote {
                // The progress line scribbled over the guest's screen; repaint
                // it from the guest's authoritative screen state.
                block_on(self.proc.resync());
            }
            outcome
        }

        /// Undo an upload that will paste nothing (cancelled or failed):
        /// delete the files that already committed — nothing points at them,
        /// and cancel means the user wants none of the dragged content in the
        /// guest — and release the reserved names so a retried drag lands on
        /// the same paths. Deletion is best effort: if the guest is unreachable
        /// the unreferenced files linger in its /tmp until the Sailbox goes away.
        fn roll_back_upload(
            &mut self,
            committed: &Arc<std::sync::Mutex<Vec<String>>>,
            guest_paths: &[String],
        ) {
            let committed = std::mem::take(&mut *committed.lock().unwrap());
            if !committed.is_empty() {
                let _ = block_on(self.proc.remove_guest_files(&committed));
            }
            for path in guest_paths {
                if let Some(name) = path.rsplit('/').next() {
                    self.used_names.remove(name);
                }
            }
        }

        /// Drain stdin during an upload. Ctrl+C cancels at once; a bare ESC
        /// cancels after a short pause — long enough for the rest of an escape
        /// sequence (an arrow key) to arrive and be stashed instead. Everything
        /// else is stashed and replayed after the upload.
        fn poll_cancel(&mut self, esc_at: &mut Option<Instant>) -> bool {
            let mut buf = [0u8; 256];
            while let InputRead::Data(n) = self.input.read(&mut buf) {
                let bytes = &buf[..n];
                for (idx, &byte) in bytes.iter().enumerate() {
                    if byte == 0x03 {
                        // Keys typed in the same burst as the cancel replay
                        // after the upload settles.
                        self.stashed_input.extend_from_slice(&bytes[idx + 1..]);
                        return true;
                    }
                    if esc_at.take().is_some() {
                        // The pending ESC was the start of a sequence after all.
                        self.stashed_input.push(0x1b);
                    }
                    if byte == 0x1b && idx == bytes.len() - 1 {
                        *esc_at = Some(Instant::now());
                    } else {
                        self.stashed_input.push(byte);
                    }
                }
            }
            // 60ms: longer than one 30ms poll tick of run_upload, so a split
            // escape sequence has a whole further poll to finish arriving.
            esc_at.is_some_and(|at| at.elapsed() >= Duration::from_millis(60))
        }
    }

    /// The background half of an upload: stream every source into the guest,
    /// publishing progress for the interactive thread's UI and each committed
    /// guest path for cancel's rollback.
    async fn upload_task(
        proc: Arc<ExecProcess>,
        sources: Vec<UploadSource>,
        progress: Arc<std::sync::atomic::AtomicU64>,
        committed: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Result<(), SailError> {
        use tokio::io::AsyncReadExt;
        for source in sources {
            let mut writer = proc.guest_file_writer(&source.guest_path);
            match source.data {
                UploadData::Memory(bytes) => {
                    for chunk in bytes.chunks(UPLOAD_CHUNK_BYTES) {
                        writer.write_chunk(chunk.to_vec()).await?;
                        progress.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    }
                }
                UploadData::File(path) => {
                    let mut file =
                        tokio::fs::File::open(&path)
                            .await
                            .map_err(|err| SailError::Internal {
                                message: format!("read {}: {err}", path.display()),
                            })?;
                    let mut chunk = vec![0u8; UPLOAD_CHUNK_BYTES];
                    loop {
                        let n = file
                            .read(&mut chunk)
                            .await
                            .map_err(|err| SailError::Internal {
                                message: format!("read {}: {err}", path.display()),
                            })?;
                        if n == 0 {
                            break;
                        }
                        writer.write_chunk(chunk[..n].to_vec()).await?;
                        progress.fetch_add(n as u64, Ordering::Relaxed);
                    }
                }
            }
            // Record the path before finish()'s await: finish closes the
            // client stream, so a cancel that aborts this task during the
            // await can still let the guest commit the file. Recording first
            // guarantees rollback has the path — an rm of a file that never
            // committed is a harmless no-op.
            committed.lock().unwrap().push(source.guest_path);
            writer.finish().await?;
        }
        Ok(())
    }

    /// The one-line upload status drawn at the cursor. It only ever repaints
    /// itself in place; whatever it overwrote is restored by the post-upload
    /// resync.
    struct ProgressLine {
        label: String,
        total: u64,
        wrote: bool,
        last_draw: Option<Instant>,
    }

    impl ProgressLine {
        fn new(label: String, total: u64) -> ProgressLine {
            ProgressLine {
                label,
                total,
                wrote: false,
                last_draw: None,
            }
        }

        fn tick(&mut self, started: Instant, sent: u64) {
            if !self.wrote && started.elapsed() < UPLOAD_UI_DELAY {
                return;
            }
            if self
                .last_draw
                .is_some_and(|last| last.elapsed() < Duration::from_millis(100))
            {
                return;
            }
            self.last_draw = Some(Instant::now());
            self.wrote = true;
            let percent = (sent.min(self.total) * 100)
                .checked_div(self.total)
                .unwrap_or(100);
            write_terminal_line(&format!(
                "[sail] uploading {}  {percent}%  {} / {}  (esc cancels)",
                self.label,
                format_bytes(sent),
                format_bytes(self.total),
            ));
        }

        /// Show a final status long enough to read before the screen repaints.
        fn flash(&mut self, message: &str) {
            self.wrote = true;
            write_terminal_line(message);
            thread::sleep(Duration::from_millis(1200));
        }

        fn clear(&mut self) {
            if self.wrote {
                write_terminal(ERASE_LINE);
            }
        }
    }

    /// Carriage return + erase-line: return to column 0 and clear the row, so
    /// the next write overwrites the cursor line in place.
    const ERASE_LINE: &[u8] = b"\r\x1b[2K";

    /// Overwrite the cursor line with `text`.
    fn write_terminal_line(text: &str) {
        let mut bytes = Vec::with_capacity(text.len() + ERASE_LINE.len());
        bytes.extend_from_slice(ERASE_LINE);
        bytes.extend_from_slice(text.as_bytes());
        write_terminal(&bytes);
    }

    fn write_terminal(bytes: &[u8]) {
        let mut sink = RawFdWriter(libc::STDOUT_FILENO);
        flush_blocking(&mut sink, bytes);
    }

    /// Sizes for the progress line, in the 1000-based MB/KB a file manager
    /// labels the dragged file with.
    fn format_bytes(n: u64) -> String {
        const MB: u64 = 1_000_000;
        if n >= 10 * MB {
            format!("{} MB", n / MB)
        } else if n >= MB {
            format!("{:.1} MB", n as f64 / MB as f64)
        } else {
            format!("{} KB", n.div_ceil(1000))
        }
    }

    // --- platform terminal plumbing ---

    /// Put the local terminal into raw mode, returning the saved settings.
    fn enter_raw_mode() -> Result<libc::termios, SailError> {
        unsafe {
            let mut saved: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &raw mut saved) != 0 {
                return Err(SailError::Internal {
                    message: format!(
                        "could not enter raw terminal mode: {}",
                        std::io::Error::last_os_error()
                    ),
                });
            }
            let mut raw = saved;
            libc::cfmakeraw(&raw mut raw);
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSADRAIN, &raw const raw) != 0 {
                return Err(SailError::Internal {
                    message: format!(
                        "could not enter raw terminal mode: {}",
                        std::io::Error::last_os_error()
                    ),
                });
            }
            Ok(saved)
        }
    }

    fn restore_terminal(saved: &libc::termios) {
        unsafe {
            let _ = libc::tcsetattr(
                libc::STDIN_FILENO,
                libc::TCSADRAIN,
                std::ptr::from_ref(saved),
            );
        }
    }

    type SigHandler = libc::sighandler_t;

    fn install_sigwinch() -> SigHandler {
        // `signal` takes the handler as a numeric `sighandler_t`; cast through a
        // concrete fn pointer first so this is a pointer-to-int cast, not a
        // fn-item-to-int cast.
        let handler = on_sigwinch as extern "C" fn(libc::c_int) as usize;
        unsafe { libc::signal(libc::SIGWINCH, handler) }
    }

    fn restore_sigwinch(prev: SigHandler) {
        unsafe {
            libc::signal(libc::SIGWINCH, prev);
        }
    }

    /// Put stdin into non-blocking mode so the input loop can interleave reads
    /// with resize handling and the stop flag. Returns the previous fcntl flags.
    fn set_stdin_nonblocking() -> libc::c_int {
        unsafe {
            let flags = libc::fcntl(libc::STDIN_FILENO, libc::F_GETFL);
            if flags >= 0 {
                libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            flags
        }
    }

    fn restore_stdin_flags(flags: libc::c_int) {
        if flags >= 0 {
            unsafe {
                libc::fcntl(libc::STDIN_FILENO, libc::F_SETFL, flags);
            }
        }
    }

    /// Put stdout into non-blocking mode so the output pump is never parked in a
    /// write to a slow terminal. Returns the previous fcntl flags.
    fn set_stdout_nonblocking() -> libc::c_int {
        unsafe {
            let flags = libc::fcntl(libc::STDOUT_FILENO, libc::F_GETFL);
            if flags >= 0 {
                libc::fcntl(libc::STDOUT_FILENO, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            flags
        }
    }

    fn restore_stdout_flags(flags: libc::c_int) {
        if flags >= 0 {
            unsafe {
                libc::fcntl(libc::STDOUT_FILENO, libc::F_SETFL, flags);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn osc52_copies_are_read_whole_from_any_split() {
            // "hi" is aGk=, "héllo" is aMOpbGxv.
            let stream = b"a\x1b]52;c;aGk=\x07b\x1b]52;;aMOpbGxv\x1b\\c".as_slice();
            for split in 0..=stream.len() {
                let mut copies = Osc52Copies::default();
                let mut texts = texts_of(copies.scan(&stream[..split]));
                texts.extend(texts_of(copies.scan(&stream[split..])));
                assert_eq!(texts, ["hi", "héllo"], "split at {split}");
            }
        }

        fn texts_of(copied: Vec<(usize, String)>) -> Vec<String> {
            copied.into_iter().map(|(_, text)| text).collect()
        }

        #[test]
        fn osc52_copies_are_read_whole_a_byte_at_a_time() {
            let stream = b"a\x1b]52;c;aGk=\x07b\x1b]52;;aMOpbGxv\x1b\\c".as_slice();
            let mut copies = Osc52Copies::default();
            let texts: Vec<String> = stream
                .iter()
                .flat_map(|byte| texts_of(copies.scan(std::slice::from_ref(byte))))
                .collect();
            assert_eq!(texts, ["hi", "héllo"]);
        }

        #[test]
        fn a_cancelled_osc52_copies_nothing_and_leaves_the_next_whole() {
            let mut copies = Osc52Copies::default();
            let texts = texts_of(copies.scan(b"\x1b]52;c;aaaa\x1b]52;c;aGk=\x07"));
            assert_eq!(texts, ["hi"]);
            let texts = texts_of(copies.scan(b"\x1b]52;c;aaaa\x18\x1b]52;c;aGk=\x07"));
            assert_eq!(texts, ["hi"]);
            assert!(copies.tail.is_empty());
        }

        #[test]
        fn osc52_queries_and_other_selections_copy_nothing() {
            let mut copies = Osc52Copies::default();
            assert!(copies.scan(b"\x1b]52;c;?\x07").is_empty());
            assert!(copies.scan(b"\x1b]52;p;aGk=\x07").is_empty());
            assert!(copies.scan(b"\x1b]52;c;not base64!\x07").is_empty());
            assert!(copies.scan(b"\x1b]52;c;//79\x07").is_empty());
            assert!(copies.tail.is_empty());
        }

        #[test]
        fn osc52_copies_beyond_the_limit_copy_nothing() {
            let mut copies = Osc52Copies::default();
            let mut stream = b"\x1b]52;c;".to_vec();
            stream.resize(OSC52_MAX_BYTES + 1, b'A');
            stream.push(0x07);
            assert!(copies.scan(&stream).is_empty());
            assert_eq!(copies.scan(b"\x1b]52;c;aGk=\x07"), [(12, "hi".to_string())]);
        }

        fn escape(chunks: &[&[u8]], disconnected: bool) -> (Vec<u8>, bool) {
            let mut filter = QuitEscape::default();
            let mut out = Vec::new();
            for chunk in chunks {
                if filter.filter(chunk, disconnected, &mut out) {
                    return (out, true);
                }
            }
            (out, false)
        }

        #[test]
        fn quit_escape_quits_only_while_disconnected() {
            assert_eq!(escape(&[b"ab\x1e."], true), (b"ab".to_vec(), true));
            // Split across reads.
            assert_eq!(escape(&[b"\x1e", b"."], true), (Vec::new(), true));
            // Attached, Ctrl-^ belongs to the application.
            assert_eq!(escape(&[b"\x1e."], false), (b"\x1e.".to_vec(), false));
            // Reconnected between the two keys: both reach the application.
            let mut filter = QuitEscape::default();
            let mut out = Vec::new();
            assert!(!filter.filter(b"\x1e", true, &mut out));
            assert!(!filter.filter(b".", false, &mut out));
            assert_eq!(out, b"\x1e.");
            // Reconnected with no key in between: the held Ctrl-^ goes to the
            // application, and a `.` in a later outage does not quit.
            let mut filter = QuitEscape::default();
            let mut out = Vec::new();
            assert!(!filter.filter(b"\x1e", true, &mut out));
            filter.release(true, &mut out);
            assert!(out.is_empty());
            filter.release(false, &mut out);
            assert_eq!(out, b"\x1e");
            assert!(!filter.filter(b".", true, &mut out));
            assert_eq!(out, b"\x1e.");
        }

        #[test]
        fn quit_escape_passes_other_keys_through() {
            assert_eq!(escape(&[b"\x1e\x1e"], true), (b"\x1e".to_vec(), false));
            assert_eq!(escape(&[b"\x1ex"], true), (b"\x1ex".to_vec(), false));
            assert_eq!(escape(&[b"\x03"], true), (b"\x03".to_vec(), false));
        }

        #[test]
        fn ctrl_d_quits_only_while_disconnected() {
            assert_eq!(escape(&[b"ab\x04"], true), (b"ab".to_vec(), true));
            assert_eq!(escape(&[b"\x1e", b"\x04"], true), (Vec::new(), true));
            // Attached, Ctrl-D belongs to the application.
            assert_eq!(escape(&[b"\x04"], false), (b"\x04".to_vec(), false));
        }

        #[test]
        fn link_notice_waits_out_brief_reconnects() {
            let start = Instant::now();
            let mut notice = LinkNotice::default();
            assert_eq!(notice.update(LinkState::Reconnecting, start), None);
            assert_eq!(
                notice.update(LinkState::Attached, start + Duration::from_millis(500)),
                None
            );
            // A fresh disconnection restarts the delay.
            let again = start + Duration::from_secs(2);
            assert_eq!(notice.update(LinkState::Reconnecting, again), None);
            assert_eq!(
                notice.update(LinkState::Reconnecting, again + RECONNECT_NOTICE_AFTER),
                Some(RECONNECTING_NOTICE)
            );
            // Printed once per disconnection.
            assert_eq!(
                notice.update(LinkState::Reconnecting, again + 2 * RECONNECT_NOTICE_AFTER),
                None
            );
        }

        #[test]
        fn link_notice_shows_sleep_at_once() {
            let now = Instant::now();
            let mut notice = LinkNotice::default();
            assert_eq!(notice.update(LinkState::Asleep, now), Some(ASLEEP_NOTICE));
            assert_eq!(notice.update(LinkState::Asleep, now), None);
            // Waking reconnects without a second notice.
            assert_eq!(
                notice.update(LinkState::Reconnecting, now + 2 * RECONNECT_NOTICE_AFTER),
                None
            );
            assert_eq!(notice.update(LinkState::Attached, now), None);
            assert_eq!(notice.update(LinkState::Asleep, now), Some(ASLEEP_NOTICE));
        }

        #[test]
        fn link_notice_shows_pause_once_through_the_wake() {
            let now = Instant::now();
            let mut notice = LinkNotice::default();
            assert_eq!(notice.update(LinkState::Asleep, now), Some(ASLEEP_NOTICE));
            assert_eq!(notice.update(LinkState::Paused, now), Some(PAUSED_NOTICE));
            assert_eq!(notice.update(LinkState::Paused, now), None);
            // Resumed: the write lands and wakes the box without another notice.
            assert_eq!(notice.update(LinkState::Asleep, now), None);
            assert_eq!(notice.update(LinkState::Reconnecting, now), None);
            assert_eq!(notice.update(LinkState::Attached, now), None);
            assert_eq!(notice.update(LinkState::Paused, now), Some(PAUSED_NOTICE));
        }

        #[test]
        fn resync_due_throttles_back_to_back_requests() {
            let mut last = None;
            // The first request is always due and stamps the clock.
            assert!(resync_due(&mut last));
            // A second request within RESYNC_MIN_INTERVAL is suppressed, so a
            // persistently-behind reader cannot flood the guest with resync requests.
            assert!(!resync_due(&mut last));
        }

        #[test]
        fn format_bytes_matches_its_thousand_based_labels() {
            assert_eq!(format_bytes(0), "0 KB");
            assert_eq!(format_bytes(1), "1 KB");
            assert_eq!(format_bytes(999_999), "1000 KB");
            assert_eq!(format_bytes(1_000_000), "1.0 MB");
            assert_eq!(format_bytes(3_200_000), "3.2 MB");
            assert_eq!(format_bytes(25_000_000), "25 MB");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_shell_quotes_an_explicit_path() {
        // A path with spaces runs as one literal program.
        assert_eq!(
            login_shell_command(Some("/opt/my tools/zsh")),
            "exec '/opt/my tools/zsh' -l"
        );
        // The default stays unquoted so the guest expands $SHELL.
        assert_eq!(
            login_shell_command(/* shell */ None),
            "exec ${SHELL:-/bin/bash} -l"
        );
    }

    #[tokio::test]
    async fn shell_requires_a_tty() {
        // Test processes have no TTY on stdin/stdout, so the precondition
        // fires before any network or terminal manipulation.
        let client = crate::Client::builder("sk_test")
            .api_url("http://127.0.0.1:1")
            .sailbox_api_url("http://127.0.0.1:1")
            .build()
            .expect("build");
        let err = client
            .sailbox("sb_test")
            .shell(/* command */ None, ShellOptions::default())
            .await
            .expect_err("no tty in tests");
        assert!(err.to_string().contains("interactive terminal"), "{err}");
    }
}
