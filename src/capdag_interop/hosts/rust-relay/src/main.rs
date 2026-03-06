/// Multi-plugin relay host test binary for cross-language interop tests.
///
/// Creates an PluginHostRuntime managing N plugin subprocesses, with optional RelaySlave layer.
/// Communicates via raw CBOR frames on stdin/stdout OR Unix socket.
///
/// Without --relay:
///     stdin/stdout carry raw CBOR frames (PluginHostRuntime relay interface).
///
/// With --relay:
///     stdin/stdout OR socket carry CBOR frames including relay-specific types.
///     RelaySlave sits between stdin/stdout (or socket) and PluginHostRuntime.
///     Initial RelayNotify sent on startup.
///
/// With --listen <socket-path>:
///     Creates a Unix socket listener and accepts ONE connection from router.
///     Router and host are independent processes (not parent-child).
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::os::unix::net::UnixListener;
use std::process::Command;

use capdag::bifaci::host_runtime::PluginHostRuntime;
use capdag::bifaci::frame::Limits;
use capdag::bifaci::io::{FrameReader, FrameWriter};
use capdag::bifaci::relay::RelaySlave;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

#[derive(Debug)]
struct Args {
    plugins: Vec<String>,
    relay: bool,
    listen_socket: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        plugins: Vec::new(),
        relay: false,
        listen_socket: None,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--spawn" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("ERROR: --spawn requires a path argument");
                    std::process::exit(1);
                }
                args.plugins.push(argv[i].clone());
            }
            "--relay" => {
                args.relay = true;
            }
            "--listen" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("ERROR: --listen requires a socket path argument");
                    std::process::exit(1);
                }
                args.listen_socket = Some(argv[i].clone());
            }
            other => {
                eprintln!("ERROR: unknown argument: {}", other);
                std::process::exit(1);
            }
        }
        i += 1;
    }
    args
}

fn spawn_plugin(plugin_path: &str) -> (std::process::ChildStdout, std::process::ChildStdin, std::process::Child) {
    let mut cmd = Command::new(plugin_path);

    cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit());

    let mut child = cmd.spawn().unwrap_or_else(|e| {
        eprintln!("Failed to spawn {}: {}", plugin_path, e);
        std::process::exit(1);
    });

    let stdout = child.stdout.take().unwrap();
    let stdin = child.stdin.take().unwrap();
    (stdout, stdin, child)
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    if args.plugins.is_empty() {
        eprintln!("ERROR: at least one --spawn required");
        std::process::exit(1);
    }

    let mut host = PluginHostRuntime::new();
    let mut children: Vec<std::process::Child> = Vec::new();

    for plugin_path in &args.plugins {
        let (stdout, stdin, child) = spawn_plugin(plugin_path);
        children.push(child);

        let plugin_read = tokio::fs::File::from_std(unsafe {
            std::fs::File::from_raw_fd(stdout.into_raw_fd())
        });
        let plugin_write = tokio::fs::File::from_std(unsafe {
            std::fs::File::from_raw_fd(stdin.into_raw_fd())
        });

        if let Err(e) = host.attach_plugin(plugin_read, plugin_write).await {
            eprintln!("Failed to attach {}: {}", plugin_path, e);
            std::process::exit(1);
        }
    }

    if args.relay {
        if let Some(socket_path) = args.listen_socket {
            run_with_relay_socket(host, &socket_path).await;
        } else {
            run_with_relay(host).await;
        }
    } else {
        run_direct(host).await;
    }

    // Cleanup
    for mut child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
}

async fn run_direct(mut host: PluginHostRuntime) {
    let relay_read = tokio::io::stdin();
    let relay_write = tokio::io::stdout();

    if let Err(e) = host.run(relay_read, relay_write, || Vec::new()).await {
        eprintln!("PluginHostRuntime.run error: {}", e);
        std::process::exit(1);
    }
}

async fn run_with_relay(mut host: PluginHostRuntime) {
    // Use tokio UnixStream pairs for slave ↔ host communication
    let (host_socket, slave_socket) = tokio::net::UnixStream::pair()
        .expect("Failed to create UnixStream pair");

    // Split sockets for bidirectional async I/O
    let (host_read, host_write) = host_socket.into_split();
    let (slave_local_read, slave_local_write) = slave_socket.into_split();

    // Run host in a tokio task
    let host_handle = tokio::spawn(async move {
        host.run(host_read, host_write, || Vec::new()).await
    });

    // Convert stdin/stdout to async
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    // Run slave in async task
    let slave = RelaySlave::new(slave_local_read, slave_local_write);
    let socket_reader = FrameReader::new(BufReader::new(stdin));
    let socket_writer = FrameWriter::new(BufWriter::new(stdout));

    // Send initial RelayNotify with CAP_IDENTITY (always available).
    // PluginHostRuntime will send updated RelayNotify after plugins connect.
    let initial_caps = vec![capdag::standard::caps::CAP_IDENTITY.to_string()];
    let initial_caps_json = serde_json::to_vec(&initial_caps)
        .expect("Failed to serialize initial caps array");
    eprintln!("[RelayHost] Initial RelayNotify payload: {} bytes: {:?}",
              initial_caps_json.len(),
              std::str::from_utf8(&initial_caps_json).unwrap_or("<invalid UTF-8>"));
    let limits = Limits::default();

    let slave_result = slave.run(
        socket_reader,
        socket_writer,
        Some((&initial_caps_json, &limits)),
    ).await;

    if let Err(e) = slave_result {
        eprintln!("RelaySlave.run error: {}", e);
    }

    // Abort host (slave finished, host should exit)
    host_handle.abort();
    let _ = host_handle.await;
}

async fn run_with_relay_socket(mut host: PluginHostRuntime, socket_path: &str) {
    // Remove existing socket if it exists
    let _ = std::fs::remove_file(socket_path);

    // Create Unix socket listener (std, then convert to tokio)
    let listener = UnixListener::bind(socket_path).unwrap_or_else(|e| {
        eprintln!("Failed to bind socket {}: {}", socket_path, e);
        std::process::exit(1);
    });

    eprintln!("[RelayHost] Listening on socket: {}", socket_path);

    // Convert to tokio listener
    listener.set_nonblocking(true).expect("Failed to set non-blocking");
    let tokio_listener = tokio::net::UnixListener::from_std(listener)
        .expect("Failed to convert to tokio UnixListener");

    // Accept ONE connection from router
    let (socket, _addr) = tokio_listener.accept().await.unwrap_or_else(|e| {
        eprintln!("Failed to accept connection: {}", e);
        std::process::exit(1);
    });

    eprintln!("[RelayHost] Router connected");

    // Use tokio UnixStream pairs for slave ↔ host communication
    let (host_socket, slave_socket) = tokio::net::UnixStream::pair()
        .expect("Failed to create UnixStream pair");

    // Split sockets for bidirectional async I/O
    let (host_read, host_write) = host_socket.into_split();
    let (slave_local_read, slave_local_write) = slave_socket.into_split();

    // Run host in a tokio task
    let host_handle = tokio::spawn(async move {
        host.run(host_read, host_write, || Vec::new()).await
    });

    // Split the incoming socket for bidirectional I/O
    let (socket_read, socket_write) = socket.into_split();

    // Run slave in async task
    let slave = RelaySlave::new(slave_local_read, slave_local_write);
    let socket_reader = FrameReader::new(BufReader::new(socket_read));
    let socket_writer = FrameWriter::new(BufWriter::new(socket_write));

    // Send initial RelayNotify with CAP_IDENTITY (always available).
    // PluginHostRuntime will send updated RelayNotify after plugins connect.
    let initial_caps = vec![capdag::standard::caps::CAP_IDENTITY.to_string()];
    let initial_caps_json = serde_json::to_vec(&initial_caps)
        .expect("Failed to serialize initial caps array");
    eprintln!("[RelayHost] Initial RelayNotify payload: {} bytes: {:?}",
              initial_caps_json.len(),
              std::str::from_utf8(&initial_caps_json).unwrap_or("<invalid UTF-8>"));
    let limits = Limits::default();

    let slave_result = slave.run(
        socket_reader,
        socket_writer,
        Some((&initial_caps_json, &limits)),
    ).await;

    if let Err(e) = slave_result {
        eprintln!("RelaySlave.run error: {}", e);
    }

    eprintln!("[RelayHost] Slave finished, router disconnected");

    // Abort host (slave finished, host should exit)
    host_handle.abort();
    let _ = host_handle.await;

    eprintln!("[RelayHost] Shutting down");
}
