/// Rust Router Binary for Interop Tests
///
/// Position: Router (RelaySwitch + RelayMaster)
/// Communicates with test orchestration via stdin/stdout (CBOR frames)
/// Connects to independent relay host processes via Unix sockets
///
/// Usage:
///   capdag-interop-router-rust --connect <socket-path> [--connect <another-socket>]
use capdag::bifaci::frame::SeqAssigner;
use capdag::bifaci::io::{FrameReader, FrameWriter};
use capdag::bifaci::relay_switch::RelaySwitch;
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};
use tokio::net::UnixStream;

#[derive(Debug)]
struct Args {
    socket_paths: Vec<String>,
}

fn parse_args() -> Args {
    let mut args = Args { socket_paths: Vec::new() };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--connect" => {
                i += 1;
                if i >= argv.len() {
                    eprintln!("ERROR: --connect requires a socket path argument");
                    std::process::exit(1);
                }
                args.socket_paths.push(argv[i].clone());
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

async fn connect_to_host(socket_path: &str) -> UnixStream {
    eprintln!("[Router] Connecting to relay host at: {}", socket_path);

    // Connect to the relay host's listening socket
    let stream = UnixStream::connect(socket_path).await.unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to connect to {}: {}", socket_path, e);
        std::process::exit(1);
    });

    eprintln!("[Router] Connected to relay host at {}", socket_path);
    stream
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    if args.socket_paths.is_empty() {
        eprintln!("ERROR: at least one --connect <socket-path> required");
        std::process::exit(1);
    }

    // Connect to all relay host sockets
    let mut sockets: Vec<UnixStream> = Vec::new();

    for socket_path in &args.socket_paths {
        let stream = connect_to_host(socket_path).await;
        sockets.push(stream);
    }

    // Create RelaySwitch with all host connections
    eprintln!("[Router] Creating RelaySwitch with {} host(s)", sockets.len());
    let cap_registry = Arc::new(capdag::cap::registry::CapRegistry::new_for_test());
    let mut switch = RelaySwitch::new(sockets, cap_registry).await.unwrap_or_else(|e| {
        eprintln!("Failed to create RelaySwitch: {}", e);
        std::process::exit(1);
    });

    eprintln!("[Router] RelaySwitch initialized, connected to {} relay host(s)", args.socket_paths.len());

    // Send initial RelayNotify to engine with aggregate capabilities.
    // The full capabilities from hosts were consumed during identity verification
    // in RelaySwitch::new(), so the engine hasn't seen them yet.
    {
        let stdout = tokio::io::stdout();
        let mut init_writer = FrameWriter::new(BufWriter::new(stdout));
        let caps = switch.capabilities().await;
        let limits = switch.limits().await;
        let notify = capdag::bifaci::frame::Frame::relay_notify(
            &caps,
            &limits,
        );
        init_writer.write(&notify).await.unwrap_or_else(|e| {
            eprintln!("Failed to write initial RelayNotify to engine: {}", e);
            std::process::exit(1);
        });
        eprintln!("[Router] Sent initial RelayNotify to engine ({} bytes)", caps.len());
    }

    // Router is a pure multiplexer - two independent tasks:
    //   - Task 1: stdin → channel (continuously read stdin, send frames to channel)
    //   - Task 2 (main): channel + RelaySwitch → stdout (multiplex stdin frames and master frames)

    use tokio::sync::mpsc;

    let (stdin_tx, mut stdin_rx) = mpsc::unbounded_channel();

    // Task 1: stdin → channel
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = FrameReader::new(BufReader::new(stdin));

        eprintln!("[Router/stdin] Starting stdin reader loop");
        loop {
            match reader.read().await {
                Ok(Some(frame)) => {
                    eprintln!("[Router/stdin] Read frame: {:?} (id={:?})", frame.frame_type, frame.id);
                    if stdin_tx.send(frame).is_err() {
                        eprintln!("[Router/stdin] Channel closed, exiting");
                        break;
                    }
                }
                Ok(None) => {
                    eprintln!("[Router/stdin] EOF on stdin, exiting");
                    break;
                }
                Err(e) => {
                    eprintln!("[Router/stdin] Error reading: {}", e);
                    break;
                }
            }
        }
    });

    // Main task: multiplex stdin_rx and RelaySwitch
    let stdout = tokio::io::stdout();
    let mut writer = FrameWriter::new(BufWriter::new(stdout));
    let mut stdout_seq = SeqAssigner::new();

    eprintln!("[Router/main] Starting main multiplexer loop");

    // Main loop: use tokio::select! for concurrent stdin and master frame reading
    loop {
        tokio::select! {
            // Try to read from stdin channel
            frame_opt = stdin_rx.recv() => {
                match frame_opt {
                    Some(frame) => {
                        eprintln!("[Router/main] Sending stdin frame to master: {:?} (id={:?})", frame.frame_type, frame.id);
                        let frame_id = frame.id.clone();
                        let is_req = frame.frame_type == capdag::bifaci::frame::FrameType::Req;
                        if let Err(e) = switch.send_to_master(frame, None).await {
                            eprintln!("[Router/main] Error sending to master: {}", e);
                            // On REQ failure, send ERR back to engine so it doesn't hang
                            if is_req {
                                let mut err_frame = capdag::bifaci::frame::Frame::err(frame_id, "NO_HANDLER", &e.to_string());
                                stdout_seq.assign(&mut err_frame);
                                if let Err(write_err) = writer.write(&err_frame).await {
                                    eprintln!("[Router/main] Failed to write ERR frame: {}", write_err);
                                }
                            }
                        }
                    }
                    None => {
                        eprintln!("[Router/main] stdin channel closed, shutting down");
                        break;
                    }
                }
            }

            // Try to read from masters via pump_one
            pump_result = switch.pump_one() => {
                match pump_result {
                    Ok(Some(mut frame)) => {
                        eprintln!("[Router/main] Received from master: {:?} (id={:?}, seq={}, payload_len={})",
                            frame.frame_type, frame.id, frame.seq,
                            frame.payload.as_ref().map_or(0, |p| p.len()));
                        stdout_seq.assign(&mut frame);
                        let encoded_size = capdag::bifaci::io::encode_frame(&frame).map(|b| b.len()).unwrap_or(0);
                        eprintln!("[Router/main] Writing frame to stdout: {:?} encoded_size={}", frame.frame_type, encoded_size);
                        if let Err(e) = writer.write(&frame).await {
                            eprintln!("[Router/main] Error writing to stdout: {}", e);
                            break;
                        }
                        eprintln!("[Router/main] Frame written successfully: {:?}", frame.frame_type);
                        if matches!(frame.frame_type, capdag::bifaci::frame::FrameType::End | capdag::bifaci::frame::FrameType::Err) {
                            stdout_seq.remove(&capdag::bifaci::frame::FlowKey::from_frame(&frame));
                        }
                    }
                    Ok(None) => {
                        // No frame available right now, continue
                    }
                    Err(e) => {
                        eprintln!("[Router/main] ERROR reading from masters: {} - Router exiting and closing connections!", e);
                        eprintln!("[Router/main] THIS IS A BUG - Router should NOT exit while test is running!");
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    eprintln!("[Router] Shutting down - relay hosts will continue running independently");
}
