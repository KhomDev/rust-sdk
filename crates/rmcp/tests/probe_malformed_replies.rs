#![cfg(all(feature = "server", feature = "client"))]

// Observation probe (not for upstream): feed malformed incoming lines to the
// stdio server transport and print what the transport yields to the service
// and what it writes back to the peer.

use std::time::Duration;

use rmcp::{RoleServer, transport::Transport, transport::async_rw::AsyncRwTransport};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const CASES: &[(&str, &str)] = &[
    ("control-not-jsonrpc", r#"{"foo":"bar"}"#),
    (
        "req-nonobject-params",
        r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":5}"#,
    ),
    (
        "req-typed-bad-params",
        r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":7}}"#,
    ),
    (
        "notif-cancelled-nonobject-params",
        r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":5}"#,
    ),
    (
        "notif-cancelled-bad-params",
        r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":{}}}"#,
    ),
    (
        "notif-progress-bad-params",
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":[]}}"#,
    ),
    (
        "notif-initialized-nonobject-params",
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":5}"#,
    ),
    (
        "notif-custom-nonobject-params",
        r#"{"jsonrpc":"2.0","method":"notifications/custom","params":5}"#,
    ),
    (
        "control-valid-notif-initialized",
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    ),
];

#[tokio::test]
async fn probe_malformed_message_replies() {
    for (i, (name, raw)) in CASES.iter().enumerate() {
        let (server_io, client_io) = tokio::io::duplex(65536);
        let (sr, sw) = tokio::io::split(server_io);
        let (cr, mut cw) = tokio::io::split(client_io);
        let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_server(sr, sw);

        let sentinel = 900 + i;
        let payload = format!(
            "{raw}\n{{\"jsonrpc\":\"2.0\",\"id\":{sentinel},\"method\":\"ping\"}}\n"
        );
        cw.write_all(payload.as_bytes()).await.unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match transport.receive().await {
                    Some(msg) => {
                        let v = serde_json::to_value(&msg).unwrap();
                        if v["id"] == serde_json::json!(sentinel) {
                            return "sentinel-reached".to_string();
                        }
                        println!("PROBE {name} | yielded-to-service: {v}");
                    }
                    None => return "transport-closed".to_string(),
                }
            }
        })
        .await;

        let mut peer = BufReader::new(cr);
        let mut replies = Vec::new();
        loop {
            let mut line = String::new();
            match tokio::time::timeout(Duration::from_millis(300), peer.read_line(&mut line)).await
            {
                Ok(Ok(n)) if n > 0 => replies.push(line.trim_end().to_string()),
                _ => break,
            }
        }
        println!(
            "PROBE {name} | input={raw} | receive={outcome:?} | replies={replies:?}"
        );
    }
}
