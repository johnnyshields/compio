//! Trace individual H2 roundtrip times — tests single-thread vs multi-thread.
//!
//! Run: cargo run -p compio-h2 --example stall_trace --release

#[path = "../benches/support.rs"]
mod support;

use std::time::Instant;

use support::{compio_h2_client_connect, compio_h2_server_loop, make_payload};

fn main() {
    // === Single-threaded: client + server on same runtime ===
    eprintln!("=== Single-threaded (client + server same runtime) ===");
    let rt = compio_runtime::Runtime::new().unwrap();
    let stalls_single = rt.block_on(async {
        let listener = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        compio_runtime::spawn(compio_h2_server_loop(listener)).detach();
        let mut send_req = compio_h2_client_connect(addr).await;

        for _ in 0..5 {
            support::compio_h2_roundtrip(&mut send_req, make_payload(100)).await;
        }

        let payload = make_payload(65536);
        let mut stalls = 0;
        for i in 0..30 {
            let t = Instant::now();
            support::compio_h2_roundtrip(&mut send_req, payload.clone()).await;
            let us = t.elapsed().as_micros();
            if us > 1000 {
                stalls += 1;
                eprintln!("  #{:>2}: {:>8}µs  STALL", i, us);
            }
        }
        eprintln!("  Stalls: {}/30\n", stalls);
        stalls
    });

    // === Multi-threaded: server on separate thread ===
    eprintln!("=== Multi-threaded (server on separate thread) ===");
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let server_rt = compio_runtime::Runtime::new().unwrap();
        server_rt.block_on(async {
            let listener = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addr_tx.send(addr).unwrap();
            compio_h2_server_loop(listener).await;
        });
    });
    let server_addr = addr_rx.recv().unwrap();

    let client_rt = compio_runtime::Runtime::new().unwrap();
    let stalls_multi = client_rt.block_on(async {
        let mut send_req = compio_h2_client_connect(server_addr).await;

        for _ in 0..5 {
            support::compio_h2_roundtrip(&mut send_req, make_payload(100)).await;
        }

        let payload = make_payload(65536);
        let mut stalls = 0;
        for i in 0..30 {
            let t = Instant::now();
            support::compio_h2_roundtrip(&mut send_req, payload.clone()).await;
            let us = t.elapsed().as_micros();
            if us > 1000 {
                stalls += 1;
                eprintln!("  #{:>2}: {:>8}µs  STALL", i, us);
            }
        }
        eprintln!("  Stalls: {}/30\n", stalls);
        stalls
    });

    // === tokio h2 crate for comparison ===
    eprintln!("=== tokio h2 crate (separate thread) ===");
    let (addr_tx2, addr_rx2) = std::sync::mpsc::channel();
    let tokio_rt = tokio::runtime::Runtime::new().unwrap();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            addr_tx2.send(addr).unwrap();
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let _ = stream.set_nodelay(true);
                tokio::spawn(async move {
                    let Ok(mut conn) = h2::server::Builder::new()
                        .initial_window_size(1 << 20)
                        .initial_connection_window_size(1 << 20)
                        .handshake(stream)
                        .await
                    else {
                        return;
                    };
                    while let Some(Ok((request, mut respond))) = conn.accept().await {
                        tokio::spawn(async move {
                            let mut body = request.into_body();
                            let mut data = Vec::new();
                            while let Some(Ok(chunk)) = body.data().await {
                                let _ = body.flow_control().release_capacity(chunk.len());
                                data.extend_from_slice(&chunk);
                            }
                            let response =
                                http::Response::builder().status(200).body(()).unwrap();
                            let Ok(mut send_stream) = respond.send_response(response, false)
                            else {
                                return;
                            };
                            send_stream
                                .send_data(bytes::Bytes::from(data), true)
                                .unwrap();
                        });
                    }
                });
            }
        });
    });
    let tokio_addr = addr_rx2.recv().unwrap();

    let stalls_tokio = tokio_rt.block_on(async {
        let stream = tokio::net::TcpStream::connect(tokio_addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut send_req, conn) = h2::client::Builder::new()
            .initial_window_size(1 << 20)
            .initial_connection_window_size(1 << 20)
            .handshake(stream)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // Warm
        for _ in 0..5 {
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("http://localhost/bench")
                .body(())
                .unwrap();
            let (resp_fut, mut ss) = send_req.send_request(request, false).unwrap();
            ss.send_data(make_payload(100), true).unwrap();
            let resp = resp_fut.await.unwrap();
            let mut recv = resp.into_body();
            while let Some(Ok(chunk)) = recv.data().await {
                let _ = recv.flow_control().release_capacity(chunk.len());
            }
        }

        let payload = make_payload(65536);
        let mut stalls = 0;
        for i in 0..30 {
            let t = Instant::now();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri("http://localhost/bench")
                .body(())
                .unwrap();
            let (resp_fut, mut ss) = send_req.send_request(request, false).unwrap();
            ss.send_data(payload.clone(), true).unwrap();
            let resp = resp_fut.await.unwrap();
            let mut recv = resp.into_body();
            while let Some(Ok(chunk)) = recv.data().await {
                let _ = recv.flow_control().release_capacity(chunk.len());
            }
            let us = t.elapsed().as_micros();
            if us > 1000 {
                stalls += 1;
                eprintln!("  #{:>2}: {:>8}µs  STALL", i, us);
            }
        }
        eprintln!("  Stalls: {}/30\n", stalls);
        stalls
    });

    eprintln!("=== Summary ===");
    eprintln!("  Single-threaded stalls: {}/30", stalls_single);
    eprintln!("  Multi-threaded stalls:  {}/30", stalls_multi);
    eprintln!("  tokio h2 stalls:        {}/30", stalls_tokio);
}
