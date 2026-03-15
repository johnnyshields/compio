//! Benchmark demonstrating the zerocopy send threshold fix.
//!
//! Measures write latency for small payloads (13B, like H2 WINDOW_UPDATE)
//! after large payloads (64KB, like H2 DATA frames). Without the threshold,
//! send_zerocopy's second CQE (buffer release) waits for the peer's TCP
//! delayed ACK (~40ms). With the threshold, small writes use regular send
//! (1 CQE) and complete in microseconds.
//!
//! Run: cargo run -p compio-net --example write_threshold_bench --release

use std::time::Instant;

use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncReadExt, AsyncWriteExt, util::Splittable};
use compio_net::TcpListener;

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // === Setup: echo server ===
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        compio_runtime::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            let (mut rh, mut wh) = stream.split();
            loop {
                let buf: Vec<u8> = Vec::with_capacity(65536);
                let BufResult(res, buf) = AsyncRead::read(&mut rh, buf).await;
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let BufResult(r, _) = wh.write_all(buf).await;
                        if r.is_err() {
                            break;
                        }
                    }
                }
            }
        })
        .detach();

        let stream = compio_net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut rh, mut wh) = stream.split();

        // Warm up
        for _ in 0..50 {
            let BufResult(r, _) = wh.write_all(vec![0u8; 100]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(100)).await;
            r.unwrap();
        }

        let iters = 200u128;

        // === Benchmark 1: Standalone 13B write (WINDOW_UPDATE size) ===
        let t = Instant::now();
        for _ in 0..iters {
            let BufResult(r, _) = wh.write_all(vec![0u8; 13]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(13)).await;
            r.unwrap();
        }
        eprintln!(
            "13B roundtrip:             {:>6}µs/iter",
            t.elapsed().as_micros() / iters
        );

        // === Benchmark 2: 64KB then 13B (the stall pattern) ===
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..100 {
            // Send 64KB + read echo
            let BufResult(r, _) = wh.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(65536)).await;
            r.unwrap();
            // Now the small write that used to stall
            let t = Instant::now();
            let BufResult(r, _) = wh.write_all(vec![0u8; 13]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(13)).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!(
            "13B after 64KB roundtrip:  median={:>6}µs  p99={:>6}µs  max={:>6}µs",
            times[50], times[99], times[99]
        );
        if times[99] > 1000 {
            eprintln!(
                "  WARNING: p99 > 1ms — zerocopy threshold may not be working"
            );
        }

        // === Benchmark 3: 64KB write (should still use zerocopy) ===
        let t = Instant::now();
        for _ in 0..iters {
            let BufResult(r, _) = wh.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(65536)).await;
            r.unwrap();
        }
        eprintln!(
            "64KB roundtrip:            {:>6}µs/iter",
            t.elapsed().as_micros() / iters
        );

        // === Benchmark 4: 8KB write (at threshold, uses zerocopy) ===
        let t = Instant::now();
        for _ in 0..iters {
            let BufResult(r, _) = wh.write_all(vec![0u8; 8192]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(8192)).await;
            r.unwrap();
        }
        eprintln!(
            "8KB roundtrip (at thresh): {:>6}µs/iter",
            t.elapsed().as_micros() / iters
        );

        // === Benchmark 5: 8KB-1 write (below threshold, regular send) ===
        let t = Instant::now();
        for _ in 0..iters {
            let BufResult(r, _) = wh.write_all(vec![0u8; 8191]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(Vec::with_capacity(8191)).await;
            r.unwrap();
        }
        eprintln!(
            "8KB-1 roundtrip (below):   {:>6}µs/iter",
            t.elapsed().as_micros() / iters
        );
    });
}
