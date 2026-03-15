//! Diagnostic: where does the 11ms 64KB H2 roundtrip latency come from?
//!
//! Run: cargo run -p compio-h2 --example read_diag --release

#[path = "../benches/support.rs"]
mod support;

use std::time::Instant;

use support::{compio_h2_client_connect, compio_h2_roundtrip, compio_h2_server_loop, make_payload};

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let listener = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        compio_runtime::spawn(compio_h2_server_loop(listener)).detach();
        let mut send_req = compio_h2_client_connect(addr).await;

        // Warm up
        for _ in 0..10 {
            compio_h2_roundtrip(&mut send_req, make_payload(100)).await;
        }

        // Profile individual roundtrips at different sizes
        for &size in &[100, 1024, 16384, 16385, 32768, 65536] {
            let payload = make_payload(size);
            let iters = if size <= 1024 { 200 } else { 50 };

            // Per-iteration timing to find outliers
            let mut times: Vec<u128> = Vec::with_capacity(iters);
            for _ in 0..iters {
                let t = Instant::now();
                compio_h2_roundtrip(&mut send_req, payload.clone()).await;
                times.push(t.elapsed().as_micros());
            }

            times.sort();
            let median = times[times.len() / 2];
            let p99 = times[(times.len() as f64 * 0.99) as usize];
            let min = times[0];
            let max = *times.last().unwrap();
            let mean = times.iter().sum::<u128>() / iters as u128;

            eprintln!(
                "{:>6}B: mean={:>7}µs  median={:>7}µs  min={:>7}µs  max={:>7}µs  p99={:>7}µs",
                size, mean, median, min, max, p99
            );
        }
    });
}
