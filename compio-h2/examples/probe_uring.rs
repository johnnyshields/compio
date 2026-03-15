//! Probe io_uring op support and benchmark write paths.
//!
//! Run: cargo run -p compio-h2 --example probe_uring --release

use std::time::Instant;

use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncWriteExt};
use compio_net::TcpListener;

async fn make_drain_pair() -> compio_net::TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    compio_runtime::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut rh, _wh) = stream.split();
        loop {
            let buf: Vec<u8> = Vec::with_capacity(65536);
            let BufResult(res, _) = AsyncRead::read(&mut rh, buf).await;
            match res {
                Ok(0) | Err(_) => break,
                _ => {}
            }
        }
    })
    .detach();
    compio_net::TcpStream::connect(addr).await.unwrap()
}

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let iters: u128 = 500;

        // === write_all (uses send_zerocopy → 2 CQEs) ===
        eprintln!("=== write_all (send_zerocopy, 2 CQEs) ===");
        let stream = make_drain_pair().await;
        stream.set_nodelay(true).unwrap();
        let mut s = &stream;

        for _ in 0..50 {
            let BufResult(r, _) = s.write_all(vec![0u8; 13]).await;
            r.unwrap();
        }

        let t = Instant::now();
        for _ in 0..iters {
            let BufResult(r, _) = s.write_all(vec![0u8; 13]).await;
            r.unwrap();
        }
        eprintln!("  13B standalone:          {:>6}µs/iter", t.elapsed().as_micros() / iters);

        let mut times: Vec<u128> = Vec::new();
        for _ in 0..200 {
            let BufResult(r, _) = s.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            let t = Instant::now();
            let BufResult(r, _) = s.write_all(vec![0u8; 13]).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!(
            "  64KB then 13B:           median={:>6}µs  p90={:>6}µs  p99={:>6}µs  max={:>6}µs",
            times[100], times[180], times[198], times[199]
        );

        // === send_zerocopy directly, measuring each CQE ===
        eprintln!("\n=== send_zerocopy timing breakdown ===");
        let stream2 = make_drain_pair().await;
        stream2.set_nodelay(true).unwrap();

        for _ in 0..50 {
            let BufResult(r, fut) = stream2.send_zerocopy(vec![0u8; 13], 0).await;
            r.unwrap();
            fut.await;
        }

        // Measure CQE1 vs CQE2 timing
        let mut cqe1_times: Vec<u128> = Vec::new();
        let mut cqe2_times: Vec<u128> = Vec::new();
        for _ in 0..200 {
            let BufResult(r, fut) = stream2.send_zerocopy(vec![0u8; 65536], 0).await;
            r.unwrap();
            fut.await;

            let t0 = Instant::now();
            let BufResult(r, fut) = stream2.send_zerocopy(vec![0u8; 13], 0).await;
            let t1 = Instant::now();
            r.unwrap();
            fut.await;
            let t2 = Instant::now();

            cqe1_times.push((t1 - t0).as_micros());
            cqe2_times.push((t2 - t1).as_micros());
        }
        cqe1_times.sort();
        cqe2_times.sort();
        eprintln!(
            "  CQE1 (send done):        median={:>6}µs  p90={:>6}µs  p99={:>6}µs  max={:>6}µs",
            cqe1_times[100], cqe1_times[180], cqe1_times[198], cqe1_times[199]
        );
        eprintln!(
            "  CQE2 (buf release):      median={:>6}µs  p90={:>6}µs  p99={:>6}µs  max={:>6}µs",
            cqe2_times[100], cqe2_times[180], cqe2_times[198], cqe2_times[199]
        );

        // === blocking write syscall (bypass io_uring entirely) ===
        eprintln!("\n=== blocking write(2) (no io_uring) ===");
        // Use a separate std TCP pair so io_uring is not involved at all
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let std_addr = std_listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut conn, _) = std_listener.accept().unwrap();
            let mut buf = [0u8; 65536];
            loop {
                match std::io::Read::read(&mut conn, &mut buf) {
                    Ok(0) | Err(_) => break,
                    _ => {}
                }
            }
        });
        let mut std_stream = std::net::TcpStream::connect(std_addr).unwrap();
        std_stream.set_nodelay(true).unwrap();
        use std::io::Write;

        // Warm
        for _ in 0..50 {
            std_stream.write_all(&[0u8; 13]).unwrap();
        }

        let t = Instant::now();
        for _ in 0..iters {
            std_stream.write_all(&[0u8; 13]).unwrap();
        }
        eprintln!("  13B standalone:          {:>6}µs/iter", t.elapsed().as_micros() / iters);

        let mut times: Vec<u128> = Vec::new();
        let big = vec![0u8; 65536];
        for _ in 0..200 {
            std_stream.write_all(&big).unwrap();
            let t = Instant::now();
            std_stream.write_all(&[0u8; 13]).unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!(
            "  64KB then 13B:           median={:>6}µs  p90={:>6}µs  p99={:>6}µs  max={:>6}µs",
            times[100], times[180], times[198], times[199]
        );
    });
}
