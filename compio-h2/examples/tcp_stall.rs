//! Reproduce the 43ms stall at the raw TCP level.
//! Tests: does write(13B) then write(64KB) on the same socket stall?
//!
//! Run: cargo run -p compio-h2 --example tcp_stall --release

use std::time::Instant;

use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use compio_net::TcpListener;

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Echo server on separate thread
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
                        if r.is_err() { break; }
                    }
                }
            }
        }).detach();

        let stream = compio_net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (mut rh, mut wh) = stream.split();

        // Warm up
        for _ in 0..20 {
            let BufResult(r, _) = wh.write_all(vec![0u8; 100]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 100]).await;
            r.unwrap();
        }

        eprintln!("=== Test 1: single write(64KB) roundtrip ===");
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..30 {
            let t = Instant::now();
            let BufResult(r, _) = wh.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 65536]).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);

        // Warm up again
        for _ in 0..20 {
            let BufResult(r, _) = wh.write_all(vec![0u8; 100]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 100]).await;
            r.unwrap();
        }

        eprintln!("\n=== Test 2: write(13B) then write(64KB) as TWO writes, roundtrip ===");
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..30 {
            let t = Instant::now();
            let BufResult(r, _) = wh.write_all(vec![0u8; 13]).await;
            r.unwrap();
            let BufResult(r, _) = wh.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 13 + 65536]).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);

        eprintln!("\n=== Test 3: write(13B+64KB) as ONE write, roundtrip ===");
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..30 {
            let t = Instant::now();
            let BufResult(r, _) = wh.write_all(vec![0u8; 13 + 65536]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 13 + 65536]).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);

        eprintln!("\n=== Test 4: write(13B) with OwnedWriteHalf (Splittable::split) ===");
        {
            use compio_io::util::Splittable;
            let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr2 = listener2.local_addr().unwrap();
            compio_runtime::spawn(async move {
                let (stream, _) = listener2.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let (mut rh, mut wh) = stream.split();
                loop {
                    let buf: Vec<u8> = Vec::with_capacity(65536);
                    let BufResult(res, buf) = AsyncRead::read(&mut rh, buf).await;
                    match res {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let BufResult(r, _) = wh.write_all(buf).await;
                            if r.is_err() { break; }
                        }
                    }
                }
            }).detach();

            let stream2 = compio_net::TcpStream::connect(addr2).await.unwrap();
            stream2.set_nodelay(true).unwrap();
            let (mut rh2, mut wh2) = stream2.split();

            // Warm
            for _ in 0..20 {
                let BufResult(r, _) = wh2.write_all(vec![0u8; 100]).await;
                r.unwrap();
                let BufResult(r, _) = rh2.read_exact(vec![0u8; 100]).await;
                r.unwrap();
            }

            // Echo 64KB, then standalone 13B write
            let mut times: Vec<u128> = Vec::new();
            for _ in 0..30 {
                // Send 64KB, read echo
                let BufResult(r, _) = wh2.write_all(vec![0u8; 65536]).await;
                r.unwrap();
                let BufResult(r, _) = rh2.read_exact(vec![0u8; 65536]).await;
                r.unwrap();
                // Now write 13B standalone (like WINDOW_UPDATE)
                let t = Instant::now();
                let BufResult(r, _) = wh2.write_all(vec![0u8; 13]).await;
                r.unwrap();
                let BufResult(r, _) = rh2.read_exact(vec![0u8; 13]).await;
                r.unwrap();
                times.push(t.elapsed().as_micros());
            }
            times.sort();
            eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);
        }

        eprintln!("\n=== Test 5: H2-like - concurrent reader, then write(13B) ===");
        // This simulates the H2 pattern more exactly: reader task has a
        // pending read while writer sends 13B.
        {
            use compio_io::util::Splittable;
            let listener3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr3 = listener3.local_addr().unwrap();
            compio_runtime::spawn(async move {
                let (stream, _) = listener3.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let (mut rh, mut wh) = stream.split();
                loop {
                    let buf: Vec<u8> = Vec::with_capacity(65536);
                    let BufResult(res, buf) = AsyncRead::read(&mut rh, buf).await;
                    match res {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let BufResult(r, _) = wh.write_all(buf).await;
                            if r.is_err() { break; }
                        }
                    }
                }
            }).detach();

            let stream3 = compio_net::TcpStream::connect(addr3).await.unwrap();
            stream3.set_nodelay(true).unwrap();
            let (mut rh3, mut wh3) = stream3.split();

            // Warm
            for _ in 0..20 {
                let BufResult(r, _) = wh3.write_all(vec![0u8; 100]).await;
                r.unwrap();
                let BufResult(r, _) = rh3.read_exact(vec![0u8; 100]).await;
                r.unwrap();
            }

            // Spawn a reader that has a PENDING read (like H2 reader task)
            let read_flag = std::rc::Rc::new(std::cell::Cell::new(false));
            let rf = read_flag.clone();
            let reader_task = compio_runtime::spawn(async move {
                loop {
                    let buf: Vec<u8> = Vec::with_capacity(65536);
                    let BufResult(res, _) = rh3.read(buf).await;
                    match res {
                        Ok(0) | Err(_) => break,
                        Ok(_) => { rf.set(true); }
                    }
                }
            });

            // Write 13B while reader has pending read
            let mut times: Vec<u128> = Vec::new();
            for _ in 0..30 {
                let t = Instant::now();
                let BufResult(r, _) = wh3.write_all(vec![0u8; 13]).await;
                r.unwrap();
                // Wait for echo
                while !read_flag.get() {
                    compio_runtime::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                read_flag.set(false);
                times.push(t.elapsed().as_micros());
            }
            times.sort();
            eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);
            drop(reader_task);
        }

        eprintln!("\n=== Test 6: H2-like pattern - echo 64KB, THEN write(13B), repeat ===");
        // This simulates: server echoes response, then sends WINDOW_UPDATE
        let mut times: Vec<u128> = Vec::new();
        for _ in 0..30 {
            let t = Instant::now();
            // Client sends 64KB
            let BufResult(r, _) = wh.write_all(vec![0u8; 65536]).await;
            r.unwrap();
            // Server echoes 64KB (we read it)
            let BufResult(r, _) = rh.read_exact(vec![0u8; 65536]).await;
            r.unwrap();
            // Now server would send 13B WINDOW_UPDATE — simulate by
            // sending 13B from client and reading echo
            let BufResult(r, _) = wh.write_all(vec![0u8; 13]).await;
            r.unwrap();
            let BufResult(r, _) = rh.read_exact(vec![0u8; 13]).await;
            r.unwrap();
            times.push(t.elapsed().as_micros());
        }
        times.sort();
        eprintln!("  median={:>6}µs  p99={:>6}µs  max={:>6}µs", times[15], times[29], times[29]);
    });
}
