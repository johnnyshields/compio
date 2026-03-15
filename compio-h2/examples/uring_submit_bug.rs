//! Progressive isolation of the 43ms write_all stall.
//!
//! Tests increasingly H2-like scenarios to find the exact trigger.
//! Run: cargo run -p compio-h2 --example uring_submit_bug --release

use std::cell::RefCell;
use std::future::poll_fn;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Instant;

use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncReadExt, AsyncWriteExt, util::Splittable};

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ─── Test 1: Raw write 13B (baseline) ───
        let (mut rh, mut wh, _server) = make_echo_pair().await;
        // warm
        for _ in 0..5 { let _ = wh.write_all(vec![0u8; 13]).await; }
        let _ = rh.read(vec![0u8; 4096]).await;
        let t = Instant::now();
        for _ in 0..100 { let _ = wh.write_all(vec![0u8; 13]).await; }
        eprintln!("Test 1 - Raw write 13B:             {:>8}µs/iter", t.elapsed().as_micros() / 100);

        // ─── Test 2: Write 64KB then immediately write 13B ───
        let (mut rh, mut wh, _server) = make_echo_pair().await;
        let t = Instant::now();
        for _ in 0..20 {
            let _ = wh.write_all(vec![0u8; 65536]).await;
            let _ = wh.write_all(vec![0u8; 13]).await;
            let BufResult(_, _) = rh.read_exact(vec![0u8; 65536 + 13]).await;
        }
        eprintln!("Test 2 - 64KB then 13B (sequential): {:>8}µs/iter", t.elapsed().as_micros() / 20);

        // ─── Test 3: Spawned writer task with poll_fn waker pattern ───
        let (rh, wh, _server) = make_echo_pair().await;
        let state = Rc::new(RefCell::new(FlushState {
            buf: Vec::new(),
            poller: None,
        }));
        let ws = state.clone();
        let writer_task = compio_runtime::spawn(async move {
            let mut wh: WriteHalf = wh;
            loop {
                // poll_fn wait (like io_flush_loop)
                poll_fn(|cx| {
                    let mut s = ws.borrow_mut();
                    let ready = s.poller.is_none() || !s.buf.is_empty();
                    s.poller = Some(cx.waker().clone());
                    if ready { Poll::Ready(()) } else { Poll::Pending }
                }).await;
                let buf = {
                    let mut s = ws.borrow_mut();
                    if s.buf.is_empty() { continue; }
                    std::mem::take(&mut s.buf)
                };
                let BufResult(_, _) = wh.write_all(buf).await;
            }
        });
        // Spawn reader that drains
        let reader_task = compio_runtime::spawn(async move {
            let mut rh: ReadHalf = rh;
            loop {
                let BufResult(r, _) = rh.read(vec![0u8; 65536]).await;
                if matches!(r, Ok(0) | Err(_)) { break; }
            }
        });
        // Push data through the writer task
        let t = Instant::now();
        for _ in 0..100 {
            let mut s = state.borrow_mut();
            s.buf.extend_from_slice(&[0u8; 13]);
            if let Some(w) = s.poller.take() { w.wake(); }
        }
        compio_runtime::time::sleep(std::time::Duration::from_millis(200)).await;
        eprintln!("Test 3 - Spawned writer poll_fn 13B: {:>8}µs/iter", t.elapsed().as_micros() / 100);
        drop(writer_task); drop(reader_task);

        // ─── Test 4: Same as 3 but push 64KB then 13B ───
        let (rh, wh, _server) = make_echo_pair().await;
        let state = Rc::new(RefCell::new(FlushState {
            buf: Vec::new(),
            poller: None,
        }));
        let ws = state.clone();
        let flush_times = Rc::new(RefCell::new(Vec::new()));
        let ft = flush_times.clone();
        let writer_task = compio_runtime::spawn(async move {
            let mut wh: WriteHalf = wh;
            loop {
                poll_fn(|cx| {
                    let mut s = ws.borrow_mut();
                    let ready = s.poller.is_none() || !s.buf.is_empty();
                    s.poller = Some(cx.waker().clone());
                    if ready { Poll::Ready(()) } else { Poll::Pending }
                }).await;
                let buf = {
                    let mut s = ws.borrow_mut();
                    if s.buf.is_empty() { continue; }
                    std::mem::take(&mut s.buf)
                };
                let len = buf.len();
                let t = Instant::now();
                let BufResult(_, _) = wh.write_all(buf).await;
                ft.borrow_mut().push((len, t.elapsed()));
            }
        });
        let reader_task = compio_runtime::spawn(async move {
            let mut rh: ReadHalf = rh;
            loop {
                let BufResult(r, _) = rh.read(vec![0u8; 65536]).await;
                if matches!(r, Ok(0) | Err(_)) { break; }
            }
        });
        // Simulate H2: push 64KB, wake, then push 13B, wake
        for _ in 0..10 {
            {
                let mut s = state.borrow_mut();
                s.buf.extend_from_slice(&[0u8; 65536]);
                if let Some(w) = s.poller.take() { w.wake(); }
            }
            // Yield to let writer flush the 64KB
            compio_runtime::time::sleep(std::time::Duration::from_millis(1)).await;
            {
                let mut s = state.borrow_mut();
                s.buf.extend_from_slice(&[0u8; 13]);
                if let Some(w) = s.poller.take() { w.wake(); }
            }
            compio_runtime::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        compio_runtime::time::sleep(std::time::Duration::from_millis(100)).await;
        let times = flush_times.borrow();
        let slow: Vec<_> = times.iter().filter(|(_, d)| d.as_millis() > 1).collect();
        eprintln!("Test 4 - 64KB then 13B via poll_fn:");
        for (len, dur) in slow.iter() {
            eprintln!("  SLOW: write_all({len}B) = {dur:?}");
        }
        if slow.is_empty() { eprintln!("  No stalls"); }
        drop(writer_task); drop(reader_task);

        // ─── Test 5: Same but reader is on a SPLIT HALF with pending read ───
        let (rh, wh, _server) = make_echo_pair().await;
        let state = Rc::new(RefCell::new(FlushState {
            buf: Vec::new(),
            poller: None,
        }));
        let ws = state.clone();
        let flush_times = Rc::new(RefCell::new(Vec::new()));
        let ft = flush_times.clone();
        // Writer task on write half
        let writer_task = compio_runtime::spawn(async move {
            let mut wh: WriteHalf = wh;
            loop {
                poll_fn(|cx| {
                    let mut s = ws.borrow_mut();
                    let ready = s.poller.is_none() || !s.buf.is_empty();
                    s.poller = Some(cx.waker().clone());
                    if ready { Poll::Ready(()) } else { Poll::Pending }
                }).await;
                let buf = {
                    let mut s = ws.borrow_mut();
                    if s.buf.is_empty() { continue; }
                    std::mem::take(&mut s.buf)
                };
                let len = buf.len();
                let t = Instant::now();
                let BufResult(_, _) = wh.write_all(buf).await;
                ft.borrow_mut().push((len, t.elapsed()));
            }
        });
        // Reader on read half — always has pending read (like H2 reader)
        let reader_task = compio_runtime::spawn(async move {
            let mut rh: ReadHalf = rh;
            loop {
                let BufResult(r, _) = rh.read(vec![0u8; 65536]).await;
                if matches!(r, Ok(0) | Err(_)) { break; }
            }
        });
        for _ in 0..10 {
            {
                let mut s = state.borrow_mut();
                s.buf.extend_from_slice(&[0u8; 65536]);
                if let Some(w) = s.poller.take() { w.wake(); }
            }
            compio_runtime::time::sleep(std::time::Duration::from_millis(1)).await;
            {
                let mut s = state.borrow_mut();
                s.buf.extend_from_slice(&[0u8; 13]);
                if let Some(w) = s.poller.take() { w.wake(); }
            }
            compio_runtime::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        compio_runtime::time::sleep(std::time::Duration::from_millis(100)).await;
        let times = flush_times.borrow();
        let slow: Vec<_> = times.iter().filter(|(_, d)| d.as_millis() > 1).collect();
        eprintln!("Test 5 - Split half, 64KB then 13B:");
        for (len, dur) in slow.iter() {
            eprintln!("  SLOW: write_all({len}B) = {dur:?}");
        }
        if slow.is_empty() { eprintln!("  No stalls"); }
        drop(writer_task); drop(reader_task);

        // ─── Test 6: Reader WAKES the writer (the H2 pattern) ───
        let (rh, wh, _server) = make_echo_pair().await;
        let state = Rc::new(RefCell::new(FlushState {
            buf: Vec::new(),
            poller: None,
        }));
        let ws = state.clone();
        let flush_times = Rc::new(RefCell::new(Vec::new()));
        let ft = flush_times.clone();
        let writer_task = compio_runtime::spawn(async move {
            let mut wh: WriteHalf = wh;
            loop {
                poll_fn(|cx| {
                    let mut s = ws.borrow_mut();
                    let ready = s.poller.is_none() || !s.buf.is_empty();
                    s.poller = Some(cx.waker().clone());
                    if ready { Poll::Ready(()) } else { Poll::Pending }
                }).await;
                let buf = {
                    let mut s = ws.borrow_mut();
                    if s.buf.is_empty() { continue; }
                    std::mem::take(&mut s.buf)
                };
                let len = buf.len();
                let t = Instant::now();
                let BufResult(_, _) = wh.write_all(buf).await;
                ft.borrow_mut().push((len, t.elapsed()));
            }
        });
        // Reader that wakes the writer when data arrives (like H2 reader after handle_frame)
        let rs = state.clone();
        let reader_task = compio_runtime::spawn(async move {
            let mut rh: ReadHalf = rh;
            loop {
                let BufResult(r, _) = rh.read(vec![0u8; 65536]).await;
                match r {
                    Ok(n) if n > 0 => {
                        // Like H2: after reading echoed data, add 13B to write_buf and wake writer
                        let mut s = rs.borrow_mut();
                        s.buf.extend_from_slice(&[0u8; 13]);
                        if let Some(w) = s.poller.take() { w.wake(); }
                    }
                    _ => break,
                }
            }
        });
        // Send 64KB, let echo come back, reader adds 13B + wakes writer
        for _ in 0..10 {
            let mut s = state.borrow_mut();
            s.buf.extend_from_slice(&[0u8; 65536]);
            if let Some(w) = s.poller.take() { w.wake(); }
            drop(s);
            compio_runtime::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        compio_runtime::time::sleep(std::time::Duration::from_millis(200)).await;
        let times = flush_times.borrow();
        let slow: Vec<_> = times.iter().filter(|(_, d)| d.as_millis() > 1).collect();
        eprintln!("Test 6 - Reader wakes writer (H2 pattern):");
        for (len, dur) in slow.iter() {
            eprintln!("  SLOW: write_all({len}B) = {dur:?}");
        }
        if slow.is_empty() { eprintln!("  No stalls"); }
        drop(writer_task); drop(reader_task);

        // ─── Test 7: Like 6 but 64KB + 13B sent together (no separate wake) ───
        let (rh, wh, _server) = make_echo_pair().await;
        let state = Rc::new(RefCell::new(FlushState {
            buf: Vec::new(),
            poller: None,
        }));
        let ws = state.clone();
        let flush_times = Rc::new(RefCell::new(Vec::new()));
        let ft = flush_times.clone();
        let writer_task = compio_runtime::spawn(async move {
            let mut wh: WriteHalf = wh;
            loop {
                poll_fn(|cx| {
                    let mut s = ws.borrow_mut();
                    let ready = s.poller.is_none() || !s.buf.is_empty();
                    s.poller = Some(cx.waker().clone());
                    if ready { Poll::Ready(()) } else { Poll::Pending }
                }).await;
                let buf = {
                    let mut s = ws.borrow_mut();
                    if s.buf.is_empty() { continue; }
                    std::mem::take(&mut s.buf)
                };
                let len = buf.len();
                let t = Instant::now();
                let BufResult(_, _) = wh.write_all(buf).await;
                ft.borrow_mut().push((len, t.elapsed()));
            }
        });
        // Reader does NOT wake writer — just drains
        let reader_task = compio_runtime::spawn(async move {
            let mut rh: ReadHalf = rh;
            loop {
                let BufResult(r, _) = rh.read(vec![0u8; 65536]).await;
                if matches!(r, Ok(0) | Err(_)) { break; }
            }
        });
        // Send 64KB + 13B together (writer flushes both in one write_all)
        for _ in 0..10 {
            let mut s = state.borrow_mut();
            s.buf.extend_from_slice(&[0u8; 65536 + 13]);
            if let Some(w) = s.poller.take() { w.wake(); }
            drop(s);
            compio_runtime::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        compio_runtime::time::sleep(std::time::Duration::from_millis(200)).await;
        let times = flush_times.borrow();
        let slow: Vec<_> = times.iter().filter(|(_, d)| d.as_millis() > 1).collect();
        eprintln!("Test 7 - 64KB+13B in single write:");
        for (len, dur) in slow.iter() {
            eprintln!("  SLOW: write_all({len}B) = {dur:?}");
        }
        if slow.is_empty() { eprintln!("  No stalls"); }
        drop(writer_task); drop(reader_task);
    });
}

struct FlushState {
    buf: Vec<u8>,
    poller: Option<Waker>,
}

type ReadHalf = compio_net::OwnedReadHalf<compio_net::TcpStream>;
type WriteHalf = compio_net::OwnedWriteHalf<compio_net::TcpStream>;

/// Create a TCP echo pair: returns (client_read, client_write, server_task).
async fn make_echo_pair() -> (ReadHalf, WriteHalf, impl Drop) {
    let listener = compio_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = compio_runtime::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        loop {
            let BufResult(r, buf) = stream.read(vec![0u8; 65536]).await;
            match r { Ok(n) if n > 0 => { let _ = stream.write_all(buf[..n].to_vec()).await; }, _ => break }
        }
    });
    let stream = compio_net::TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (rh, wh) = stream.into_split();
    (rh, wh, server)
}
