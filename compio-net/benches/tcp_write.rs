//! Benchmark for TcpStream write paths — validates the zerocopy threshold.
//!
//! Small writes (< 8KB) use regular send (1 CQE) to avoid the ~40ms
//! delayed-ACK penalty from send_zerocopy's buffer-release CQE.
//! Large writes (>= 8KB) use zerocopy send for kernel copy savings.
//!
//! Run: cargo bench -p compio-net --bench tcp_write

use std::time::Instant;

use compio_buf::BufResult;
use compio_io::{AsyncRead, AsyncReadExt, AsyncWriteExt, util::Splittable};
use compio_net::{TcpListener, TcpStream};
use criterion::{BenchmarkId, Criterion, Throughput};

struct EchoPair {
    rh: compio_net::OwnedReadHalf<TcpStream>,
    wh: compio_net::OwnedWriteHalf<TcpStream>,
}

fn setup_echo_pair(rt: &compio_runtime::Runtime) -> EchoPair {
    rt.block_on(async {
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

        let stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (rh, wh) = stream.split();

        EchoPair { rh, wh }
    })
}

fn bench_write_roundtrip(c: &mut Criterion, rt: &compio_runtime::Runtime) {
    let mut group = c.benchmark_group("tcp_write_roundtrip");

    let mut pair = setup_echo_pair(rt);

    // Warm up
    rt.block_on(async {
        for _ in 0..50 {
            let BufResult(r, _) = pair.wh.write_all(vec![0u8; 100]).await;
            r.unwrap();
            let BufResult(r, _) = pair.rh.read_exact(Vec::with_capacity(100)).await;
            r.unwrap();
        }
    });

    for &size in &[13, 100, 1024, 8191, 8192, 16384, 65536] {
        let label = match size {
            13 => "13B",
            100 => "100B",
            1024 => "1KB",
            8191 => "8KB-1",
            8192 => "8KB",
            16384 => "16KB",
            65536 => "64KB",
            _ => unreachable!(),
        };
        group.throughput(Throughput::Bytes((size * 2) as u64));
        group.bench_function(BenchmarkId::new("echo", label), |b| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let BufResult(r, _) = pair.wh.write_all(vec![0u8; size]).await;
                        r.unwrap();
                        let BufResult(r, _) =
                            pair.rh.read_exact(Vec::with_capacity(size)).await;
                        r.unwrap();
                    }
                    start.elapsed()
                })
            });
        });
    }

    group.finish();
}

fn bench_small_after_large(c: &mut Criterion, rt: &compio_runtime::Runtime) {
    let mut group = c.benchmark_group("tcp_write_small_after_large");

    let mut pair = setup_echo_pair(rt);

    // Warm up
    rt.block_on(async {
        for _ in 0..50 {
            let BufResult(r, _) = pair.wh.write_all(vec![0u8; 100]).await;
            r.unwrap();
            let BufResult(r, _) = pair.rh.read_exact(Vec::with_capacity(100)).await;
            r.unwrap();
        }
    });

    // The pattern that triggered the 40ms stall: large write then small write.
    // Without the zerocopy threshold fix, the 13B write stalls ~40ms waiting
    // for the delayed-ACK CQE. With the fix, it completes in microseconds.
    group.bench_function("64KB_then_13B", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let BufResult(r, _) = pair.wh.write_all(vec![0u8; 65536]).await;
                    r.unwrap();
                    let BufResult(r, _) =
                        pair.rh.read_exact(Vec::with_capacity(65536)).await;
                    r.unwrap();
                    let BufResult(r, _) = pair.wh.write_all(vec![0u8; 13]).await;
                    r.unwrap();
                    let BufResult(r, _) = pair.rh.read_exact(Vec::with_capacity(13)).await;
                    r.unwrap();
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

fn main() {
    let rt = compio_runtime::Runtime::new().unwrap();
    let mut c = Criterion::default().configure_from_args();

    bench_write_roundtrip(&mut c, &rt);
    bench_small_after_large(&mut c, &rt);
}
