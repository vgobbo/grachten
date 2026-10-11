use std::time::{Duration, Instant};

const SAMPLES: usize = 5;

struct BenchResult {
	name: &'static str,
	grachten_time: Duration,
	tokio_time: Duration,
	grachten_msgs_per_sec: f64,
	tokio_msgs_per_sec: f64,
}

fn median_duration(mut times: Vec<Duration>) -> Duration {
	times.sort();
	times[times.len() / 2]
}

async fn bench_grachten_spsc(capacity: usize, total_msgs: usize) -> Duration {
	let start = Instant::now();
	let (tx, mut rx) = grachten::bounded(capacity);

	let producer = tokio::spawn(async move {
		for i in 0..total_msgs {
			tx.send(i).await.unwrap();
		}
	});

	let consumer = tokio::spawn(async move {
		let mut count = 0;
		while rx.recv().await.is_some() {
			count += 1;
			if count == total_msgs {
				break;
			}
		}
	});

	producer.await.unwrap();
	consumer.await.unwrap();
	start.elapsed()
}

async fn bench_tokio_spsc(capacity: usize, total_msgs: usize) -> Duration {
	let start = Instant::now();
	let (tx, mut rx) = tokio::sync::mpsc::channel(capacity);

	let producer = tokio::spawn(async move {
		for i in 0..total_msgs {
			tx.send(i).await.unwrap();
		}
	});

	let consumer = tokio::spawn(async move {
		let mut count = 0;
		while rx.recv().await.is_some() {
			count += 1;
			if count == total_msgs {
				break;
			}
		}
	});

	producer.await.unwrap();
	consumer.await.unwrap();
	start.elapsed()
}

async fn bench_grachten_mpsc(num_producers: usize, capacity: usize, total_msgs: usize) -> Duration {
	let msgs_per_producer = total_msgs / num_producers;
	let start = Instant::now();
	let (tx, mut rx) = grachten::bounded(capacity);

	let mut producers = Vec::with_capacity(num_producers);
	for _ in 0..num_producers {
		let tx = tx.clone();
		producers.push(tokio::spawn(async move {
			for i in 0..msgs_per_producer {
				tx.send(i).await.unwrap();
			}
		}));
	}
	drop(tx);

	let consumer = tokio::spawn(async move {
		let mut count = 0;
		while rx.recv().await.is_some() {
			count += 1;
		}
		count
	});

	for p in producers {
		p.await.unwrap();
	}
	let received = consumer.await.unwrap();
	assert_eq!(received, msgs_per_producer * num_producers);
	start.elapsed()
}

async fn bench_tokio_mpsc(num_producers: usize, capacity: usize, total_msgs: usize) -> Duration {
	let msgs_per_producer = total_msgs / num_producers;
	let start = Instant::now();
	let (tx, mut rx) = tokio::sync::mpsc::channel(capacity);

	let mut producers = Vec::with_capacity(num_producers);
	for _ in 0..num_producers {
		let tx = tx.clone();
		producers.push(tokio::spawn(async move {
			for i in 0..msgs_per_producer {
				tx.send(i).await.unwrap();
			}
		}));
	}
	drop(tx);

	let consumer = tokio::spawn(async move {
		let mut count = 0;
		while rx.recv().await.is_some() {
			count += 1;
		}
		count
	});

	for p in producers {
		p.await.unwrap();
	}
	let received = consumer.await.unwrap();
	assert_eq!(received, msgs_per_producer * num_producers);
	start.elapsed()
}

fn run_spsc_suite(rt: &tokio::runtime::Runtime, name: &'static str, capacity: usize, msgs: usize) -> BenchResult {
	// Warmup
	rt.block_on(async {
		bench_grachten_spsc(capacity, msgs / 10).await;
		bench_tokio_spsc(capacity, msgs / 10).await;
	});

	let mut grachten_times = Vec::with_capacity(SAMPLES);
	let mut tokio_times = Vec::with_capacity(SAMPLES);

	for _ in 0..SAMPLES {
		grachten_times.push(rt.block_on(bench_grachten_spsc(capacity, msgs)));
		tokio_times.push(rt.block_on(bench_tokio_spsc(capacity, msgs)));
	}

	let grachten_median = median_duration(grachten_times);
	let tokio_median = median_duration(tokio_times);

	BenchResult {
		name,
		grachten_time: grachten_median,
		tokio_time: tokio_median,
		grachten_msgs_per_sec: msgs as f64 / grachten_median.as_secs_f64(),
		tokio_msgs_per_sec: msgs as f64 / tokio_median.as_secs_f64(),
	}
}

fn run_mpsc_suite(
	rt: &tokio::runtime::Runtime,
	name: &'static str,
	producers: usize,
	capacity: usize,
	msgs: usize,
) -> BenchResult {
	// Warmup
	rt.block_on(async {
		bench_grachten_mpsc(producers, capacity, msgs / 10).await;
		bench_tokio_mpsc(producers, capacity, msgs / 10).await;
	});

	let mut grachten_times = Vec::with_capacity(SAMPLES);
	let mut tokio_times = Vec::with_capacity(SAMPLES);

	for _ in 0..SAMPLES {
		grachten_times.push(rt.block_on(bench_grachten_mpsc(producers, capacity, msgs)));
		tokio_times.push(rt.block_on(bench_tokio_mpsc(producers, capacity, msgs)));
	}

	let grachten_median = median_duration(grachten_times);
	let tokio_median = median_duration(tokio_times);

	BenchResult {
		name,
		grachten_time: grachten_median,
		tokio_time: tokio_median,
		grachten_msgs_per_sec: msgs as f64 / grachten_median.as_secs_f64(),
		tokio_msgs_per_sec: msgs as f64 / tokio_median.as_secs_f64(),
	}
}

fn print_results(results: &[BenchResult]) {
	println!("\n{:=^96}", " BENCHMARK RESULTS: Grachten vs Tokio MPSC ");
	println!(
		"{:<34} | {:>14} | {:>14} | {:>12} | {:>10}",
		"Scenario", "Grachten (time)", "Tokio (time)", "Grachten M/s", "Tokio M/s"
	);
	println!("{:-<96}", "");

	for r in results {
		let grachten_ms = r.grachten_time.as_secs_f64() * 1000.0;
		let tokio_ms = r.tokio_time.as_secs_f64() * 1000.0;
		let g_m_sec = r.grachten_msgs_per_sec / 1_000_000.0;
		let t_m_sec = r.tokio_msgs_per_sec / 1_000_000.0;

		println!(
			"{:<34} | {:>11.2} ms | {:>11.2} ms | {:>9.2} M/s | {:>7.2} M/s",
			r.name, grachten_ms, tokio_ms, g_m_sec, t_m_sec
		);
	}
	println!("{:=^96}\n", "");
}

fn main() {
	println!("Initializing multi-threaded Tokio runtime for benchmarking...");
	let rt = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.expect("failed to create Tokio multi-thread runtime");

	println!("Running benchmarks (taking median over {} samples)...", SAMPLES);

	let results = vec![
		// 1. SPSC - moderate capacity
		run_spsc_suite(&rt, "SPSC (cap 64, 500k msgs)", 64, 500_000),
		// 2. SPSC - large capacity
		run_spsc_suite(&rt, "SPSC (cap 1024, 500k msgs)", 1024, 500_000),
		// 3. Backpressure Ping-Pong (cap 1)
		run_spsc_suite(&rt, "Ping-Pong (cap 1, 100k msgs)", 1, 100_000),
		// 4. MPSC - 4 producers
		run_mpsc_suite(&rt, "MPSC 4 senders (cap 64, 500k msgs)", 4, 64, 500_000),
		// 5. MPSC - 8 producers
		run_mpsc_suite(&rt, "MPSC 8 senders (cap 128, 800k msgs)", 8, 128, 800_000),
		// 6. MPSC - 16 producers (high contention)
		run_mpsc_suite(&rt, "MPSC 16 senders (cap 256, 800k msgs)", 16, 256, 800_000),
	];

	print_results(&results);
}
