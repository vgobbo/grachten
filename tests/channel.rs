use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use grachten::{Error, bounded};

fn block_on<F: std::future::Future>(future: F) -> F::Output {
	tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.expect("failed to build runtime")
		.block_on(future)
}

#[test]
fn test_basic_send_and_recv() {
	block_on(async {
		let (tx, mut rx) = bounded(2);
		tx.send(42).await.unwrap();
		assert_eq!(rx.recv().await, Some(42));
	});
}

#[test]
fn test_fifo_ordering() {
	block_on(async {
		let (tx, mut rx) = bounded(16);
		for i in 0..100 {
			tx.send(i).await.unwrap();
			assert_eq!(rx.recv().await, Some(i));
		}
	});
}

#[test]
fn test_backpressure_bounded_capacity() {
	let (tx, mut rx) = bounded(2);

	let done = Arc::new(AtomicBool::new(false));
	let done_clone = done.clone();

	let handle = std::thread::spawn(move || {
		block_on(async {
			// Capacity is 2. Sizing is next_power_of_two(2) = 2.
			tx.send(1).await.unwrap();
			tx.send(2).await.unwrap();

			// Third send must block until receiver drains
			tx.send(3).await.unwrap();
			done_clone.store(true, Ordering::SeqCst);
		});
	});

	// Give the sender thread time to fill the buffer and block on the 3rd send
	std::thread::sleep(Duration::from_millis(50));
	assert!(!done.load(Ordering::SeqCst), "send should be blocked by backpressure");

	block_on(async {
		assert_eq!(rx.recv().await, Some(1));
	});

	// Now sender should be unblocked
	handle.join().unwrap();
	assert!(done.load(Ordering::SeqCst));

	block_on(async {
		assert_eq!(rx.recv().await, Some(2));
		assert_eq!(rx.recv().await, Some(3));
	});
}

#[test]
fn test_disconnect_empty_channel() {
	block_on(async {
		let (tx, mut rx) = bounded::<i32>(4);
		drop(tx);
		assert_eq!(rx.recv().await, None);
	});
}

#[test]
fn test_disconnect_drain_buffered_messages() {
	block_on(async {
		let (tx, mut rx) = bounded(4);
		tx.send("alpha").await.unwrap();
		tx.send("beta").await.unwrap();
		tx.send("gamma").await.unwrap();
		drop(tx);

		assert_eq!(rx.recv().await, Some("alpha"));
		assert_eq!(rx.recv().await, Some("beta"));
		assert_eq!(rx.recv().await, Some("gamma"));
		assert_eq!(rx.recv().await, None);
	});
}

#[test]
fn test_receiver_drop_causes_send_error() {
	block_on(async {
		let (tx, rx) = bounded::<i32>(4);
		drop(rx);
		assert!(matches!(tx.send(1).await, Err(Error::Closed)));
	});
}

#[test]
fn test_receiver_drop_unblocks_pending_sender() {
	let (tx, rx) = bounded::<i32>(1);

	let send_result = Arc::new(std::sync::Mutex::new(None));
	let send_result_clone = send_result.clone();

	let handle = std::thread::spawn(move || {
		block_on(async {
			tx.send(1).await.unwrap();
			// This send will block on capacity
			let res = tx.send(2).await;
			*send_result_clone.lock().unwrap() = Some(res);
		});
	});

	std::thread::sleep(Duration::from_millis(50));

	// Drop receiver while sender is waiting
	drop(rx);

	handle.join().unwrap();
	let res = send_result.lock().unwrap().take().unwrap();
	assert!(matches!(res, Err(Error::Closed)));
}

#[test]
fn test_multiple_producers_multi_threaded_stress() {
	const NUM_PRODUCERS: usize = 8;
	const MSGS_PER_PRODUCER: usize = 500;
	const CAPACITY: usize = 16;

	let (tx, mut rx) = bounded::<(usize, usize)>(CAPACITY);
	let mut threads = Vec::new();

	for producer_id in 0..NUM_PRODUCERS {
		let tx_clone = tx.clone();
		threads.push(std::thread::spawn(move || {
			block_on(async {
				for msg_id in 0..MSGS_PER_PRODUCER {
					tx_clone.send((producer_id, msg_id)).await.unwrap();
				}
			});
		}));
	}

	drop(tx); // Drop original sender handle

	let mut counts = [0; NUM_PRODUCERS];
	let mut last_seen = [0; NUM_PRODUCERS];

	block_on(async {
		while let Some((producer_id, msg_id)) = rx.recv().await {
			assert_eq!(msg_id, last_seen[producer_id], "per-producer messages must preserve FIFO");
			last_seen[producer_id] += 1;
			counts[producer_id] += 1;
		}
	});

	for t in threads {
		t.join().unwrap();
	}

	for count in counts {
		assert_eq!(count, MSGS_PER_PRODUCER);
	}
}

#[test]
fn test_no_leaks_on_channel_drop_with_buffered_items() {
	struct DropTracker {
		_val: usize,
		active: Arc<AtomicUsize>,
	}

	impl Drop for DropTracker {
		fn drop(&mut self) {
			self.active.fetch_sub(1, Ordering::SeqCst);
		}
	}

	let active = Arc::new(AtomicUsize::new(0));

	{
		let (tx, mut rx) = bounded(8);

		block_on(async {
			for i in 0..6 {
				active.fetch_add(1, Ordering::SeqCst);
				tx.send(DropTracker { _val: i, active: active.clone() }).await.unwrap();
			}

			// Consume 2 items
			let _ = rx.recv().await.unwrap();
			let _ = rx.recv().await.unwrap();
		});

		// At this point, 4 items remain buffered
		assert_eq!(active.load(Ordering::SeqCst), 4);
		// rx and tx drop here at end of scope
	}

	// Shared::drop should have reclaimed all 4 buffered items
	assert_eq!(active.load(Ordering::SeqCst), 0, "all buffered items must be dropped when channel is destroyed");
}

#[test]
fn test_send_non_sync_type() {
	// Cell<i32> is Send but !Sync
	block_on(async {
		let (tx, mut rx) = bounded(4);
		let cell = Cell::new(99);
		tx.send(cell).await.unwrap();

		let received = rx.recv().await.unwrap();
		assert_eq!(received.get(), 99);
	});
}

#[test]
fn test_interleaved_producers_relative_fifo() {
	let (tx, mut rx) = bounded(4);

	let tx1 = tx.clone();
	let tx2 = tx;

	let barrier = Arc::new(std::sync::Barrier::new(3));
	let b1 = barrier.clone();
	let b2 = barrier.clone();

	let h1 = std::thread::spawn(move || {
		block_on(async {
			b1.wait();
			for i in 0..50 {
				tx1.send((1, i)).await.unwrap();
			}
		});
	});

	let h2 = std::thread::spawn(move || {
		block_on(async {
			b2.wait();
			for i in 0..50 {
				tx2.send((2, i)).await.unwrap();
			}
		});
	});

	barrier.wait();

	let mut last_1 = 0;
	let mut count_1 = 0;
	let mut last_2 = 0;
	let mut count_2 = 0;

	block_on(async {
		for _ in 0..100 {
			let (sender, seq) = rx.recv().await.unwrap();
			if sender == 1 {
				assert_eq!(seq, last_1);
				last_1 += 1;
				count_1 += 1;
			} else {
				assert_eq!(seq, last_2);
				last_2 += 1;
				count_2 += 1;
			}
		}
		assert_eq!(count_1, 50);
		assert_eq!(count_2, 50);
	});

	h1.join().unwrap();
	h2.join().unwrap();
}

#[test]
fn test_capacity_one_alternating() {
	block_on(async {
		let (tx, mut rx) = bounded(1);

		for i in 0..20 {
			tx.send(i).await.unwrap();
			assert_eq!(rx.recv().await, Some(i));
		}
	});
}

#[test]
fn test_cancellation_safety_on_recv() {
	block_on(async {
		let (tx, mut rx) = bounded(4);

		// Attempt to receive with timeout on empty channel (will cancel future)
		let res = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await;
		assert!(res.is_err(), "recv should time out");

		// Send after cancellation
		tx.send(777).await.unwrap();

		// Next recv must receive 777 without skipping or corrupting state
		assert_eq!(rx.recv().await, Some(777));
	});
}
