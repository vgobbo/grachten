//! A multi-producer, single-consumer (MPSC) bounded channel implementation with an interface
//! similar to Tokio's MPSC channel.
//!
//! # Overview
//!
//! A channel is created using the [`bounded`] function, specifying a buffer capacity.
//! It returns a sending handle ([`Tx`]) and a receiving handle ([`Rx`]).
//!
//! The sender can be cloned to allow multiple tasks or threads to send values concurrently.
//! The receiver is strictly single-consumer and requires exclusive (`&mut self`) access to receive messages.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;

/// Errors that can occur when interacting with the channel.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Error {
	/// The channel is closed.
	///
	/// For senders, this indicates that the receiver ([`Rx`]) has been dropped.
	Closed,
}

/// A specialized [`Result`](std::result::Result) type for channel operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The sending half of a bounded MPSC channel.
///
/// Senders can be cloned to send messages to the same receiver from multiple asynchronous tasks or threads.
/// When all senders are dropped, the receiver will drain remaining messages and then observe channel closure.
pub struct Tx<T>
where
	T: Send,
{
	shared: Arc<Shared<T>>,
}

/// The receiving half of a bounded MPSC channel.
///
/// Only a single consumer is permitted. When all corresponding [`Tx`] senders have been dropped
/// and all buffered messages have been received, [`recv`](Rx::recv) returns `None`.
pub struct Rx<T>
where
	T: Send,
{
	shared: Arc<Shared<T>>,
}

struct Shared<T>
where
	T: Send,
{
	buffer: Vec<Entry<T>>,
	buffer_head: AtomicUsize,
	buffer_tail: AtomicUsize,
	tx_sem: Semaphore,
	rx_sem: Semaphore,
	tx_count: AtomicU32,
}

unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

#[derive(Default)]
struct Yielder {
	count: u32,
}

impl Yielder {
	pub async fn pass(&mut self) {
		match self.count {
			0..20 => tokio::task::yield_now().await,
			20..100 => tokio::time::sleep(Duration::from_millis(1)).await,
			100..200 => tokio::time::sleep(Duration::from_millis(2)).await,
			_ => tokio::time::sleep(Duration::from_millis(10)).await,
		}
		self.count += 1;
	}
}

type State = AtomicU8;
const STATE_FREE: u8 = 0;
const STATE_TAKEN: u8 = 1;
const STATE_ABORTED: u8 = 2;

struct Entry<T>
where
	T: Send,
{
	state: State,
	value: UnsafeCell<MaybeUninit<T>>,
}

struct EntryTxGuard<'a, T>
where
	T: Send,
{
	armed: bool,
	entry: &'a Entry<T>,
}

impl<'a, T> EntryTxGuard<'a, T>
where
	T: Send,
{
	pub fn new(entry: &'a Entry<T>) -> Self {
		Self { armed: true, entry }
	}

	pub fn disarm(&mut self) {
		self.armed = false;
	}
}

impl<T> Drop for EntryTxGuard<'_, T>
where
	T: Send,
{
	fn drop(&mut self) {
		if self.armed {
			self.entry.state.store(STATE_ABORTED, Ordering::Release);
		}
	}
}

impl<T> Tx<T>
where
	T: Send,
{
	/// Sends a value across the channel, waiting asynchronously if the channel is full.
	///
	/// If the receiver has been dropped, this method returns [`Err(Error::Closed)`](Error::Closed).
	pub async fn send(&self, datum: T) -> Result<()> {
		let permit = self.shared.tx_sem.acquire().await.map_err(|_| Error::Closed)?;

		let buffer_tail = self.shared.buffer_tail.fetch_add(1, Ordering::Relaxed);
		let index = buffer_tail & (self.shared.buffer.len() - 1);

		let entry = &self.shared.buffer[index];

		let mut guard = EntryTxGuard::new(entry);

		let ptr: *mut MaybeUninit<T> = entry.value.get();
		unsafe {
			*ptr = MaybeUninit::new(datum);
		}
		entry.state.store(STATE_TAKEN, Ordering::Release);

		self.shared.rx_sem.add_permits(1);

		guard.disarm();
		permit.forget();

		Ok(())
	}
}

impl<T> Clone for Tx<T>
where
	T: Send,
{
	fn clone(&self) -> Self {
		self.shared.tx_count.fetch_add(1, Ordering::Relaxed);
		Self { shared: self.shared.clone() }
	}
}

impl<T> Drop for Tx<T>
where
	T: Send,
{
	fn drop(&mut self) {
		if self.shared.tx_count.fetch_sub(1, Ordering::AcqRel) == 1 {
			self.shared.rx_sem.close();
		}
	}
}

impl<T> Rx<T>
where
	T: Send,
{
	/// Receives the next value for this receiver.
	///
	/// Returns:
	/// - `Some(datum)` if a message is available (waiting asynchronously if the buffer is currently empty).
	/// - `None` if the channel is closed and all messages sent prior to closure have been consumed.
	pub async fn recv(&mut self) -> Option<T> {
		let mut yielder = Yielder::default();

		while let Ok(permit) = self.shared.rx_sem.acquire().await {
			if let Some(datum) = self.take() {
				permit.forget();
				self.shared.tx_sem.add_permits(1);

				return Some(datum);
			} else {
				yielder.pass().await;
				continue;
			}
		}

		self.take()
	}

	#[inline]
	fn advance(&self) {
		self.shared.buffer_head.fetch_add(1, Ordering::Release);
	}

	#[inline]
	fn resolve(&self) -> Option<(usize, usize)> {
		loop {
			let head = self.shared.buffer_head.load(Ordering::Acquire);
			let tail = self.shared.buffer_tail.load(Ordering::Acquire);
			if head == tail {
				return None;
			}

			let index = head & (self.shared.buffer.len() - 1);

			let entry = &self.shared.buffer[index];
			let state = entry.state.load(Ordering::Acquire);
			if state == STATE_FREE {
				return None;
			}
			if state == STATE_ABORTED {
				entry.state.store(STATE_FREE, Ordering::Release);
				self.advance();
				continue;
			}

			return Some((head, index));
		}
	}

	fn take(&self) -> Option<T> {
		let (_, index) = self.resolve()?;
		let entry = &self.shared.buffer[index];

		self.advance();

		let ptr: *mut MaybeUninit<T> = entry.value.get();
		let datum = unsafe {
			let datum = (*ptr).assume_init_read();
			*ptr = MaybeUninit::uninit();

			datum
		};

		entry.state.store(STATE_FREE, Ordering::Release);

		Some(datum)
	}
}

impl<T> Drop for Rx<T>
where
	T: Send,
{
	fn drop(&mut self) {
		self.shared.tx_sem.close();
	}
}

impl<T> Drop for Shared<T>
where
	T: Send,
{
	fn drop(&mut self) {
		for entry in &self.buffer {
			if entry.state.load(Ordering::Relaxed) == STATE_TAKEN {
				let ptr: *mut MaybeUninit<T> = entry.value.get();
				unsafe {
					(*ptr).assume_init_drop();
				}
			}
		}
	}
}

/// Creates a bounded MPSC channel with capacity for at least `n` messages.
///
/// Senders can be cloned and shared across tasks. If the channel is full, calls to [`Tx::send`]
/// will asynchronously wait until capacity becomes available.
///
/// # Panics
///
/// Panics if `n == 0`.
pub fn bounded<T>(n: usize) -> (Tx<T>, Rx<T>)
where
	T: Send,
{
	assert!(n > 0);

	let buffer_capacity = n.next_power_of_two();

	let buffer: Vec<_> = (0..buffer_capacity)
		.map(|_| Entry { state: State::new(STATE_FREE), value: UnsafeCell::new(MaybeUninit::uninit()) })
		.collect();

	let shared = Shared {
		buffer,
		buffer_head: AtomicUsize::new(0),
		buffer_tail: AtomicUsize::new(0),
		tx_sem: Semaphore::new(n),
		rx_sem: Semaphore::new(0),
		tx_count: AtomicU32::new(1),
	};
	let shared = Arc::new(shared);

	let tx = Tx { shared: shared.clone() };
	let rx = Rx { shared: shared.clone() };

	(tx, rx)
}
