use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};

use tokio::sync::Semaphore;

#[derive(Copy, Clone, Debug)]
pub enum Error {
	Closed,
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Tx<T>
where
	T: Send,
{
	shared: Arc<Shared<T>>,
}

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
	pub async fn recv(&mut self) -> Option<T> {
		while let Ok(permit) = self.shared.rx_sem.acquire().await {
			if let Some(datum) = self.take() {
				permit.forget();
				self.shared.tx_sem.add_permits(1);

				return Some(datum);
			} else {
				tokio::task::yield_now().await;
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
