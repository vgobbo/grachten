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

struct Entry<T>
where
	T: Send,
{
	state: State,
	value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Tx<T>
where
	T: Send,
{
	pub async fn send(&self, datum: T) -> Result<()> {
		let permit = self.shared.tx_sem.acquire().await.map_err(|_| Error::Closed)?;

		let index = self.shared.buffer_tail.fetch_add(1, Ordering::Relaxed);
		let index = index % self.shared.buffer.len();

		let entry = &self.shared.buffer[index];

		let ptr: *mut MaybeUninit<T> = entry.value.get();
		unsafe {
			*ptr = MaybeUninit::new(datum);
		}
		entry.state.store(STATE_TAKEN, Ordering::Release);

		permit.forget();
		self.shared.rx_sem.add_permits(1);

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
		self.shared.tx_count.fetch_sub(1, Ordering::Relaxed);
	}
}

impl<T> Rx<T>
where
	T: Send,
{
	pub async fn recv(&mut self) -> Result<T> {
		// only one consumer is allowed, so we can read once and increment only when we are certain.
		let buffer_head = self.shared.buffer_head.load(Ordering::Acquire);
		let index = buffer_head % self.shared.buffer.len();

		loop {
			let permit = self.shared.rx_sem.acquire().await.map_err(|_| Error::Closed)?;

			let entry = &self.shared.buffer[index];
			if entry.state.load(Ordering::Acquire) == STATE_FREE {
				// drop the permit, so it can be acquired again after yielding.
				drop(permit);

				// TODO Effective yield.
				tokio::task::yield_now().await;
				continue;
			}

			self.shared.buffer_head.store(buffer_head + 1, Ordering::Release);

			let ptr: *mut MaybeUninit<T> = entry.value.get();
			let datum = unsafe {
				let datum = (*ptr).assume_init_read();
				*ptr = MaybeUninit::uninit();

				datum
			};

			entry.state.store(STATE_FREE, Ordering::Release);

			permit.forget();
			self.shared.tx_sem.add_permits(1);

			if self.shared.tx_count.load(Ordering::Relaxed) == 0 {
				let buffer_tail = self.shared.buffer_tail.load(Ordering::Relaxed);
				if buffer_head == buffer_tail {
					self.shared.rx_sem.close();
				}
			}

			return Ok(datum);
		}
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
	T: Send + Sync,
{
	assert!(n > 0);

	let buffer: Vec<_> = (0..n)
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
