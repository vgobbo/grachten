use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use tokio::sync::Semaphore;

#[derive(Copy, Clone, Debug)]
pub enum Error {
	Closed,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Tx<T>
where
	T: Send + Sync,
{
	shared: Arc<Shared<T>>,
}

pub struct Rx<T>
where
	T: Send + Sync,
{
	shared: Arc<Shared<T>>,
}

struct Shared<T>
where
	T: Send + Sync,
{
	buffer: Vec<Entry<T>>,
	buffer_head: AtomicUsize,
	buffer_tail: AtomicUsize,
	tx_sem: Semaphore,
	rx_sem: Semaphore,
}

unsafe impl<T: Send + Sync> Send for Shared<T> {}
unsafe impl<T: Send + Sync> Sync for Shared<T> {}

type State = AtomicU8;
const STATE_FREE: u8 = 0;
const STATE_TAKEN: u8 = 1;

struct Entry<T>
where
	T: Send + Sync,
{
	state: State,
	value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Tx<T>
where
	T: Send + Sync,
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

impl<T> Rx<T>
where
	T: Send + Sync,
{
	pub async fn recv(&mut self) -> Result<T> {
		let permit = self.shared.rx_sem.acquire().await.map_err(|_| Error::Closed)?;

		let index = self.shared.buffer_head.fetch_add(1, Ordering::Acquire);
		let index = index % self.shared.buffer.len();

		loop {
			let entry = &self.shared.buffer[index];
			if entry.state.load(Ordering::Acquire) == STATE_FREE {
				// TODO Effective yield.
				tokio::task::yield_now().await;
				continue;
			}

			let ptr: *mut MaybeUninit<T> = entry.value.get();
			let datum = unsafe {
				let datum = (*ptr).assume_init_read();
				*ptr = MaybeUninit::uninit();

				datum
			};

			entry.state.store(STATE_FREE, Ordering::Release);

			permit.forget();
			self.shared.tx_sem.add_permits(1);

			return Ok(datum);
		}
	}
}

impl<T> Drop for Rx<T>
where
	T: Send + Sync,
{
	fn drop(&mut self) {
		self.shared.tx_sem.close();
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
	};
	let shared = Arc::new(shared);

	let tx = Tx { shared: shared.clone() };
	let rx = Rx { shared: shared.clone() };

	(tx, rx)
}
