use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::{mem::MaybeUninit, sync::Arc};

use tokio::sync::Semaphore;

#[derive(Clone)]
#[allow(unused)]
pub struct Tx<T>
where
	T: Send + Sync,
{
	shared: Arc<Shared<T>>,
}

#[allow(unused)]
pub struct Rx<T>
where
	T: Send + Sync,
{
	shared: Arc<Shared<T>>,
}

#[allow(unused)]
struct Shared<T>
where
	T: Send + Sync,
{
	buffer: Vec<UnsafeCell<MaybeUninit<T>>>,
	buffer_head: AtomicUsize,
	buffer_tail: AtomicUsize,
	tx_sem: Semaphore,
	rx_sem: Semaphore,
}

pub fn bounded<T>(n: usize) -> (Tx<T>, Rx<T>)
where
	T: Send + Sync,
{
	assert!(n > 0);

	let buffer: Vec<_> = (0..n).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();

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
