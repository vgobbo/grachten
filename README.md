# Grachten

**Grachten** is an educational multi-producer, single-consumer (MPSC) bounded channel implementation in Rust, designed with an interface and behavioral semantics similar to [Tokio's MPSC channel](https://docs.rs/tokio/latest/tokio/sync/mpsc/).

> [!NOTE]
> **Educational & Learning Project**: This repository was built from the ground up as a learning exploration of Rust concurrency primitives, atomic memory orderings, synchronization mechanics, and async runtime interactions.

---

## Features

- **Bounded Multi-Producer, Single-Consumer (MPSC)**: Senders (`Tx`) can be freely cloned across tasks or threads; the receiver (`Rx`) is strictly single-consumer (`&mut self`).
- **Tokio-like Ergonomics**: Matches Tokio's async channel interface (`send(&self, T) -> Result<()>`, `recv(&mut self) -> Option<T>`).
- **Strict FIFO Ordering**: Messages are processed strictly in the order they were sequenced, even across multiple concurrent producers.
- **Acquire-Release Synchronization**: Sound lock-free message slot publication without data races or undefined behavior.
- **Cancellation & Panic Resilience**: Employs an RAII panic guard (`EntryTxGuard`) to ensure that tasks aborted mid-send do not stall or live-lock the receiver.
- **Leak-Free Destruction**: Dropping a channel with pending items in transit cleanly reclaims all initialized values.

---

## Architectural Design Decisions

Grachten blends higher-level async primitives with low-level lock-free techniques:

### 1. Two-Layer Synchronization Model

Instead of relying solely on a mutex or a purely lock-free queue, Grachten separates **capacity coordination** from **payload publication**:

1. **Backpressure & Permitting (`tokio::sync::Semaphore`)**:
   - `tx_sem` bounds the number of in-flight items to the configured capacity $n$.
   - `rx_sem` tracks the count of published messages available for the consumer.
   - This prevents buffer overflows and coordinates async task sleep/wakeup.

2. **Per-Slot Publication Protocol (`Entry<T>`)**:
   - Each slot holds an atomic state flag (`AtomicU8`: `STATE_FREE`, `STATE_TAKEN`, `STATE_ABORTED`) and a payload container (`UnsafeCell<MaybeUninit<T>>`).
   - Senders write the payload and store `STATE_TAKEN` with `Ordering::Release`.
   - The receiver checks the slot state with `Ordering::Acquire`, creating a sound happens-before edge that ensures the initialized value is observed without data races.

### 2. Power-of-Two Circular Buffer & Bitwise Masking

- Bounded capacities are rounded up to the nearest power of two (`n.next_power_of_two()`).
- Modulo index calculation (`index % len`) is replaced by bitwise masking (`index & (capacity - 1)`).
- **Wrap-around Safety**: Because $2^{\text{BITS}}$ is divisible by any power of two, ticket counters wrapping at `usize::MAX` transition seamlessly back to index `0` without skipping slots or double-visiting entries.
- **Performance**: Bitwise masking executes in a single CPU cycle, avoiding expensive hardware division instructions.

### 3. Out-of-Order Producer Handling & Progressive Backoff (`Yielder`)

When concurrent producers claim sequence tickets via `buffer_tail.fetch_add(1)`, Producer 2 may finish writing before Producer 1 publishes. 

Because the channel preserves strict FIFO ordering, the consumer cannot consume slot 1 until slot 0 is resolved. To resolve this without blocking the runtime worker thread or spinning at 100% CPU:
- Grachten uses a progressive backoff helper (`Yielder`).
- For the first few attempts, it cooperatively yields to the Tokio scheduler (`tokio::task::yield_now()`), allowing co-located tasks on the same thread to finish.
- If the producer was preempted across OS threads, it graduates to short non-blocking sleep intervals (`tokio::time::sleep`), eliminating busy-wait CPU starvation.

### 4. Panic & Task Abortion Safety (`EntryTxGuard`)

If a producer task claims a ticket but panics or is aborted before publishing:
- An RAII guard (`EntryTxGuard`) detects the unwind and marks the slot as `STATE_ABORTED`.
- The unused capacity permit is automatically returned to `tx_sem` by Tokio's permit drop handler.
- When the receiver reaches the aborted slot, it resets the state to `STATE_FREE`, advances `buffer_head`, and seamlessly proceeds to the next message without live-locking.

### 5. Disconnection & Draining Semantics

- An atomic counter (`tx_count`) tracks active sender handles with `AcqRel` orderings.
- When the final sender drops, `rx_sem` is closed.
- If messages remain in the buffer when senders disconnect, `recv()` drains all queued items in FIFO order before returning `None`.

---

## Getting Started

Add Grachten and Tokio to your `Cargo.toml`:

```toml
[dependencies]
grachten = { path = "." }
tokio = { version = "1", features = ["rt", "sync", "time"] }
```

### Basic Example

```rust
use grachten::bounded;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = bounded(4);

    // Spawn a producer task
    let tx_worker = tx.clone();
    tokio::spawn(async move {
        tx_worker.send("Hello from producer!").await.unwrap();
    });

    // Drop the initial sender so the channel can close once workers finish
    drop(tx);

    // Consume messages
    while let Some(msg) = rx.recv().await {
        println!("Received: {msg}");
    }

    println!("All senders dropped and channel drained.");
}
```

---

## Running Tests

Run the full integration test suite:

```shell
cargo test
```

Generate the API documentation:

```shell
cargo doc --no-deps --open
```

---

## Benchmarks: Grachten vs Tokio MPSC

Grachten includes a built-in benchmark target comparing its performance directly against `tokio::sync::mpsc::channel` across varying contention and capacity profiles.

Anyone can easily execute the benchmark suite with:

```shell
cargo bench
```

### Representative Benchmark Results

Measured on Linux (x86_64), taking the median over 5 samples across a multi-threaded Tokio runtime:

| Scenario | Grachten (time) | Tokio (time) | Grachten Throughput | Tokio Throughput |
|---|---|---|---|---|
| **SPSC** (cap 64, 500k msgs) | ~34.6 ms | ~26.4 ms | **14.45 M msgs/sec** | **18.93 M msgs/sec** |
| **SPSC** (cap 1024, 500k msgs) | ~56.2 ms | ~36.6 ms | **8.89 M msgs/sec** | **13.65 M msgs/sec** |
| **Ping-Pong** (cap 1, 100k msgs) | ~29.4 ms | ~24.1 ms | **3.40 M msgs/sec** | **4.16 M msgs/sec** |
| **MPSC 4 senders** (cap 64, 500k msgs) | ~88.9 ms | ~61.9 ms | **5.62 M msgs/sec** | **8.08 M msgs/sec** |
| **MPSC 8 senders** (cap 128, 800k msgs) | ~168.5 ms | ~110.2 ms | **4.75 M msgs/sec** | **7.26 M msgs/sec** |
| **MPSC 16 senders** (cap 256, 800k msgs) | ~205.4 ms | ~133.7 ms | **3.89 M msgs/sec** | **5.98 M msgs/sec** |

#### Observations:
- In low-contention SPSC pipelines, Grachten achieves **~14.5 million messages/sec**, reaching roughly **75% of Tokio's throughput** despite its simpler design.
- In high-contention multi-producer scenarios (8–16 senders), Tokio's chunked linked-list design and dedicated waker list maintain a ~1.5× lead, whereas Grachten maintains a solid **3.9–4.8 million messages/sec**.
