# TPQ — fixed-size, ordered chunk queue

`tpq.h` is a small, single-header C queue for receiving numbered chunks into a fixed-size ring and processing them with one or more consumers. Chunks may arrive out of order within the current window, and consumers may finish out of order. **The queue's committed head advances in chunk-ID order.** It allocates no memory dynamically.

Typical use: a transport receiver accepts numbered packets, while workers consume their payloads asynchronously.

## Configuration

Define configuration macros **before** including `tpq.h` in the translation unit that defines `TPQ_IMPLEMENTATION`.

| Macro | Default | Purpose |
| --- | --- | --- |
| `TPQ_BUFFER_COUNT` | `32` | Number of ring slots / maximum receive window |
| `TPQ_BUFFER_SIZE` | `244` | Payload capacity of each slot, in elements of `TPQ_BUF_TYPE` |
| `TPQ_BUF_TYPE` | `uint8_t` | Payload element type |
| `TPQ_SIZE_TYPE` | `size_t` | Payload length type |
| `TPQ_ID_TYPE` | `uint16_t` | Chunk sequence-number type |
| `TPQ_BUF_COPY(bf,of,si,ib)` | `memcpy(ib,of+bf,si)` | Copy from source `bf` at offset `of` into destination `ib` |
| `TPQ_MUTEX_TYPE` | unset | Mutex member type; enables the mutex member in `tpq_t` |
| `TPQ_MUTEX_INIT(m)` | no-op | Initialize mutex lvalue `m` |
| `TPQ_MUTEX_LOCK(m)` | no-op | Lock mutex lvalue `m` |
| `TPQ_MUTEX_UNLOCK(m)` | no-op | Unlock mutex lvalue `m` |

The default copy macro uses `memcpy` byte counts; if changing `TPQ_BUF_TYPE` away from `uint8_t`, adjust copy semantics accordingly. Include `<string.h>` and `<stddef.h>` (or otherwise make `memcpy`, `memset`, and `size_t` available) before the implementation.

**Concurrency:** Supply working mutex macros whenever producers and consumers can run on different threads. Without them, operations are not synchronized. The implementation uses plain fields, not atomics.

## Quick start (pthreads)

```c
#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>

#define TPQ_BUFFER_COUNT 32
#define TPQ_BUFFER_SIZE 256
#define TPQ_MUTEX_TYPE pthread_mutex_t
#define TPQ_MUTEX_INIT(m) pthread_mutex_init(&(m), NULL)
#define TPQ_MUTEX_LOCK(m) pthread_mutex_lock(&(m))
#define TPQ_MUTEX_UNLOCK(m) pthread_mutex_unlock(&(m))
#define TPQ_IMPLEMENTATION
#include "tpq.h"

static struct tpq_tp_queue storage[TPQ_BUFFER_COUNT];
static tpq_t queue;

int main(void)
{
    tpq_init(&queue, storage, sizeof(storage));

    const uint8_t message[] = { 0x11, 0x22, 0x33 };
    if (tpq_fill(&queue, 0, message, 0, sizeof(message)) != 0)
        return 1;

    const uint8_t *data;
    size_t size;
    int handle = tpq_prc_begin(&queue, &data, &size);
    if (handle >= 0) {
        /* Process data[0..size-1] before releasing the handle. */
        printf("received %zu bytes: %02x\n", size, data[0]);
        if (tpq_prc_done(&queue, handle, 1) != 0)
            return 1;
    }
    pthread_mutex_destroy(&queue.tpq_mut);
    return 0;
}
```

Build: `cc -std=c11 -Wall -Wextra -pthread main.c -o example` (with `tpq.h` in the include path).

`tpq_init()` initializes the mutex and resets the queue. Call it once; use `tpq_reset()` only when no producer or consumer is using the queue or retaining a handle/payload pointer. Pass the **byte size of the entire backing array** to `tpq_init()` / `tpq_reset()`; the implementation uses that argument directly in `memset` and does not validate it.

## API

All functions are defined `static` when `TPQ_IMPLEMENTATION` is enabled, so the implementation is local to that translation unit. There are no external linker symbols.

| Function | Behavior |
| --- | --- |
| `tpq_init(tpq_t *q, struct tpq_tp_queue *storage, int bytes)` | Initialize mutex and queue state; clear `bytes` of backing storage |
| `tpq_reset(q, storage, bytes)` | Reset counters, head, and backing storage; **not safe during active processing** |
| `tpq_fill(q, id, src, offset, size)` | Copy a numbered chunk into its slot; `0` on success, negative error otherwise |
| `tpq_prc_begin(q, &data, &size)` | Claim a ready chunk for processing; return physical slot index (`>=0`) or `-1` |
| `tpq_prc_done(q, handle, success)` | On nonzero `success`, mark a processing slot done; return `0` on success, `-1` otherwise |
| `tpq_get_expected_chunk_id(q)` | Mutex-protected snapshot of the next chunk ID awaiting in-order completion |
| `tpq_map(q)` | Occupancy bitmap for up to 64 slots; returns `0` for larger configurations |

`tpq_prc_begin()` can set either output pointer to `NULL` if the caller does not need it. The returned payload pointer is owned by the queue: do not free it or use it after completing the handle. The caller must not modify a claimed payload concurrently with other users.

`tpq_prc_done(q, handle, 0)` returns `-1` and **does not release or retry** the slot. The current API has no cancel/requeue operation. Consumers must eventually complete successfully or stop and reset the queue after all activity has ceased.

### Producer return codes

| Return | Meaning |
| --- | --- |
| `0` | Accepted |
| `TPQ_ERR_INVALID_CHUNK` (`-1`) | Size is zero or exceeds `TPQ_BUFFER_SIZE` (for an ID inside the receive window) |
| `TPQ_ERR_DUPLICATED_CHUNK` (`-2`) | The target slot is already occupied (`FULL`, `PROCESSING`, or `DONE`) |
| `TPQ_ERR_OLD_CHUNK` (`-3`) | Chunk ID is classified as behind the current window |
| `TPQ_ERR_NO_SPACE` (`-4`) | Chunk ID is outside the current receive window on the forward side |

An out-of-window ID is classified before payload size is checked. `TPQ_ERR_NO_SPACE` is suitable for retry after the head advances; duplicates and old chunks should not be blindly retried.

## Ordering and slot lifecycle

Each slot transitions as follows:

```text
EMPTY --tpq_fill--> FULL --tpq_prc_begin--> PROCESSING
                                                  |
                                     tpq_prc_done(success=1)
                                                  |
                                                 DONE
                                                  |
                      head reaches this slot --> EMPTY
```

A completed slot can remain `DONE` while an earlier chunk is still processing. When the head chunk completes, the implementation releases it and any consecutive `DONE` slots, advancing `expected_chunk_id` and increasing `tpq_space` for each released slot.

For example, with chunk IDs 0, 1, 2, and 3 claimed by consumers:

- Finish 2 and 3: head remains 0; neither slot is reusable.
- Finish 0: head advances to 1 and stops.
- Finish 1: head advances through 1, 2, and 3; all four slots become reusable.

`tpq_prc_begin()` skips slots already `PROCESSING` or `DONE` and claims a `FULL` slot. It does **not** skip a missing (`EMPTY`) slot: an arrival gap can temporarily prevent later chunks from being claimed, even when those later chunks have already arrived.

### Sequence numbers and window

`tpq_fill()` calculates a modular offset from `expected_chunk_id`; accepted IDs must be within `[head, head + TPQ_BUFFER_COUNT)` in sequence-number space. With the default 16-bit ID, the window wraps naturally from `65535` to `0`.

The implementation uses `(int16_t)slot` to distinguish old from forward/out-of-window IDs. **This assumes 16-bit sequence IDs and a receive window much smaller than half the sequence space.** Changing `TPQ_ID_TYPE` alone does not make that classification generic; adjust the signed comparison if using another ID width.

`expected_rec_chunk` is maintained by `tpq_fill()` as the first `EMPTY` slot found when scanning from the current processing head. It is a receive-progress indicator, not the committed/processed head. It is not automatically recomputed by `tpq_prc_done()`; read it under the queue mutex if another thread may update it.

### Occupancy map

For `TPQ_BUFFER_COUNT <= 64`, `tpq_map()` returns a bitmask with the **highest of the configured bits representing the logical head** and the lowest bit representing the final slot in the window. A set bit means the slot is not `EMPTY` (including `DONE`). For 32 slots, the head is bit 31. For configurations above 64, `tpq_map()` is a macro returning `0`, not a useful occupancy report.

## Threading and ownership rules

- With mutex macros enabled, queue metadata updates in the public operations are mutex-protected. Payload processing occurs outside the lock, allowing concurrent consumers.
- Each successful `tpq_prc_begin()` reserves one slot until successful `tpq_prc_done()`. A consumer must finish using the payload before calling `done`.
- Do not call `tpq_reset()` or destroy the mutex while workers are active or holding handles/pointers.
- Handles are physical slot indices, **not persistent identifiers**. Reusing a stale handle after its slot has been recycled is a caller error. The library does not provide generation checks or double-release protection.
- `tpq_prc_done()` does not validate that `handle` is below `TPQ_BUFFER_COUNT`; only pass handles obtained from a successful `tpq_prc_begin()`.
- If reading public struct fields directly from another thread (`tpq_space`, `expected_rec_chunk`, etc.), take the queue mutex. Prefer `tpq_get_expected_chunk_id()` for that counter.

## Memory and limitations

Storage is fixed: `TPQ_BUFFER_COUNT * sizeof(struct tpq_tp_queue)` bytes plus the `tpq_t` control structure. No heap allocation occurs. The queue copies accepted input into its own backing buffers. The maximum number of simultaneously occupied slots is `TPQ_BUFFER_COUNT`.

The implementation provides no blocking wait, condition variable, cancellation, timeout, automatic retransmission, or shutdown coordination. Callers implement retry/backpressure and worker scheduling externally. It is intended for trusted callers that respect buffer sizes, valid handles, and the reset lifecycle.

## Testing

The companion test program exercises normal operation, full queues, ring and sequence-number wraparound, out-of-order arrival and completion, multiple consumers, concurrent producer/consumer stress, and structural invariants. Run it under Valgrind or a sanitizer when supported by your toolchain; concurrency tests should be repeated because scheduling varies.
