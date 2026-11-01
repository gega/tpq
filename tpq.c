#include <stdio.h>
#include <stdlib.h>
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

static struct tpq_tp_queue queue[TPQ_BUFFER_COUNT];
static tpq_t tpq;


static void check_invariants(const char *file, int line)
{
  int empty = 0;

  pthread_mutex_lock(&tpq.tpq_mut);

  for(int i = 0; i < TPQ_BUFFER_COUNT; i++)
  {
    int status = queue[i].status;

    if(status == TPQI_STATUS_EMPTY)
    {
      empty++;
    }
    else if(status != TPQI_STATUS_FULL &&
            status != TPQI_STATUS_PROCESSING &&
            status != TPQI_STATUS_DONE)
    {
      fprintf(stderr, "INVARIANT %s:%d: slot %d status=%d\n", file, line, i, status);
      abort();
    }
  }

  if(tpq.tpq_space < 0 ||
     tpq.tpq_space > TPQ_BUFFER_COUNT ||
     tpq.tpq_space != empty ||
     tpq.tpq_head < 0 ||
     tpq.tpq_head >= TPQ_BUFFER_COUNT)
  {
    fprintf(stderr, "INVARIANT %s:%d: space=%d empty=%d head=%d\n", file, line, tpq.tpq_space, empty, tpq.tpq_head);
    abort();
  }

  pthread_mutex_unlock(&tpq.tpq_mut);
}

#define CHECK(x) do { \
          check_invariants(__FILE__, __LINE__); \
          if(!(x)) { \
            fprintf(stderr, "FAIL %s:%d: %s\n", \
                    __FILE__, __LINE__,#x); \
            exit(1); \
          } \
          check_invariants(__FILE__, __LINE__); \
} while(0)

#define EQ(a, b) do { \
          check_invariants(__FILE__, __LINE__); \
          long long _a = (a), _b = (b); \
          if(_a != _b) { \
            fprintf(stderr, \
                    "FAIL %s:%d: %s=%lld expected %s=%lld\n", \
                    __FILE__, __LINE__,#a, _a,#b, _b); \
            exit(1); \
          } \
          check_invariants(__FILE__, __LINE__); \
} while(0)


static void reset_queue(void)
{
  tpq_reset(&tpq, queue, sizeof(queue));
}
static int put(uint16_t id)
{
  uint8_t packet[8] = {0xa5, 0x5a, (uint8_t)id, (uint8_t)(id >> 8), 0x11, 0x22, 0x33, 0x44};
  return tpq_fill(&tpq, id, packet, 2, 4);
}
static int begin(uint16_t id)
{
  const uint8_t *p = NULL;
  size_t n = 0;
  int h = tpq_prc_begin(&tpq, &p, &n);
  CHECK(h >= 0);
  EQ(n, 4);
  EQ(p[0], (uint8_t)id);
  EQ(p[1], (uint8_t)(id >> 8));
  EQ(p[2], 0x11);
  EQ(p[3], 0x22);
  return h;
}
static void happy(void)
{
  reset_queue();
  EQ(tpq_get_expected_chunk_id(&tpq), 0);
  EQ(tpq_prc_begin(&tpq, NULL, NULL), -1);
  EQ(put(0), 0);
  EQ(tpq.expected_rec_chunk, 1);
  int h = begin(0);
  EQ(tpq_prc_begin(&tpq, NULL, NULL), -1);
  EQ(tpq_prc_done(&tpq, h, 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 1);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
  EQ(tpq_prc_done(&tpq, h, 1), -1);
}
static void full(void)
{
  reset_queue();
  for(int i = 0; i < TPQ_BUFFER_COUNT; i++) EQ(put(i), 0);
  EQ(tpq.tpq_space, 0);
  EQ(tpq.expected_rec_chunk, TPQ_BUFFER_COUNT);
  EQ(put(TPQ_BUFFER_COUNT), TPQ_ERR_NO_SPACE);
  EQ(put(0), TPQ_ERR_DUPLICATED_CHUNK);
  EQ(tpq_fill(&tpq, 1, (const uint8_t *)"x", 0, 0), TPQ_ERR_INVALID_CHUNK);
  EQ(tpq_fill(&tpq, 1, (const uint8_t *)"x", 0, TPQ_BUFFER_SIZE + 1), TPQ_ERR_INVALID_CHUNK);
  for(int i = 0; i < TPQ_BUFFER_COUNT; i++)
  {
    int h = begin(i); EQ(tpq_prc_done(&tpq, h, 1), 0);
  }
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
  EQ(tpq_get_expected_chunk_id(&tpq), TPQ_BUFFER_COUNT);
  EQ(put(0), TPQ_ERR_OLD_CHUNK);
}
static void roaming(void)
{
  reset_queue();
  for(int i = 0; i < 1000; i++)
  {
    EQ(put((uint16_t)i), 0);
    int h = begin((uint16_t)i);
    EQ(h, i % TPQ_BUFFER_COUNT);
    EQ(tpq_prc_done(&tpq, h, 1), 0);
    EQ(tpq_get_expected_chunk_id(&tpq), i + 1);
  }
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
}
static void out_of_order_fill(void)
{
  reset_queue();
  EQ(put(2), 0);
  EQ(tpq.expected_rec_chunk, 0);
  EQ(tpq_prc_begin(&tpq, NULL, NULL), -1);
  EQ(put(0), 0);
  EQ(tpq.expected_rec_chunk, 1);
  EQ(put(1), 0);
  EQ(tpq.expected_rec_chunk, 3);
  for(int i = 0; i < 3; i++)
  {
    int h = begin(i); EQ(tpq_prc_done(&tpq, h, 1), 0);
  }
}
static void multi_in_order(void)
{
  reset_queue();
  for(int i = 0; i < 4; i++) EQ(put(i), 0);
  int h[4];
  for(int i = 0; i < 4; i++) h[i] = begin(i);
  EQ(tpq_prc_begin(&tpq, NULL, NULL), -1);
  for(int i = 0; i < 4; i++)
  {
    EQ(tpq_prc_done(&tpq, h[i], 1), 0);
    EQ(tpq_get_expected_chunk_id(&tpq), i + 1);
  }
}
static void multi_out_of_order(void)
{
  reset_queue();
  for(int i = 0; i < 4; i++) EQ(put(i), 0);
  int h[4];
  for(int i = 0; i < 4; i++) h[i] = begin(i);
  EQ(tpq_get_expected_chunk_id(&tpq), 0);
  EQ(tpq_prc_done(&tpq, h[3], 1), 0);
  EQ(tpq_prc_done(&tpq, h[2], 1), 0);
  EQ(tpq_prc_done(&tpq, h[1], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 0);
  EQ(tpq_prc_done(&tpq, h[0], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 4);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
  EQ(put(4), 0);
  int next = begin(4);
  EQ(tpq_prc_done(&tpq, next, 1), 0);
}
static void failure(void)
{
  reset_queue();
  EQ(put(0), 0);
  int h = begin(0);
  EQ(tpq_prc_done(&tpq, h, 0), -1);
  EQ(tpq_get_expected_chunk_id(&tpq), 0);
  EQ(tpq_prc_done(&tpq, h, 1), 0);
}
static void wrap_ids(void)
{
  reset_queue();
  tpq.expected_chunk_id = UINT16_MAX - 1;
  tpq.expected_rec_chunk = UINT16_MAX - 1;
  EQ(put(UINT16_MAX - 1), 0);
  EQ(put(UINT16_MAX), 0);
  EQ(put(0), 0);
  EQ(tpq.expected_rec_chunk, 1);
  const uint16_t ids[] = {UINT16_MAX - 1, UINT16_MAX, 0};
  for(int i = 0; i < 3; i++)
  {
    int h = begin(ids[i]); EQ(tpq_prc_done(&tpq, h, 1), 0);
  }
  EQ(tpq_get_expected_chunk_id(&tpq), 1);
}
static void map_bits(void)
{
  reset_queue();
  EQ(tpq_map(&tpq), 0);
  EQ(put(0), 0);
  EQ(tpq_map(&tpq), UINT32_C(0x80000000));
  EQ(put(31), 0);
  EQ(tpq_map(&tpq), UINT32_C(0x80000001));
}


static void complete_all_out_of_order(void)
{
  reset_queue();
  int h[TPQ_BUFFER_COUNT];

  for(int i = 0; i < TPQ_BUFFER_COUNT; i++)EQ(put(i), 0);

  for(int i = 0; i < TPQ_BUFFER_COUNT; i++)h[i] = begin(i);

  for(int i = TPQ_BUFFER_COUNT - 1; i > 0; i--)
  {
    EQ(tpq_prc_done(&tpq, h[i], 1), 0);
    EQ(tpq_get_expected_chunk_id(&tpq), 0);
  }

  EQ(tpq.tpq_space, 0);
  EQ(tpq_prc_done(&tpq, h[0], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), TPQ_BUFFER_COUNT);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);

  for(int i = 0; i < TPQ_BUFFER_COUNT; i++)EQ(queue[i].status, TPQI_STATUS_EMPTY);

  EQ(put(TPQ_BUFFER_COUNT), 0);
  int next = begin(TPQ_BUFFER_COUNT);
  EQ(tpq_prc_done(&tpq, next, 1), 0);
}

static void partial_head_advance(void)
{
  reset_queue();
  int h[5];

  for(int i = 0; i < 5; i++)EQ(put(i), 0);

  for(int i = 0; i < 5; i++)h[i] = begin(i);

  for(int i = 2; i <= 4; i++)EQ(tpq_prc_done(&tpq, h[i], 1), 0);

  EQ(tpq_get_expected_chunk_id(&tpq), 0);

  EQ(tpq_prc_done(&tpq, h[0], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 1);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT - 4);

  EQ(tpq_prc_done(&tpq, h[1], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 5);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
}

static void completion_wraparound(void)
{
  reset_queue();

  for(int i = 0; i < 30; i++)
  {
    EQ(put(i), 0);
    int h = begin(i);
    EQ(tpq_prc_done(&tpq, h, 1), 0);
  }

  EQ(tpq.tpq_head, 30);
  EQ(tpq_get_expected_chunk_id(&tpq), 30);

  int h[4];
  for(int i = 0; i < 4; i++)EQ(put(30 + i), 0);

  for(int i = 0; i < 4; i++)
  {
    h[i] = begin(30 + i);
    EQ(h[i], (30 + i) % TPQ_BUFFER_COUNT);
  }

  for(int i = 3; i > 0; i--)
  {
    EQ(tpq_prc_done(&tpq, h[i], 1), 0);
    EQ(tpq_get_expected_chunk_id(&tpq), 30);
  }

  EQ(tpq_prc_done(&tpq, h[0], 1), 0);
  EQ(tpq_get_expected_chunk_id(&tpq), 34);
  EQ(tpq.tpq_head, 2);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
}


#include <sched.h>

#define TEST_THREADS 8

static pthread_barrier_t start_barrier;
static int acquired[TEST_THREADS];

static void *acquire_worker(void *arg)
{
  int id = *(int *)arg;
  pthread_barrier_wait(&start_barrier);
  acquired[id] = tpq_prc_begin(&tpq, NULL, NULL);
  return NULL;
}

static void concurrent_consumers(void)
{
  reset_queue();

  pthread_t threads[TEST_THREADS];
  int ids[TEST_THREADS];

  for(int i = 0; i < TEST_THREADS; i++)EQ(put(i), 0);
  EQ(pthread_barrier_init(&start_barrier, NULL, TEST_THREADS + 1), 0);
  for(int i = 0; i < TEST_THREADS; i++)
  {
    ids[i] = i;
    EQ(pthread_create(&threads[i], NULL, acquire_worker, &ids[i]), 0);
  }

  pthread_barrier_wait(&start_barrier);

  for(int i = 0; i < TEST_THREADS; i++)EQ(pthread_join(threads[i], NULL), 0);
  for(int i = 0; i < TEST_THREADS; i++)
  {
    CHECK(acquired[i] >= 0);
    for(int j = 0; j < i; j++)CHECK(acquired[i] != acquired[j]);
  }

  for(int i = 0; i < TEST_THREADS; i++)EQ(tpq_prc_done(&tpq, acquired[i], 1), 0);

  EQ(tpq_get_expected_chunk_id(&tpq), TEST_THREADS);
  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);

  pthread_barrier_destroy(&start_barrier);
}


#include <sched.h>
#include <stdatomic.h>

#define STRESS_COUNT 100000
#define STRESS_WORKERS 8

static pthread_mutex_t seen_mutex = PTHREAD_MUTEX_INITIALIZER;
static unsigned char seen[STRESS_COUNT];
static atomic_int stress_consumed;
static atomic_int stress_producer_done;
static atomic_int stress_error;

static void *stress_producer(void *arg)
{
  (void)arg;

  for(int i = 0; i < STRESS_COUNT; i++)
  {
    int r;
    while((r = put((uint16_t)i)) == TPQ_ERR_NO_SPACE)sched_yield();

    if(r != 0)
    {
      fprintf(stderr, "Producer failed: i=%d id=%u ret=%d expected=%u space=%d\n", i, (unsigned)(uint16_t)i, r, tpq_get_expected_chunk_id(&tpq), tpq.tpq_space);
      atomic_store(&stress_error, 1);
      break;
    }
  }

  atomic_store(&stress_producer_done, 1);
  return NULL;
}

static void *stress_consumer(void *arg)
{
  unsigned id = (unsigned)(uintptr_t)arg;

  for(;;)
  {
    if(atomic_load(&stress_error))break;

    if(atomic_load(&stress_consumed) == STRESS_COUNT)break;

    const uint8_t *p = NULL;
    size_t n = 0;
    int h = tpq_prc_begin(&tpq, &p, &n);

    if(h < 0)
    {
      if(atomic_load(&stress_producer_done) && atomic_load(&stress_consumed) == STRESS_COUNT)break;
      sched_yield();
      continue;
    }

    if(n != 4 || p[2] != 0x11 || p[3] != 0x22)
    {
      atomic_store(&stress_error, 2);
      break;
    }

    uint16_t chunk = (uint16_t)p[0] | ((uint16_t)p[1] << 8);

    pthread_mutex_lock(&tpq.tpq_mut);
    uint16_t head_id = tpq.expected_chunk_id;
    pthread_mutex_unlock(&tpq.tpq_mut);

    int completed = atomic_load(&stress_consumed);

    int base = completed & ~65535;
    int seq = base + chunk;

    if(seq < completed - TPQ_BUFFER_COUNT)seq += 65536;
    if(seq >= STRESS_COUNT)seq -= 65536;

    (void)head_id;

    if(seq < 0 || seq >= STRESS_COUNT)
    {
      atomic_store(&stress_error, 3);
      break;
    }

    pthread_mutex_lock(&seen_mutex);
    if(seen[seq]++)atomic_store(&stress_error, 4);
    pthread_mutex_unlock(&seen_mutex);

    for(unsigned j = 0; j < (id * 17 + chunk) % 100; j++)sched_yield();

    if(tpq_prc_done(&tpq, h, 1) != 0)
    {
      atomic_store(&stress_error, 5);
      break;
    }

    atomic_fetch_add(&stress_consumed, 1);
  }

  return NULL;
}

static void concurrent_stress(void)
{
  reset_queue();
  memset(seen, 0, sizeof(seen));
  atomic_store(&stress_consumed, 0);
  atomic_store(&stress_producer_done, 0);
  atomic_store(&stress_error, 0);

  pthread_t producer;
  pthread_t workers[STRESS_WORKERS];

  for(uintptr_t i = 0; i < STRESS_WORKERS; i++)EQ(pthread_create(&workers[i], NULL, stress_consumer, (void *)i), 0);

  EQ(pthread_create(&producer, NULL, stress_producer, NULL), 0);

  EQ(pthread_join(producer, NULL), 0);

  for(int i = 0; i < STRESS_WORKERS; i++)EQ(pthread_join(workers[i], NULL), 0);

  EQ(atomic_load(&stress_error), 0);
  EQ(atomic_load(&stress_consumed), STRESS_COUNT);

  for(int i = 0; i < STRESS_COUNT; i++)EQ(seen[i], 1);

  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
  EQ(tpq_get_expected_chunk_id(&tpq), (uint16_t)STRESS_COUNT);
}



#define OOO_COUNT   100000
#define OOO_WORKERS 8
#define OOO_BATCH   16

static unsigned char ooo_seen[OOO_COUNT];
static pthread_mutex_t ooo_seen_mutex = PTHREAD_MUTEX_INITIALIZER;

static atomic_int ooo_completed;
static atomic_int ooo_producer_done;
static atomic_int ooo_error;

static void *ooo_producer(void *arg)
{
  (void)arg;

  for(int base = 0; base < OOO_COUNT; base += OOO_BATCH)
  {
    int end = base + OOO_BATCH;
    if(end > OOO_COUNT) end = OOO_COUNT;

    for(int i = end - 1; i >= base; i--)
    {
      uint8_t payload[4] = { (uint8_t)i, (uint8_t)(i >> 8), (uint8_t)(i >> 16), (uint8_t)(i >> 24) };

      int r;
      do 
      {
        if(atomic_load(&ooo_error))goto done;

        r = tpq_fill(&tpq, (uint16_t)i, payload, 0, sizeof(payload));

        if(r == TPQ_ERR_NO_SPACE)sched_yield();
      } while(r == TPQ_ERR_NO_SPACE);

      if(r != 0)
      {
        fprintf(stderr, "OOO producer: seq=%d ret=%d\n", i, r);
        atomic_store(&ooo_error, 1);
        goto done;
      }
    }
  }

done:
  atomic_store(&ooo_producer_done, 1);
  return NULL;
}

static void *ooo_consumer(void *arg)
{
  uintptr_t worker = (uintptr_t)arg;

  for(;;)
  {
    if(atomic_load(&ooo_error))break;

    if(atomic_load(&ooo_completed) >= OOO_COUNT)break;

    const uint8_t *p = NULL;
    size_t n = 0;
    int h = tpq_prc_begin(&tpq, &p, &n);

    if(h < 0)
    {
      sched_yield();
      continue;
    }

    if(n != 4)
    {
      atomic_store(&ooo_error, 2);
      break;
    }

    uint32_t seq = (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);

    if(seq >= OOO_COUNT)
    {
      atomic_store(&ooo_error, 3);
      break;
    }

    pthread_mutex_lock(&ooo_seen_mutex);
    if(ooo_seen[seq]++)atomic_store(&ooo_error, 4);
    pthread_mutex_unlock(&ooo_seen_mutex);

    unsigned delay = (unsigned)((seq * 17 + worker * 31) % 80);
    for(unsigned j = 0; j < delay; j++)sched_yield();

    if(tpq_prc_done(&tpq, h, 1) != 0)
    {
      atomic_store(&ooo_error, 5);
      break;
    }

    atomic_fetch_add(&ooo_completed, 1);
  }

  return NULL;
}

static void concurrent_out_of_order(void)
{
  reset_queue();

  memset(ooo_seen, 0, sizeof(ooo_seen));
  atomic_store(&ooo_completed, 0);
  atomic_store(&ooo_producer_done, 0);
  atomic_store(&ooo_error, 0);

  pthread_t producer;
  pthread_t consumers[OOO_WORKERS];

  for(uintptr_t i = 0; i < OOO_WORKERS; i++)EQ(pthread_create(&consumers[i], NULL, ooo_consumer, (void *)i), 0);

  EQ(pthread_create(&producer, NULL, ooo_producer, NULL), 0);

  EQ(pthread_join(producer, NULL), 0);

  if(atomic_load(&ooo_error))goto finish;

finish:
  for(int i = 0; i < OOO_WORKERS; i++)EQ(pthread_join(consumers[i], NULL), 0);

  EQ(atomic_load(&ooo_error), 0);
  EQ(atomic_load(&ooo_producer_done), 1);
  EQ(atomic_load(&ooo_completed), OOO_COUNT);

  for(int i = 0; i < OOO_COUNT; i++)EQ(ooo_seen[i], 1);

  EQ(tpq.tpq_space, TPQ_BUFFER_COUNT);
  EQ(tpq_get_expected_chunk_id(&tpq), (uint16_t)OOO_COUNT);
}



struct testcase { const char *name; void (*fn)(void); };
static const struct testcase tests[] =
{
  {"happy", happy},
  {"full", full},
  {"roaming", roaming},
  {"out_of_order_fill", out_of_order_fill},
  {"multi_in_order", multi_in_order},
  {"multi_out_of_order", multi_out_of_order},
  {"failure", failure},
  {"wrap_ids", wrap_ids},
  {"map_bits", map_bits},
  {"complete_all_out_of_order", complete_all_out_of_order},
  {"partial_head_advance", partial_head_advance},
  {"completion_wraparound", completion_wraparound},
  {"concurrent_consumers", concurrent_consumers},
  {"concurrent_stress", concurrent_stress},
  {"concurrent_out_of_order", concurrent_out_of_order},
};
int main(int argc, char **argv)
{
  tpq_init(&tpq, queue, sizeof(queue));
  int count = 0;
  for(size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++)
  {
    if(argc > 1 && strcmp(argv[1], tests[i].name)) continue;
    fprintf(stderr, "RUN %s\n", tests[i].name);
    tests[i].fn();
    fprintf(stderr, "PASS %s\n", tests[i].name);
    count++;
  }
  CHECK(count > 0);
  pthread_mutex_destroy(&tpq.tpq_mut);
  return 0;
}
