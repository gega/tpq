#include <stdint.h>


#ifndef TPQ_BUFFER_COUNT
#define TPQ_BUFFER_COUNT 32
#endif

#ifndef TPQ_BUFFER_SIZE
#define TPQ_BUFFER_SIZE  244
#endif

#ifndef TPQ_MUTEX_TYPE
#undef TPQ_MUTEX_TYPE
#endif

#ifndef TPQ_MUTEX_LOCK
#define TPQ_MUTEX_LOCK(m)
#endif

#ifndef TPQ_MUTEX_UNLOCK
#define TPQ_MUTEX_UNLOCK(m)
#endif

#ifndef TPQ_MUTEX_INIT
#define TPQ_MUTEX_INIT(m)
#endif

#ifndef TPQ_BUF_TYPE
#define TPQ_BUF_TYPE uint8_t
#endif

#ifndef TPQ_BUF_COPY
#define TPQ_BUF_COPY(bf,of,si,ib) memcpy(ib,of+bf,si)
#endif

#ifndef TPQ_SIZE_TYPE
#define TPQ_SIZE_TYPE size_t
#endif

#ifndef TPQ_ID_TYPE
#define TPQ_ID_TYPE uint16_t
#endif

#define TPQI_STATUS_EMPTY      (0)
#define TPQI_STATUS_FULL       (1)
#define TPQI_STATUS_PROCESSING (2)
#define TPQI_STATUS_DONE       (3)

struct tpq_tp_queue
{
  uint8_t buf[TPQ_BUFFER_SIZE];
  TPQ_SIZE_TYPE size;
  uint8_t status;
};

typedef struct
{
  struct tpq_tp_queue *btq;
#ifdef TPQ_MUTEX_TYPE
  TPQ_MUTEX_TYPE tpq_mut;
#endif
  TPQ_ID_TYPE expected_chunk_id;
  TPQ_ID_TYPE expected_rec_chunk;
  int tpq_space;
  int tpq_head;
  TPQ_ID_TYPE tpq_head_id;
} tpq_t;


#ifdef TPQ_IMPLEMENTATION

#define TPQIDX(b,n) (((n)+((b)->tpq_head))%TPQ_BUFFER_COUNT)
#define TPQI(b,n)   (b)->btq[TPQIDX(b,n)]


static void tpq_reset(tpq_t *b, struct tpq_tp_queue *q, int size)
{
  TPQ_MUTEX_LOCK(b->tpq_mut);
  b->tpq_space=TPQ_BUFFER_COUNT;
  b->btq=q;
  memset(b->btq, 0, size);
  b->expected_chunk_id=0;
  b->expected_rec_chunk=0;
  b->tpq_head=0;
  b->tpq_head_id=0;
  TPQ_MUTEX_UNLOCK(b->tpq_mut);
}

static void tpq_init(tpq_t *b, struct tpq_tp_queue *q, int size)
{
  TPQ_MUTEX_INIT(b->tpq_mut);
  tpq_reset(b,q,size);
}

#define TPQ_ERR_INVALID_CHUNK -1
#define TPQ_ERR_DUPLICATED_CHUNK -2
#define TPQ_ERR_OLD_CHUNK -3
#define TPQ_ERR_NO_SPACE -4

static int tpq_fill(tpq_t *b, TPQ_ID_TYPE id, const TPQ_BUF_TYPE *om, int offset, TPQ_SIZE_TYPE size) // producer
{
  int ret=0,i;

  TPQ_MUTEX_LOCK(b->tpq_mut);
  TPQ_ID_TYPE exp_prc=b->expected_chunk_id;
  TPQ_ID_TYPE slot=(TPQ_ID_TYPE)(id - exp_prc);
  if(slot<TPQ_BUFFER_COUNT)
  {
    if(size>0 && size<=TPQ_BUFFER_SIZE)
    {
      if(TPQI(b,slot).status==TPQI_STATUS_EMPTY)
      {
        TPQI(b,slot).size=size;
        TPQ_BUF_COPY(om, offset, size, TPQI(b,slot).buf);
        TPQI(b,slot).status=TPQI_STATUS_FULL;
        // find the first missing slot
        for(i=0; i<TPQ_BUFFER_COUNT; i++) if(TPQI(b,i).status==TPQI_STATUS_EMPTY) break;
        b->expected_rec_chunk=exp_prc+i;
        b->tpq_space--;
      }
      else ret=TPQ_ERR_DUPLICATED_CHUNK;
    }
    else ret=TPQ_ERR_INVALID_CHUNK;
  }
  else
  {
    if((int16_t)slot<0) ret=TPQ_ERR_OLD_CHUNK;
    else ret=TPQ_ERR_NO_SPACE;
  }
  TPQ_MUTEX_UNLOCK(b->tpq_mut);

  return(ret);
}

#if TPQ_BUFFER_COUNT <= 64
static
#if TPQ_BUFFER_COUNT <= 16
#define TPQ_MAP_TYPE uint16_t
#elif TPQ_BUFFER_COUNT <= 32
#define TPQ_MAP_TYPE uint32_t
#else
#define TPQ_MAP_TYPE uint64_t
#endif
TPQ_MAP_TYPE tpq_map(tpq_t *b)
{
  TPQ_MAP_TYPE ret=0;
  int i;
  TPQ_MUTEX_LOCK(b->tpq_mut);
  for(i=0; i<TPQ_BUFFER_COUNT; i++) ret=(ret<<1) | (TPQI(b,i).status!=TPQI_STATUS_EMPTY);
  TPQ_MUTEX_UNLOCK(b->tpq_mut);
  return(ret);
}
#else
#define tpq_map(b) (0)
#endif

static int tpq_prc_begin(tpq_t *b, const uint8_t **data, TPQ_SIZE_TYPE *size) // consumer
{
  int ret=-1;
  TPQ_MUTEX_LOCK(b->tpq_mut);
  ret=b->tpq_head;
  if(ret>=0)
  {
    for(int i=1;i<=(TPQ_BUFFER_COUNT-b->tpq_space) && (b->btq[ret].status==TPQI_STATUS_PROCESSING||b->btq[ret].status==TPQI_STATUS_DONE);i++) ret=TPQIDX(b,i);
    if(ret>=0 && b->btq[ret].status==TPQI_STATUS_FULL)
    {
      b->btq[ret].status=TPQI_STATUS_PROCESSING;
      if(NULL!=data) *data=b->btq[ret].buf;
      if(NULL!=size) *size=b->btq[ret].size;
    }
    else ret=-1;
  }
  TPQ_MUTEX_UNLOCK(b->tpq_mut);
  return(ret);
}

static int tpq_prc_done(tpq_t *b, int handle, int success) // consumer
{
  int ret=-1;
  // finish processing the slot, advance the ring if possible
  if(handle>=0 && success)
  {
    TPQ_MUTEX_LOCK(b->tpq_mut);
    int head=b->tpq_head;
    if(b->btq[handle].status==TPQI_STATUS_PROCESSING)
    {
      b->btq[handle].size=0;
      b->btq[handle].status=TPQI_STATUS_DONE;
      if(handle==head)
      {
        while(b->btq[handle].status==TPQI_STATUS_DONE)
        {
          b->btq[handle].status=TPQI_STATUS_EMPTY;
          head=(++head>=TPQ_BUFFER_COUNT?0:head);
          b->tpq_head_id++;
          TPQ_ID_TYPE curr=b->expected_chunk_id;
          b->tpq_head=head;
          b->expected_chunk_id=curr+1;
          handle=(handle+1)%TPQ_BUFFER_COUNT;
          b->tpq_space++;
        }
      }
      ret=0;
    }
    TPQ_MUTEX_UNLOCK(b->tpq_mut);
  }
  return(ret);
}

static TPQ_ID_TYPE tpq_get_expected_chunk_id(tpq_t *b)
{
  TPQ_ID_TYPE ret;
  TPQ_MUTEX_LOCK(b->tpq_mut);
  ret=b->expected_chunk_id;
  TPQ_MUTEX_UNLOCK(b->tpq_mut);
  return(ret);
}

#endif
