/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"


/*
 * The shared freelist control information.
 */
typedef struct
{
	int 		lruHead;			/* most recently used in linked list */
	int 		lruTail;			/* least recently used in linked list */

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer; /* Tail of list of unused buffers */

	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Statistics.	These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Notification latch, or NULL if none.  See StrategyNotifyBgWriter.
	 */
	Latch	   *bgwriterLatch;
} BufferStrategyControl;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

/*
 * Since the buffer ring replacement strategy was disabled, this struct is no
 * longer used. To satisfy the compiler, this code has not been removed or
 * commented out.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			ring_size;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * True if the buffer just returned by StrategyGetBuffer had been in the
	 * ring already.
	 */
	bool		current_was_in_ring;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[1];		/* VARIABLE SIZE ARRAY */
}	BufferAccessStrategyData;


/* Prototypes for internal functions */
static volatile BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy);
static void AddBufferToRing(BufferAccessStrategy strategy,
				volatile BufferDesc *buf);
static void LRURemove(volatile BufferDesc *buf);


/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.  If
 *	*lock_held is set on exit, we have returned with the BufFreelistLock
 *	still held, as well; the caller must release that lock once the spinlock
 *	is dropped.  We do it that way because releasing the BufFreelistLock
 *	might awaken other processes, and it would be bad to do the associated
 *	kernel calls while holding the buffer header spinlock.
 */
volatile BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, bool *lock_held)
{
	volatile BufferDesc *buf;
	Latch	   *bgwriterLatch;
	int 		nextVictimBuffer;

	/*
	 * Make sure that we weren't given a strategy object, since non-default
	 * buffer replacement strategies have been disabled.
	 */
	if (strategy != NULL)
	{
		elog(ERROR, "StrategyGetBuffer: strategy is not default");
	}

	/* lock the freelist */
	*lock_held = true;
	LWLockAcquire(BufFreelistLock, LW_EXCLUSIVE);

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.	Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	StrategyControl->numBufferAllocs++;

	/*
	 * If bgwriterLatch is set, we need to waken the bgwriter, but we should
	 * not do so while holding BufFreelistLock; so release and re-grab.  This
	 * is annoyingly tedious, but it happens at most once per bgwriter cycle,
	 * so the performance hit is minimal.
	 */
	bgwriterLatch = StrategyControl->bgwriterLatch;
	if (bgwriterLatch)
	{
		StrategyControl->bgwriterLatch = NULL;
		LWLockRelease(BufFreelistLock);
		SetLatch(bgwriterLatch);
		LWLockAcquire(BufFreelistLock, LW_EXCLUSIVE);
	}

	/*
	 * Try to get a buffer from the freelist.  Note that the freeNext fields
	 * are considered to be protected by the BufFreelistLock not the
	 * individual buffer spinlocks, so it's OK to manipulate them without
	 * holding the spinlock.
	 */
	while (StrategyControl->firstFreeBuffer >= 0)
	{
		buf = &BufferDescriptors[StrategyControl->firstFreeBuffer];
		Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

		/* Unconditionally remove buffer from freelist */
		StrategyControl->firstFreeBuffer = buf->freeNext;
		buf->freeNext = FREENEXT_NOT_IN_LIST;

		/*
		 * If the buffer is pinned or is in the recently used list, we cannot
		 * use it; discard it and retry.  (This can only happen if VACUUM put a
		 * valid buffer in the freelist and then someone else used it before
		 * we got to it.  It's probably impossible altogether as of 8.3, but
		 * we'd better check anyway.)
		 */
		LockBufHdr(buf);
		if (buf->refcount == 0 && buf->lruNext == LRU_NOT_IN_LIST)
		{
			if (strategy != NULL)
				AddBufferToRing(strategy, buf);
			return buf;
		}
		UnlockBufHdr(buf);
	}

	/*
	 * Nothing on the freelist, so run the LRU algorithm. Note that if the tail
	 * of the LRU linked list is LRU_END_OF_LIST, the list is empty. This
	 * shouldn't happen if there aren't any buffers in the freelist, but to be
	 * safe, let's check.
	 */
	if (StrategyControl->lruTail == LRU_END_OF_LIST)
		elog(ERROR, "no buffers in the LRU linked list");

	nextVictimBuffer = StrategyControl->lruTail;
	for (;;)
	{
		buf = &BufferDescriptors[nextVictimBuffer];

		/*
		 * If the buffer is pinned, we cannot use it; leave it in the same spot
		 * in the LRU linked list and keep scanning.
		 */
		LockBufHdr(buf);
		if (buf->refcount == 0)
		{
			/* buf is usable! Update the LRU linked list and return buf. */
			elog(LOG, "StrategyGetBuffer: %i popping", buf->buf_id);
			LRURemove(buf);
			return buf;
		}
		else if (buf->lruPrev == LRU_END_OF_LIST)
		{
			/*
			 * We've gone through all of the LRU linked list, so all of the
			 * buffers must be pinned. As in the original PostgreSQL 9.2.2
			 * code, we'll give up and die rather than risk getting caught in
			 * an infinite loop while hoping that someone eventually frees
			 * a buffer.
			 */
			UnlockBufHdr(buf);
			elog(ERROR, "no unpinned buffers available");
		}
		else
		{
			/*
			 * buf isn't usable, and buf isn't at the end of the list, so
			 * update nextVictimBuffer to check the next buffer
			 */
			nextVictimBuffer = buf->lruPrev;
			UnlockBufHdr(buf);
		}
	}

	/* not reached */
	return NULL;
}

/*
 * StrategyUsedBuffer
 * 
 *  Called by PinBuffer in bufmgr.c to indicate that a buffer has been used.
 *  This modifies the LRU linked list, placing buf at the head.
 */
void
StrategyUsedBuffer(volatile BufferDesc *buf)
{
	volatile BufferDesc *oldHead;

	if (StrategyControl->lruHead != LRU_END_OF_LIST &&
		buf == &BufferDescriptors[StrategyControl->lruHead])
	{
		/* buf is already the head; don't do anything. */
		elog(LOG, "StrategyUsedBuffer: %i already at head", buf->buf_id);
		return;
	}
	else
	{
		/*
		 * If buf is in the list already, remove it before reinserting it at
		 * the head
		 */
		LockBufHdr(buf);
		LRURemove(buf);
		elog(LOG, "StrategyUsedBuffer: %i moving to head", buf->buf_id);
		buf->lruPrev = LRU_END_OF_LIST;
		buf->lruNext = StrategyControl->lruHead;
		UnlockBufHdr(buf);

		if (StrategyControl->lruHead != LRU_END_OF_LIST)
		{
			/*
			 * Before this call, the recently used list was non-empty; we
			 * need to update the lruPrev pointer of the old lruHead.
			 */
			oldHead = &BufferDescriptors[StrategyControl->lruHead];
			LockBufHdr(oldHead);
			oldHead->lruPrev = buf->buf_id;
			UnlockBufHdr(oldHead);
		}
		else
		{
			/*
			 * Before this call, the recently used list was empty; we need
			 * to update the lruTail as well as the lruHead
			 */
			StrategyControl->lruTail = buf->buf_id;
		}

		StrategyControl->lruHead = buf->buf_id;
	}
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 *
 * Also removes the buffer from the LRU list.
 */
void
StrategyFreeBuffer(volatile BufferDesc *buf)
{
	LWLockAcquire(BufFreelistLock, LW_EXCLUSIVE);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = StrategyControl->firstFreeBuffer;
		if (buf->freeNext < 0)
			StrategyControl->lastFreeBuffer = buf->buf_id;
		StrategyControl->firstFreeBuffer = buf->buf_id;

		LockBufHdr(buf);
		LRURemove(buf);
		UnlockBufHdr(buf);
	}

	LWLockRelease(BufFreelistLock);
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the count of recent buffer allocs if non-NULL
 * pointers are passed. The alloc count is reset after being read. Since
 * the completePasses field of StrategyControl no longer makes sense when not
 * using the clock replacement strategy, a hardcoded constant is returned in
 * *complete_passes; unless this causes something horrible to happen, this
 * will remain the case.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	int			result;

	LWLockAcquire(BufFreelistLock, LW_EXCLUSIVE);
	result = StrategyControl->lruTail;
	if (complete_passes)
	{
		/*
		 * Since StrategyControl->completePasses was removed when switching
		 * from the clock replacement strategy, I'm not sure what should be
		 * returned in *complete_passes. For now, always return 1 and see if
		 * anything horrible happens.
		 */
		*complete_passes = 1;
	}
	if (num_buf_alloc)
	{
		*num_buf_alloc = StrategyControl->numBufferAllocs;
		StrategyControl->numBufferAllocs = 0;
	}
	LWLockRelease(BufFreelistLock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwriterLatch isn't NULL, the next invocation of StrategyGetBuffer will
 * set that latch.	Pass NULL to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(Latch *bgwriterLatch)
{
	/*
	 * We acquire the BufFreelistLock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	LWLockAcquire(BufFreelistLock, LW_EXCLUSIVE);
	StrategyControl->bgwriterLatch = bgwriterLatch;
	LWLockRelease(BufFreelistLock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;

	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						sizeof(BufferStrategyControl),
						&found);

	if (!found)
	{
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		/*
		 * Grab the whole linked list of free buffers for our strategy. We
		 * assume it was previously set up by InitBufferPool().
		 */
		StrategyControl->firstFreeBuffer = 0;
		StrategyControl->lastFreeBuffer = NBuffers - 1;

		/* Initialize the LRU pointer */
		StrategyControl->lruHead = LRU_END_OF_LIST;
		StrategyControl->lruTail = LRU_END_OF_LIST;

		/* Clear statistics */
		StrategyControl->numBufferAllocs = 0;

		/* No pending notification */
		StrategyControl->bgwriterLatch = NULL;
	}
	else
		Assert(!init);
}

/* ----------------------------------------------------------------
 *				Private LRU management functions
 * ----------------------------------------------------------------
 */

/*
 * LRURemove -- remove a buffer from the LRU linked list
 * 
 * Does nothing if the buffer isn't already in the list.
 * Assumes that a spin lock is held on buf before calling.
 * Returns with a spin lock held on buf.
 */
static void
LRURemove(volatile BufferDesc *buf)
{
	volatile BufferDesc *bufPrev, *bufNext;

	int next = buf->lruNext;
	int prev = buf->lruPrev;

	if (prev == LRU_NOT_IN_LIST)
		return;
	
	if (prev == LRU_END_OF_LIST)
		StrategyControl->lruHead = next;

	if (next == LRU_END_OF_LIST)
		StrategyControl->lruTail = prev;

	buf->lruPrev = buf->lruNext = LRU_NOT_IN_LIST;
	UnlockBufHdr(buf);

	if (prev >= 0)
	{
		bufPrev = &BufferDescriptors[prev];
		LockBufHdr(bufPrev);
		bufPrev->lruNext = next;
		UnlockBufHdr(bufPrev);
	}
	if (next >= 0)
	{
		bufNext = &BufferDescriptors[next];
		LockBufHdr(bufNext);
		bufNext->lruPrev = prev;
		UnlockBufHdr(bufNext);
	}

	/* Lock buf again like the caller expects */
	LockBufHdr(buf);
}

/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 *              
 * Since the buffer ring replacement strategy was disabled by making
 * GetAccessStrategy() return NULL, these functions are no longer
 * used. To be safe (and to satisfy the compiler), they haven't
 * been deleted or commented out.
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	/* 
	 * In order to test the performance of a pure buffer replacement strategy
	 * without switching to a buffer ring strategy for improved performance on
	 * some queries, always return the "default" BufferAccessStrategy
	 */
	return NULL;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static volatile BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy)
{
	/* this should never be called */
	elog(ERROR, "reached freelist.c:GetBufferFromRing");

	/* not reached */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, volatile BufferDesc *buf)
{
	/* this should never be called */
	elog(ERROR, "reached freelist.c:AddBufferToRing");
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.	This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, volatile BufferDesc *buf)
{
	/* this should never be called */
	elog(ERROR, "reached freelist.c:StrategyRejectBuffer");

	return true;
}
