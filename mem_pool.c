/*
* Created by Ivo Georgiev on 2/9/16.
*/

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

//susing namespace std;

/*************/
/*           */
/* Constants */
/*           */
/*************/

// define these as precompiler constants instead of variables, or else compile errors
#define _MEM_FILL_FACTOR                                0.75
#define _MEM_EXPAND_FACTOR                              2
#define _MEM_POOL_STORE_INIT_CAPACITY					20
#define _MEM_NODE_HEAP_INIT_CAPACITY					40
#define _MEM_GAP_IX_INIT_CAPACITY						40

static const float      MEM_FILL_FACTOR = _MEM_FILL_FACTOR;
static const unsigned   MEM_EXPAND_FACTOR = _MEM_EXPAND_FACTOR;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY = _MEM_POOL_STORE_INIT_CAPACITY;
static const float      MEM_POOL_STORE_FILL_FACTOR = _MEM_FILL_FACTOR;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR = _MEM_EXPAND_FACTOR;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY = _MEM_NODE_HEAP_INIT_CAPACITY;
static const float      MEM_NODE_HEAP_FILL_FACTOR = _MEM_FILL_FACTOR;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR = _MEM_EXPAND_FACTOR;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY = _MEM_GAP_IX_INIT_CAPACITY;
static const float      MEM_GAP_IX_FILL_FACTOR = _MEM_FILL_FACTOR;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR = _MEM_EXPAND_FACTOR;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
	alloc_t alloc_record;
	unsigned used;
	unsigned allocated;
	struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
	size_t size;
	node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
	pool_t pool;
	node_pt node_heap;
	unsigned total_nodes;
	unsigned used_nodes;
	gap_pt gap_ix;
	unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;





/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
_mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
	size_t size,
	node_pt node);
static alloc_status
_mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
	size_t size,
	node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);
static node_pt _mem_find_unused_node(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
	// ensure that it's called only once until mem_free
	// allocate the pool store with initial capacity
	// note: holds pointers only, other functions to allocate/deallocate


	if (pool_store)
	{
		puts("mem_init() called has already been called.  pool_store has already been initialized.\n");
		return ALLOC_NOT_FREED;
	}

	pool_store = malloc(sizeof(pool_mgr_pt[_MEM_POOL_STORE_INIT_CAPACITY]));
	if (pool_store == NULL) {
		puts("mem_init(): Could not allocate pool store.\n");
		return ALLOC_FAIL;
	}
	
	pool_store_size = 0;
	pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;

	for (int i = 0; i < pool_store_capacity; i++)
		pool_store[i] = NULL;


	return ALLOC_OK;

}

alloc_status mem_free() {
	// ensure that it's called only once for each mem_init
	// make sure all pool managers have been deallocated
	// can free the pool store array
	// update static variables

	if (!pool_store)
		return ALLOC_CALLED_AGAIN;


	for (unsigned i = 0; i < pool_store_capacity; i++) {
		if (pool_store[i])
			free(pool_store[i]);
	}

	free(pool_store);
	pool_store = NULL;
	pool_store_size = 0;
	pool_store_capacity = 0;


	return ALLOC_OK;

}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {

	// make sure there the pool store is allocated
	if (!pool_store)
		return NULL;

	// expand the pool store, if necessary
	if ((float)pool_store_capacity / pool_store_size > MEM_FILL_FACTOR)
		_mem_resize_pool_store();


	// allocate a new mem pool mgr
	pool_mgr_pt pool_mgr = (pool_mgr_t*) malloc(sizeof(pool_mgr_t));

	// check success, on error return null
	if (pool_mgr == NULL) {
		puts("mem_pool_open(): Could not allocate pool manager.");
		return NULL;
	}

	//   initialize pool mgr
	pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
	pool_mgr->used_nodes = 1;
	pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;





	// allocate a new memory pool
	pool_t pool;
	pool.mem = (char*) malloc(sizeof(char) * size);

	// check success, on error deallocate mgr and return null
	if (pool.mem == NULL) {
		puts("mem_pool_open(): Could not allocate pool.");
		free(pool_mgr);
		return NULL;
	}

	// initialize metadata
	pool.policy = policy;
	pool.total_size = size;
	pool.alloc_size = 0;
	pool.num_allocs = 0;
	pool.num_gaps = 1;

	//   link pool mgr to pool store
	pool_mgr->pool = pool;






	// allocate a new node heap
	node_pt node_heap = (node_pt) malloc(sizeof(node_t[_MEM_NODE_HEAP_INIT_CAPACITY]));

	// check success, on error deallocate mgr/pool and return null
	if (!node_heap) {
		puts("mem_pool_open(): Could not allocate node heap.");
		free(pool_mgr->pool.mem);
		free(pool_mgr);
		return NULL;
	}


	// now to initialize each individual node
	//   initialize top node of node heap	node_heap[0].used = 1;
	node_heap[0].allocated = 0;
	node_heap[0].alloc_record.size = size;
	node_heap[0].alloc_record.mem = pool.mem;
	node_heap[0].next = NULL;
	node_heap[0].prev = NULL;

	// populate the middle of the array
	for (int i = 1; i < MEM_NODE_HEAP_INIT_CAPACITY; i++) {
		node_heap[i].used = 0;
		node_heap[i].allocated = 0;
		node_heap[i].next = NULL;
		node_heap[i].prev = NULL;
	}


	// it has been created and populated, save the node heap
	pool_mgr->node_heap = node_heap;





	// allocate a new gap index
	gap_pt gap_ix = malloc(sizeof(node_t[_MEM_GAP_IX_INIT_CAPACITY]));

	// check success, on error deallocate mgr/pool/heap and return null
	if (!gap_ix) {
		puts("Could not allocate gap index.");
		free(pool_mgr->pool.mem);
		free(pool_mgr->node_heap);
		free(pool_mgr);
		return NULL;
	}

	//   initialize top node of gap index
	gap_ix[0].size = size;
	gap_ix[0].node = pool_mgr->node_heap;

	// initialize the rest as no gaps
	for (int i = 1; i < MEM_GAP_IX_INIT_CAPACITY; i++) {
		gap_ix[i].size = 0;
		gap_ix[i].node = NULL;
	}

	// it's ready to be saved to the pool manager
	pool_mgr->gap_ix = gap_ix;






	// save to pool store
	// search for empty spot in pool store
	int x;
	for (x = 0; x < pool_store_size; x++) {
		if (pool_store[x] == NULL)
			break;  // all we have to do is break to preserve the value of x
	}

	// x will either be the index of an empty spot or one past the last to expand
	pool_store[x] = pool_mgr;

	// if it did not find an empty spot, it was tacked on to the end
	// we must increase the size
	if (x == pool_store_size)
		pool_store_size++;



	// return the address of the mgr, cast to (pool_pt)
	return (pool_pt) (pool_mgr);

}

alloc_status mem_pool_close(pool_pt pool) {
	// get mgr from pool by casting the pointer to (pool_mgr_pt)
	// check if this pool is allocated
	// check if pool has only one gap
	// check if it has zero allocations
	// free memory pool
	// free node heap
	// free gap index
	// find mgr in pool store and set to null
	// note: don't decrement pool_store_size, because it only grows
	// free mgr

	pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;


	if (pool->total_size <= 0) {
		// the pool's mem has not been initialized?
		// continue anyway, still try to delete dynamic memory if it's there
	}

	if (pool->num_gaps == 1) {
		// we still have to delete everything because memory has already been allocated, not sure why we should do these checks
	}

	if (pool->num_allocs == 0) {
		// we still have to delete everything because memory has already been allocated, not sure why we should do these checks
	}



	// free dynamic memory
	if (pool->mem)
		free(pool->mem);
	if (pool_mgr->node_heap)
		free(pool_mgr->node_heap);
	if (pool_mgr->gap_ix)
		free(pool_mgr->gap_ix);



	// remove pool_mgr from pool_store
	for (int i = 0; i < pool_store_size; i++) {
		if (pool_store[i] == pool_mgr) {
			pool_store[i] = NULL;
			break;
		}
	}

	// free dynamic memory
	free(pool_mgr);

	return ALLOC_OK;

}


alloc_pt mem_new_alloc(pool_pt pool, size_t size) {


	// size sanity check
	if (size > pool->total_size) {
		puts("mem_new_alloc(): Requested size is greater than the total pool size.");
		return NULL;
	}


	// get mgr from pool by casting the pointer to (pool_mgr_pt)
	pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;


	// check if any gaps, return null if none
	if (!pool->num_gaps) {
		puts("mem_new_alloc(): No gaps available.");
		return NULL;
	}


	// expand heap node, if necessary, quit on error
	if (_mem_resize_node_heap(pool_mgr) == ALLOC_FAIL) {
		puts("mem_new_alloc(): Could not resize heap pool.");
		return NULL;
	}


	// check used nodes fewer than total nodes, quit on error
	if (pool_mgr->used_nodes > pool_mgr->total_nodes)
	{
		puts("mem_new_alloc(): Unknown error: number of used nodes exceeds the total nodes.");
		return NULL;
	}




	// get a node for allocation:
	// if FIRST_FIT, then find the first sufficient node in the node heap
	// if BEST_FIT, then find the first sufficient node in the gap index
	node_pt node = NULL;

	if (pool->policy == FIRST_FIT) {


		// loop through node heap
		for (int i = 0; i < pool_mgr->total_nodes; i++) {

			node_pt n = &(pool_mgr->node_heap[i]);

			// we found a gap!
			if (n->used && (!n->allocated)) {
				// is it big enough?
				if (n->alloc_record.size >= size) {
					node = n;
					break;
				}
			}

		}


	}
	else if (pool->policy == BEST_FIT) {


		for (int i = 0; i < pool->num_gaps; i++) {

			gap_t gap = pool_mgr->gap_ix[i];

			// we found a gap! is it big enough?
			if (gap.size >= size) {
				node = gap.node;
				break;
			}

		}


	}
	else {

		puts("mem_new_alloc(): Unknown allocation policy.");
		return NULL;

	}





	// check if node found
	if (!node) {
		puts("mem_new_alloc(): Could not find a suitable node.");
		return NULL;
	}
	



	// Handle the remaining gap
	size_t new_gap = node->alloc_record.size - size;


	// adjust node heap:
	//   if remaining gap, need a new node
	if (new_gap) {

		//   find an unused one in the node heap
		node_pt new_node = _mem_find_unused_node(pool_mgr);

		//   make sure one was found
		if (!new_node) {
			puts("mem_new_alloc(): Could not find unused node.");
			return NULL;
		}


		//   initialize it to a gap node
		new_node->allocated = 0;
		new_node->used = 1;
		new_node->alloc_record.mem = node->alloc_record.mem + size;
		new_node->alloc_record.size = new_gap;
		new_node->next = node->next;
		new_node->prev = node;

		//   update linked list (new node right after the node for allocation)
		if (node->next)
			node->next->prev = new_node;
		node->next = new_node;
		

		//   update metadata (used_nodes)
		pool_mgr->used_nodes++;

	}
	





	// update gap list
	if (_mem_remove_from_gap_ix(pool_mgr, size, node) == ALLOC_FAIL) {
		puts("mem_new_alloc(): Could not update gap list.");
		return NULL;
	}





	// update metadata (num_allocs, alloc_size)
	node->used = 1;
	node->allocated = 1;
	node->alloc_record.size = size;

	pool->num_allocs++;
	pool->alloc_size += size;



	

	// return allocation record by casting the node to (alloc_pt)
	return (alloc_pt) node;

}


alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {

	// get mgr from pool by casting the pointer to (pool_mgr_pt)
	pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;
	// get node from alloc by casting the pointer to (node_pt)
	node_pt node = (node_pt)pool;

	node_pt node_to_delete = NULL;



	// find the node in the node heap
	for (int i = 0; i < pool_mgr->total_nodes; i++) {
		if (pool_mgr->node_heap[i].alloc_record.mem == alloc->mem) {
			node_to_delete = &(pool_mgr->node_heap[i]);
			break;
		}
	}

	// this is node-to-delete
	// make sure it's found
	if (!node_to_delete) {
		puts("mem_del_alloc(): Could not find node to delete allocation.");
		return ALLOC_FAIL;
	}

	// convert to gap node
	node_to_delete->allocated = 0;
	node_to_delete->used = 1;

	// update metadata (num_allocs, alloc_size)
	pool->num_allocs--;
	pool->alloc_size -= alloc->size;



	// if the next node in the list is also a gap, merge into node-to-delete
	/*
	if (next->next) {
	next->next->prev = node_to_del;
	node_to_del->next = next->next;
	} else {
	node_to_del->next = NULL;
	}
	next->next = NULL;
	next->prev = NULL;
	*/
	node_pt next_node = node_to_delete->next;
	if (next_node) {
		// next_node is also a gap
		if (next_node->used && !(next_node->allocated)) {

			//   remove the next node from gap index
			if (_mem_remove_from_gap_ix(pool_mgr, next_node->alloc_record.size, next_node) == ALLOC_FAIL) {
				puts("Could not remove next node from gap index.");
				return ALLOC_FAIL;
			}
			
			//   add the size to the node-to-delete
			node_to_delete->alloc_record.size += next_node->alloc_record.size;
			//   update linked list:
			node_to_delete->next = next_node->next;
			if (node_to_delete->next)
				node_to_delete->next->prev = node_to_delete;


			next_node->alloc_record.mem = NULL;
			next_node->alloc_record.size = 0;
			next_node->allocated = 0;
			//   update node as unused
			next_node->used = 0;
			next_node->next = NULL;
			next_node->prev = NULL;

			//   update metadata (used nodes)
			pool_mgr->used_nodes--;

		}
	}



	// this merged node-to-delete might need to be added to the gap index
	// but one more thing to check...
	// if the previous node in the list is also a gap, merge into previous!
	node_pt prev_node = node_to_delete->prev;
	if (prev_node) {
		if (prev_node->used && !(prev_node->allocated)) {

			//   remove the previous node from gap index
			if (_mem_remove_from_gap_ix(pool_mgr, prev_node->alloc_record.size, prev_node) == ALLOC_FAIL) {
				puts("Could not remove previous node from gap index.");
				return ALLOC_FAIL;
			}

			//   add the size of node-to-delete to the previous
			prev_node->alloc_record.size += node_to_delete->alloc_record.size;
			prev_node->next = node_to_delete->next;


			node_to_delete = prev_node;

			//   update metadata (used nodes)
			pool_mgr->used_nodes--;

		}
	}



	_mem_add_to_gap_ix(pool_mgr, node_to_delete->alloc_record.size, node_to_delete);

	return ALLOC_OK;

}


void mem_inspect_pool(pool_pt pool, pool_segment_pt *segments, unsigned *num_segments) {

	// get the mgr from the pool
	pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;

	// allocate the segments array with size == used_nodes
	pool_segment_pt segs = (pool_segment_t*)malloc(sizeof(pool_segment_t) * pool_mgr->used_nodes);

	// check successful
	if (segs == NULL) {
		puts("Could not inspect pool.  malloc() failed.");
		return NULL;
	}

	// loop through the node heap and the segments array
	//    for each node, write the size and allocated in the segment
	for (int i = 0; i < pool_mgr->used_nodes; i++) {
		segs[i].allocated = pool_mgr->node_heap[i].allocated;
		segs[i].size = pool_mgr->node_heap[i].alloc_record.size;
	}


	// "return" the values:
	*segments = segs;
	*num_segments = pool_mgr->used_nodes;
	
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {

	if (pool_store_capacity > 0) {
		if (((float)pool_store_size / (float)pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR) {

			size_t new_size = pool_store_capacity * sizeof(pool_t) * MEM_POOL_STORE_EXPAND_FACTOR;

			pool_store = (pool_pt*)realloc(pool_store, new_size);

			if (pool_store == NULL) {
				puts("Could not resize pool store.  realloc() failed.");
				return ALLOC_FAIL;
			}

		}
	}

	return ALLOC_OK;

}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {

	if (pool_mgr->total_nodes > 0) {
		if (((float)pool_mgr->used_nodes / (float)pool_mgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR) {

			size_t new_size = pool_mgr->total_nodes * sizeof(node_t) * MEM_NODE_HEAP_EXPAND_FACTOR;

			pool_mgr->node_heap = (node_pt*)realloc(pool_mgr->node_heap, new_size);

			if (pool_mgr->node_heap == NULL) {
				puts("Could not resize node heap.  realloc() failed.");
				return ALLOC_FAIL;
			}

		}
	}

	return ALLOC_OK;

}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
	// see above

	//check if current size is above the threshold
	if (((float)(pool_mgr->pool.num_gaps) / (float)(pool_mgr->gap_ix_capacity)) > MEM_GAP_IX_FILL_FACTOR) {
	
		size_t new_size = pool_mgr->gap_ix_capacity * sizeof(gap_t) * MEM_GAP_IX_EXPAND_FACTOR;
		pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, new_size);

		if (pool_mgr->gap_ix == NULL) {
			puts("Could not resize gap index.  realloc() failed.");
			return ALLOC_FAIL;
		}
	
	}

	
	return ALLOC_OK;

}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
	size_t size,
	node_pt node) {

	// expand the gap index, if necessary (call the function)
	if (_mem_resize_gap_ix == ALLOC_FAIL) {
		puts("Could not resize gap index.");
		return ALLOC_FAIL;
	}

	// add the entry at the end
	pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
	pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;


	// update metadata (num_gaps)
	pool_mgr->pool.num_gaps++;


	// sort the gap index (call the function)
	if (_mem_sort_gap_ix(pool_mgr) == ALLOC_FAIL) {
		puts("Could not sort gap index.");
		return ALLOC_FAIL;
	}

	// check success
	return ALLOC_OK;

}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
	size_t size,
	node_pt node) {
	// find the position of the node in the gap index
	// loop from there to the end of the array:
	//    pull the entries (i.e. copy over) one position up
	//    this effectively deletes the chosen node
	// update metadata (num_gaps)
	// zero out the element at position num_gaps!

	size_t new_gap = node->alloc_record.size - size;

	// update gap list
	for (int i = 0; i < pool_mgr->pool.num_gaps; i++) {

		node_pt n = pool_mgr->gap_ix[i].node;

			// compare memory addresses
		if (n->alloc_record.mem == node->alloc_record.mem) {

			// there is some remaining gap left, update the currently used gap index
			if (new_gap > 0) {

				pool_mgr->gap_ix[i].node = n->next;
				pool_mgr->gap_ix[i].size = n->next->alloc_record.size;

				/*
				// find new node to hold the new gap, the old is becoming allocated
				node_pt new_node = _mem_find_unused_node(pool_mgr);
				if (!new_node) {
					puts("mem_remove_from_gap_ix() : Could not find an unused node.");
					return ALLOC_FAIL;
				}

				// update metadata for new_node
				new_node->used = 1;
				new_node->allocated = 0;
				new_node->alloc_record.size = new_gap;
				new_node->alloc_record.mem = n->alloc_record.mem + size;

				//put it in the linked list node heap
				new_node->prev = n;
				new_node->next = n->next;
				n->next = new_node;

				// we have a new gap node
				pool_mgr->used_nodes++;

				// just slap it in the old gap's index, then sort
				pool_mgr->gap_ix[i].node = new_node;
				pool_mgr->gap_ix[i].size = new_gap;
				_mem_sort_gap_ix(pool_mgr);
				*/
			}
			else { // there is no remaining gap, remove from gap list

				for (int j = i; j < pool_mgr->pool.num_gaps; j++) {
					//pool_mgr->gap_ix[j] = pool_mgr->gap_ix[j + 1];
					pool_mgr->gap_ix[j].node = pool_mgr->gap_ix[j + 1].node;
					pool_mgr->gap_ix[j].size = pool_mgr->gap_ix[j + 1].size;
				}

				pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;
				pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;

				// decrease gap count
				pool_mgr->pool.num_gaps--;

			}


			return ALLOC_OK;

		}

	}


	puts("Could not remove gap from gap index.");
	return ALLOC_FAIL;

}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
	// the new entry is at the end, so "bubble it up"
	// loop from num_gaps - 1 until but not including 0:
	//    if the size of the current entry is less than the previous (u - 1)
	//       swap them (by copying) (remember to use a temporary variable)

	gap_pt gap_ix = pool_mgr->gap_ix;
	for (int i = pool_mgr->pool.num_gaps - 1; i > 0; i--) {

		if (gap_ix[i].size < gap_ix[i - 1].size) {

			// we found one that's out of order, bubble it down until it's in the correct position
			for (int j = i; j > 0; j--) {

				if (gap_ix[j].size < gap_ix[j - 1].size) {
					gap_t temp = gap_ix[j];
					gap_ix[j] = gap_ix[j - 1];
					gap_ix[j - 1] = temp;
				}
				else
					break;

			}

		}
		else
			break;
	
	}

	return ALLOC_OK;

}


static node_pt _mem_find_unused_node(pool_mgr_pt pool_mgr) {

	//   find an unused one in the node heap
	for (int i = 0; i < pool_mgr->total_nodes; i++) {
		if (!pool_mgr->node_heap[i].used)
			return &(pool_mgr->node_heap[i]);
	}

	return NULL;

}