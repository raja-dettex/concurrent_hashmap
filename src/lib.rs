use std::{hash::{BuildHasher, Hash, Hasher, RandomState}, sync::atomic::Ordering};

use crossbeam::epoch::{Atomic, Guard, Shared};

use crate::node::BinEntry;

/// The largest possible table capacity.  This value must be
/// exactly 1<<30 to stay within Java array allocation and indexing
/// bounds for power of two table sizes, and is further required
/// because the top two bits of 32bit hash fields are used for
/// control purposes.
const MAXIMUM_CAPACITY: usize = 1 << 30; // TODO: use ISIZE_BITS

/// The default initial table capacity.  Must be a power of 2
/// (i.e., at least 1) and at most MAXIMUM_CAPACITY.
const DEFAULT_CAPACITY: usize = 16;

/// The bin count threshold for using a tree rather than list for a bin. Bins are
/// converted to trees when adding an element to a bin with at least this many
/// nodes. The value must be greater than 2, and should be at least 8 to mesh
/// with assumptions in tree removal about conversion back to plain bins upon
/// shrinkage.
/// 

const TREEIFY_THRESHOLD: usize = 8;
const LOAD_FACTOR: f64 = 0.75;
/// The bin count threshold for untreeifying a (split) bin during a resize
/// operation. Should be less than TREEIFY_THRESHOLD, and at most 6 to mesh with
/// shrinkage detection under removal.
const UNTREEIFY_THRESHOLD: usize = 6;

/// The smallest table capacity for which bins may be treeified. (Otherwise the
/// table is resized if too many nodes in a bin.) The value should be at least 4
/// * TREEIFY_THRESHOLD to avoid conflicts between resizing and treeification
/// thresholds.
const MIN_TREEIFY_CAPACITY: usize = 64;

/// Minimum number of rebinnings per transfer step. Ranges are
/// subdivided to allow multiple resizer threads.  This value
/// serves as a lower bound to avoid resizers encountering
/// excessive memory contention.  The value should be at least
/// DEFAULT_CAPACITY.
const MIN_TRANSFER_STRIDE: usize = 16;

/// The number of bits used for generation stamp in size_ctl.
/// Must be at least 6 for 32bit arrays.
/// 
/// 

const RESIZE_STAMP_BITS: usize = 16;

const MAX_RESIZERS: usize = ( 1 << (32 - RESIZE_STAMP_BITS)) - 1;

const RESIZE_STAMP_SHIFT: usize = 32 - RESIZE_STAMP_BITS;


mod node;
/*
// concurrent hash map is a heap allocation of the table
// and every bin is a also heap allocation and we have the atomic pointer to the address in memory

// the reason why we are doing this is because if the hash map is resized we will be 
// to swap the atomic ptr of the memory address provided by the allocator atomically
// and thus we need atomic ptr to sort of give the experience of lock free data structure.
// so anyone tries to get a node by a given key we will make sure the bin is not dropped 
// and the memory is not reclaimed as we are pinning every thread on to guard and the shared access 
// of the pointer is guarded access which are pinned to threads participating in accessing the pointer
// the guard here makes sure defer destroy [ see epoch based memory reclaimation for more info]
// and so when the guard will be dropped it will be added to garbage list

*/

pub struct ConcurrentHashMap<K,V, S = RandomState> 
where K: Eq + Hash
{ 
    build_hasher: S,
    table: Atomic<Table<K,V>>    
}

impl<K,V, S> ConcurrentHashMap<K,V, S> 
where 
    K: Eq + Hash,
    S: BuildHasher
{ 

    #[inline]
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>> { 
        let shared_table = self.table.load(Ordering::SeqCst, guard);
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        if shared_table.is_null() { 
            return None;
        }

        // the safety here is because the shared pointer which is protected by epoch GC
        // and the guard is valid so its safe to unalign the tag and get the raw pointer because
        // because the memory is not yet reclaimed to the allocator yet
        let table = unsafe { &*(shared_table.as_raw() as *const Table<K,V>) };
        let mask = (table.bins.len() - 1) as u64;
        let bini = hash & mask;
        let shared_bin = table.at(bini as usize, guard);
        if shared_bin.is_null() {
            return None;
        }
        let bin = unsafe { &*(shared_bin.as_raw())}; 
        let shared_node = bin.find(key, hash, guard);
        if shared_node.is_null() {
            return None;
        }
        let node = unsafe { &*(shared_node.as_raw())};
        let value = node.value.load(Ordering::SeqCst, guard);
        Some(value)        
    }


    pub fn get_and<R, F: FnOnce(&V) -> R>(&self, key: &K, then: F) -> Option<R> {
        let guard = crossbeam::epoch::pin();
        self.get(key, &guard).map(|v| then(unsafe { &*(v.as_raw() )} ))
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> { 
        
    }
}

pub(crate) struct Table<K,V> 
where K: Eq
{ 
    bins: [Atomic<BinEntry<K,V>>; 100]
}


impl<K,V>  Table<K,V> 
where K: Eq
{
    #[inline]
    fn at<'g>(&'g self, index: usize, guard: &'g Guard) -> Shared<'g, BinEntry<K,V>> { 
        self.bins[index].load(Ordering::SeqCst, guard)
    }
}