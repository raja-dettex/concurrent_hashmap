use std::{hash::{BuildHasher, Hash, Hasher, RandomState}, sync::atomic::Ordering};

use crossbeam::epoch::{Atomic, CompareExchangeError, Guard, Owned, Shared};
use parking_lot::{Mutex, lock_api::RawMutex};

use crate::node::{BinEntry, Node};

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
    fn hash(&self, key: &K) -> u64
    {
        let mut hasher = self.build_hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }
    fn get<'g>(&'g self, key: &K, guard: &'g Guard) -> Option<Shared<'g, V>> { 
        let shared_table = self.table.load(Ordering::SeqCst, guard);
        let hash = self.hash(key);
        if shared_table.is_null() { 
            return None;
        }

        // the safety here is because the shared pointer which is protected by epoch GC
        // and the guard is valid so its safe to unalign the tag and get the raw pointer because
        // because the memory is not yet reclaimed to the allocator yet
        let table = unsafe { &*(shared_table.as_raw() as *const Table<K,V>) };
        let bini = table.bini(hash);
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

    // pub fn insert(&self, key: K, value: V) -> Option<V> { 

    // }

    pub fn put(&self, key: K, value: V, no_replacement: bool) -> Option<()>{ 
        let hash = self.hash(&key);
        let guard = &crossbeam::epoch::pin();
        let shared_table = self.table.load(Ordering::SeqCst, guard);
        let mut new_node = Owned::new(BinEntry::Node(Node { 
            key,
            value: Atomic::new(value),
            hash, 
            next: Atomic::null(),
            lock: Mutex::new(())
        }));
        loop { 
            let shared_table = self.table.load(Ordering::SeqCst, guard);
            if shared_table.is_null() {
                self.init_table();
                continue;
            }
            let table = unsafe { &*(shared_table.as_raw())};
            let bini = table.bini(hash);
            let mut shared_bin = table.at(bini, guard);
            if shared_bin.is_null() { 
                // fast path - shared bin is empty stick us to the head of the bin
                // create the bin and just do compare and set
                match table.cas_at(bini, shared_bin, new_node, guard) {
                    Ok(garbage_now) => assert!(garbage_now.is_null()),
                    Err(changed) => { 
                        assert!(!changed.current.is_null());
                        new_node = changed.new;
                        shared_bin = changed.current;
                    }
                }
            } 
            // slow path: bin exists so create the node and stick the node to bin linked list
            let bin = unsafe { &*(shared_bin.as_raw())};
            match *bin {
                BinEntry::Moved(next_table) => table.help_transfer(next_table),
                BinEntry::Node(ref head) if no_replacement && head.hash == hash && head.key == key => {
                        // replacement are disallowed and bin matches with the first
                        return Some(());
                },
                BinEntry::Node(ref head )=> {
                    let _guard = head.lock.lock();
                    // need to check that this is still the head
                    let current_head = table.at(bini as usize, guard);
                    if current_head.as_raw() != shared_bin.as_raw() {
                        // no - try again from the start
                        continue;
                    }
                    

                    // yes it is still the head so we now can 'Own' the bin
                    // owning here means there is still readers looking into it
                    

                    // TODO: TreeBin and Reservations
                    let mut bin_count = 1;
                    let mut n = head;
                    let old_val = loop { 
                        if n.hash == hash && n.key == key {
                            // the key already exists in the map 
                            if no_replacement { 
                                // dont update
                            } else { 
                                let now_garbage = n.value.swap(Owned::new(value), Ordering::SeqCst, guard);
                                // we dont need to immediately drop the guard and reclaim cause there might still be readers
                                // so instead we should defer destroy and how we should do it is still unclear
                                break Some(());
                            }

                            let shared_next = n.next.load(Ordering::SeqCst, guard);
                            if shared_next.is_null() {
                                // still stick here
                                let next = unsafe { *(shared_next.as_raw())};
                                next.value.store(Owned::new(value), Ordering::SeqCst);
                                break None;
                            }
                        }
                    };
                }
            }
        }
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
    pub fn bini(&self, hash: u64) -> usize{ 
        let mask = (self.bins.len() - 1) as u64;
        (hash & mask) as usize
    }
    #[inline]
    fn at<'g>(&'g self, index: usize, guard: &'g Guard) -> Shared<'g, BinEntry<K,V>> { 
        self.bins[index].load(Ordering::SeqCst, guard)
    }

    fn cas_at<'g>(
        &'g self,
        index: usize,
        old_shared: Shared<'g, BinEntry<K,V>>,
        new_node: Owned<BinEntry<K,V>>,
        guard: &'g Guard
    ) -> Result<Shared<'g, BinEntry<K,V>>, CompareExchangeError<'g, BinEntry<K,V>, Owned<BinEntry<K,V>>>>
    {
        self.bins[index].compare_exchange(old_shared, new_node, Ordering::SeqCst, Ordering::SeqCst, guard)
    }
}