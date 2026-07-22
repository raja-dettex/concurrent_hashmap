// BinEntry is only read only traversals
// which sort of walks all the nodes

use std::{cell::UnsafeCell, sync::atomic::Ordering, todo};
use parking_lot::{Mutex, RawMutex};
use crossbeam::epoch::{Atomic, Guard, Shared};

use crate::Table;

pub(crate) enum BinEntry<K,V> 
where K: Eq
{ 
    Node(Node<K,V>),
    Moved(*const Table<K,V>)
} 


pub(crate) struct Node<K,V> 
where K: Eq
{
    pub(crate) hash: u64, 
    pub(crate) key: K,
    // this gives interior mutablility of value
    pub(crate) value: Atomic<V>,
    pub(crate) next: Atomic<Node<K,V>>,
    pub(crate) lock: Mutex<()>
}


impl<K,V> Node<K,V> 
where K: Eq
{ 
    pub(crate) fn find<'g>(&self, key: &K, hash: u64, guard: &'g Guard) -> Shared<'g, Node<K,V>> {
        if self.hash == hash && &self.key == key { 
            return Shared::from(self as *const _)
        }
        let next = self.next.load(Ordering::SeqCst, guard);
        if next.is_null() { 
            return Shared::null();
        }
        let next_node = unsafe { &*(next.as_raw() as *const BinEntry<K,V>) };
        next_node.find(key, hash, guard) 
    }
}


impl<K,V> BinEntry<K,V> 
where K: Eq
{ 
    pub(crate) fn find<'g>(&self, key: &K, hash: u64, guard: &'g Guard) -> Shared<'g, Node<K,V>> { 
        match *self {
            BinEntry::Node(ref n) => {                 
                return n.find(key, hash, guard);
            },
            BinEntry::Moved(_next_table) => todo!()
        }
    }
}