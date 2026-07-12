// BinEntry is only read only traversals
// which sort of walks all the nodes

use std::{cell::UnsafeCell, sync::atomic::Ordering};

use crossbeam::epoch::{Atomic, Guard, Shared};

pub(crate) enum BinEntry<K,V> 
where K: Eq
{ 
    Node(Node<K,V>)
} 


pub(crate) struct Node<K,V> {
    pub(crate) hash: u64, 
    pub(crate) key: K,
    // this gives interior mutablility of value
    pub(crate) value: UnsafeCell<V>,
    pub(crate) next: Atomic<Node<K,V>>
}


impl<K,V> BinEntry<K,V> 
where K: Eq
{ 
    pub(crate) fn find<'g>(&self, key: &K, hash: u64, guard: &'g Guard) -> Shared<'g, Node<K,V>> { 
        match *self {
            BinEntry::Node(ref n) => { 
                loop { 
                    let mut node = n; 
                    if node.hash == hash && &node.key == key { 
                        return Shared::from(node as *const _)
                    }
                    let next = node.next.load(Ordering::SeqCst, guard);
                    if next.is_null() { 
                        return Shared::null();
                    }
                    let next_node = unsafe { &*(next.as_raw() as *const Node<K,V>) };
                    node = next_node;
                }
            }
        }
    }
}