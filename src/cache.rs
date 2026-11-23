#![allow(dead_code)]

use std::collections::HashMap;
use std::hash::Hash;

struct Node<K, V> {
    key: K,
    value: V,
    bytes: usize,
    prev: Option<usize>,
    next: Option<usize>,
}

pub struct LruCache<K, V> {
    max_bytes: usize,
    used_bytes: usize,
    map: HashMap<K, usize>,
    nodes: Vec<Option<Node<K, V>>>,
    free: Vec<usize>,
    head: Option<usize>, // MRU
    tail: Option<usize>, // LRU
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            used_bytes: 0,
            map: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        let idx = *self.map.get(key)?;
        self.move_to_front(idx);
        self.nodes[idx].as_ref().map(|n| &n.value)
    }

    pub fn insert(&mut self, key: K, value: V, bytes: usize) {
        if let Some(existing) = self.remove(&key) {
            drop(existing);
        }
        if self.max_bytes == 0 || bytes > self.max_bytes {
            return;
        }
        while self.used_bytes.saturating_add(bytes) > self.max_bytes {
            if !self.evict_lru() {
                break;
            }
        }
        let idx = if let Some(slot) = self.free.pop() {
            self.nodes[slot] = Some(Node {
                key: key.clone(),
                value,
                bytes,
                prev: None,
                next: self.head,
            });
            slot
        } else {
            let slot = self.nodes.len();
            self.nodes.push(Some(Node {
                key: key.clone(),
                value,
                bytes,
                prev: None,
                next: self.head,
            }));
            slot
        };
        if let Some(old_head) = self.head {
            self.nodes[old_head].as_mut().unwrap().prev = Some(idx);
        } else {
            self.tail = Some(idx);
        }
        self.head = Some(idx);
        self.used_bytes += bytes;
        self.map.insert(key, idx);
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let idx = self.map.remove(key)?;
        let node = self.unlink_and_take(idx);
        self.used_bytes -= node.bytes;
        Some(node.value)
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.nodes.clear();
        self.free.clear();
        self.head = None;
        self.tail = None;
        self.used_bytes = 0;
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    fn move_to_front(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        let (prev, next) = {
            let n = self.nodes[idx].as_ref().unwrap();
            (n.prev, n.next)
        };
        if let Some(p) = prev {
            self.nodes[p].as_mut().unwrap().next = next;
        }
        if let Some(nx) = next {
            self.nodes[nx].as_mut().unwrap().prev = prev;
        } else {
            self.tail = prev;
        }
        if let Some(old_head) = self.head {
            self.nodes[old_head].as_mut().unwrap().prev = Some(idx);
        }
        let n = self.nodes[idx].as_mut().unwrap();
        n.prev = None;
        n.next = self.head;
        self.head = Some(idx);
    }

    fn unlink_and_take(&mut self, idx: usize) -> Node<K, V> {
        let node = self.nodes[idx].take().unwrap();
        if let Some(p) = node.prev {
            self.nodes[p].as_mut().unwrap().next = node.next;
        } else {
            self.head = node.next;
        }
        if let Some(nx) = node.next {
            self.nodes[nx].as_mut().unwrap().prev = node.prev;
        } else {
            self.tail = node.prev;
        }
        self.free.push(idx);
        node
    }

    fn evict_lru(&mut self) -> bool {
        let Some(tail_idx) = self.tail else {
            return false;
        };
        let node = self.unlink_and_take(tail_idx);
        self.map.remove(&node.key);
        self.used_bytes -= node.bytes;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recently_used_within_byte_budget() {
        let mut cache = LruCache::new(100);
        cache.insert(1u64, vec![1u8; 40], 40);
        cache.insert(2u64, vec![2u8; 40], 40);
        assert!(cache.get(&1).is_some()); // 1 is now MRU, 2 is LRU
        cache.insert(3u64, vec![3u8; 40], 40); // exceeds 100 -> evicts 2
        assert!(cache.get(&2).is_none());
        assert!(cache.get(&1).is_some());
        assert!(cache.get(&3).is_some());
        assert_eq!(cache.used_bytes(), 80);
        cache.clear();
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn zero_budget_or_oversized_entry_is_not_cached() {
        let mut cache = LruCache::new(32);
        cache.insert(1u64, vec![0u8; 64], 64);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_bytes(), 0);
    }
}
