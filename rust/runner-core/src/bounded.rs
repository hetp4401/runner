use std::collections::VecDeque;

/// Limits both item count and retained payload bytes, including when one item
/// is unusually large. Callers account for serialized data, not pointer size.
pub struct ByteQueue<T> {
    items: VecDeque<(usize, T)>,
    bytes: usize,
    max_items: usize,
    max_bytes: usize,
}
impl<T> ByteQueue<T> {
    pub fn new(max_items: usize, max_bytes: usize) -> Self {
        Self {
            items: VecDeque::new(),
            bytes: 0,
            max_items,
            max_bytes,
        }
    }
    pub fn push(&mut self, value: T, size: usize) -> bool {
        if size > self.max_bytes || self.max_items == 0 {
            return false;
        }
        while self.items.len() >= self.max_items || self.bytes > self.max_bytes - size {
            self.pop();
        }
        self.bytes += size;
        self.items.push_back((size, value));
        true
    }
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop_front().map(|(size, item)| {
            self.bytes -= size;
            item
        })
    }
    pub fn retain(&mut self, mut predicate: impl FnMut(&T) -> bool) {
        let bytes = &mut self.bytes;
        self.items.retain(|(size, item)| {
            if predicate(item) {
                true
            } else {
                *bytes -= size;
                false
            }
        });
    }
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter().map(|(_, v)| v)
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_even_for_large_items_and_pruning() {
        let mut queue = ByteQueue::new(4, 10);
        for n in 0..10_000 {
            assert!(queue.push(n, 3));
            assert!(queue.bytes() <= 10);
        }
        assert_eq!(
            queue.iter().copied().collect::<Vec<_>>(),
            [9997, 9998, 9999]
        );
        assert!(!queue.push(0, 11));
        queue.retain(|n| *n == 9999);
        assert_eq!(queue.bytes(), 3);
        queue.pop();
        assert_eq!(queue.bytes(), 0);
    }
}
