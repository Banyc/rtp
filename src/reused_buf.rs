#[derive(Debug, Clone)]
pub struct ReusedBuf<T> {
    pool: Vec<T>,
}
impl<T> ReusedBuf<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            pool: Vec::with_capacity(capacity),
        }
    }

    pub fn return_buf(&mut self, buf: T) {
        if self.pool.len() == self.pool.capacity() {
            return;
        }
        self.pool.push(buf);
    }

    pub fn reuse_buf(&mut self) -> Option<T> {
        self.pool.pop()
    }
}
