use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct SharedCell<T> {
    value: Arc<Mutex<T>>,
}
impl<T> SharedCell<T>
where
    T: Clone,
{
    pub fn new(value: T) -> Self {
        Self {
            value: Arc::new(Mutex::new(value)),
        }
    }

    pub fn set(&self, value: T) {
        *self.value.lock().unwrap() = value;
    }

    pub fn get(&self) -> T {
        self.value.lock().unwrap().clone()
    }
}
