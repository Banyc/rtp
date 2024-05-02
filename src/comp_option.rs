/// [`None`] is the smallest
#[derive(Debug, Clone, Copy)]
pub struct CompOption<T> {
    inner: Option<T>,
}
impl<T> CompOption<T> {
    pub fn new(value: Option<T>) -> Self {
        Self { inner: value }
    }

    pub fn set(&mut self, value: Option<T>) {
        self.inner = value;
    }

    #[allow(dead_code)]
    pub fn get(&self) -> Option<&T> {
        self.inner.as_ref()
    }
}
impl<T> PartialEq for CompOption<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}
impl<T> Eq for CompOption<T> where T: Eq {}
impl<T> PartialOrd for CompOption<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.inner, &other.inner) {
            (None, None) => Some(std::cmp::Ordering::Equal),
            (None, Some(_)) => Some(std::cmp::Ordering::Less),
            (Some(_), None) => Some(std::cmp::Ordering::Greater),
            (Some(a), Some(b)) => a.partial_cmp(b),
        }
    }
}
impl<T> Ord for CompOption<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.inner, &other.inner) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => a.cmp(b),
        }
    }
}
