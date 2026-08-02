use core::fmt;
use std::io::{self, ErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoErr {
    kind: ErrorKind,
    raw_os_error: Option<i32>,
}

impl IoErr {
    pub fn new(kind: ErrorKind, raw_os_error: Option<i32>) -> Self {
        Self { kind, raw_os_error }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn raw_os_error(&self) -> Option<i32> {
        self.raw_os_error
    }

    pub fn with_kind(self, kind: ErrorKind) -> Self {
        Self { kind, ..self }
    }
}

impl From<io::Error> for IoErr {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }
}

impl From<ErrorKind> for IoErr {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            raw_os_error: None,
        }
    }
}

impl From<IoErr> for io::Error {
    fn from(error: IoErr) -> Self {
        let Some(errno) = error.raw_os_error else {
            return io::Error::from(error.kind);
        };
        let os = io::Error::from_raw_os_error(errno);
        match os.kind() == error.kind {
            true => os,
            false => io::Error::new(error.kind, os),
        }
    }
}

impl PartialEq<ErrorKind> for IoErr {
    fn eq(&self, other: &ErrorKind) -> bool {
        self.kind == *other
    }
}

impl PartialEq<IoErr> for ErrorKind {
    fn eq(&self, other: &IoErr) -> bool {
        other.kind == *self
    }
}

impl fmt::Display for IoErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.raw_os_error {
            Some(errno) => write!(
                f,
                "{} (os error {errno})",
                io::Error::from_raw_os_error(errno)
            ),
            None => write!(f, "{}", self.kind),
        }
    }
}

impl std::error::Error for IoErr {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_uncategorized_errno_still_names_itself() {
        let raw = io::Error::from_raw_os_error(39);
        let err = IoErr::from(raw);
        assert_eq!(err.raw_os_error(), Some(39));
        assert!(
            err.to_string().contains("os error 39"),
            "an errno must survive into the message, got {err}"
        );
    }

    #[test]
    fn a_kind_with_no_syscall_behind_it_carries_no_errno() {
        let err = IoErr::from(ErrorKind::WouldBlock);
        assert_eq!(err.raw_os_error(), None);
        assert_eq!(err, ErrorKind::WouldBlock);
        assert_eq!(err.to_string(), ErrorKind::WouldBlock.to_string());
    }

    #[test]
    fn reclassifying_keeps_the_errno_that_explains_it() {
        let enobufs = IoErr::from(io::Error::from_raw_os_error(55));
        let retryable = enobufs.with_kind(ErrorKind::WouldBlock);
        assert_eq!(retryable.kind(), ErrorKind::WouldBlock);
        assert_eq!(
            retryable.raw_os_error(),
            Some(55),
            "the reclassified errno must still be readable in a log"
        );
    }

    #[test]
    fn a_round_trip_through_io_error_preserves_the_errno() {
        let err = IoErr::from(io::Error::from_raw_os_error(55));
        let round_tripped = IoErr::from(io::Error::from(err));
        assert_eq!(round_tripped, err);
    }

    #[test]
    fn converting_a_reclassified_error_keeps_the_new_kind() {
        let normalized =
            IoErr::from(io::Error::from_raw_os_error(55)).with_kind(ErrorKind::WouldBlock);
        let as_io = io::Error::from(normalized);
        assert_eq!(as_io.kind(), ErrorKind::WouldBlock);
        assert!(
            as_io.to_string().contains("os error 55"),
            "the reclassified errno must stay in the message, got {as_io}",
        );
    }
}
