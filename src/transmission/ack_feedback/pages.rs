use crate::ack::MAX_ACK_BLOCKS;

pub(crate) const MAX_NUM_ACK: usize = MAX_ACK_BLOCKS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckPage {
    pub(crate) first_block_index: usize,
    pub(crate) max_blocks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AckPagePlan {
    pages: [Option<AckPage>; 2],
    len: usize,
    next_cursor: usize,
}

impl AckPagePlan {
    pub(super) fn new(cursor: usize, history_count: usize) -> Self {
        let cursor = cursor.max(MAX_NUM_ACK).min(history_count);
        let deep = (cursor < history_count).then_some(AckPage {
            first_block_index: cursor,
            max_blocks: MAX_NUM_ACK,
        });
        let len = 1 + usize::from(deep.is_some());
        Self {
            pages: [
                Some(AckPage {
                    first_block_index: 0,
                    max_blocks: MAX_NUM_ACK,
                }),
                deep,
            ],
            len,
            next_cursor: deep.map_or(MAX_NUM_ACK, |_| {
                next_page_cursor(cursor, history_count, MAX_NUM_ACK)
            }),
        }
    }

    pub(super) fn pages(self) -> [Option<AckPage>; 2] {
        self.pages
    }

    pub(super) fn len(self) -> usize {
        self.len
    }

    pub(super) fn next_cursor(self) -> usize {
        self.next_cursor
    }
}

fn next_page_cursor(cursor: usize, history_count: usize, max_blocks: usize) -> usize {
    if cursor + max_blocks < history_count {
        cursor + max_blocks
    } else {
        max_blocks
    }
}
