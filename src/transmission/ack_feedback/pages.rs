use crate::ack::MAX_ACK_BLOCKS;

pub(crate) const MAX_NUM_ACK: usize = MAX_ACK_BLOCKS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckPage {
    pub(crate) first_block_index: usize,
    /// How many blocks this page can carry. The head page always carries a
    /// full page (`MAX_NUM_ACK`); the deep page is capped by its planned
    /// tail length (`history_count - first_block_index`), so a short tail is
    /// never billed as a full page in the flush-completion accounting.
    pub(crate) max_blocks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AckPagePlan {
    pages: [Option<AckPage>; 2],
    len: usize,
    cursor: usize,
    history_count: usize,
    next_cursor: usize,
}

impl AckPagePlan {
    pub(super) fn new(cursor: usize, history_count: usize) -> Self {
        let cursor = cursor.max(MAX_NUM_ACK).min(history_count);
        let deep = (cursor < history_count).then_some(AckPage {
            first_block_index: cursor,
            max_blocks: (history_count - cursor).min(MAX_NUM_ACK),
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
            cursor,
            history_count,
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

    /// Sum of the block capacities of the first `pages_sent` pages (in wire
    /// order): the blocks this flush actually placed on the wire. The head
    /// page contributes a full page; the deep page contributes its planned
    /// tail length, so a short tail page never counts as `MAX_NUM_ACK`.
    pub(super) fn conveyed_blocks(&self, pages_sent: usize) -> usize {
        debug_assert!(pages_sent <= self.len);
        self.pages
            .iter()
            .flatten()
            .take(pages_sent)
            .map(|page| page.max_blocks)
            .sum()
    }

    /// Deep-walk cursor for the claim following this one after `pages_sent`
    /// pages went out: a full flush lands on the planned `next_cursor`, a
    /// partial flush (head delivered, deep page blocked) advances one deep
    /// page so the re-flush plans the NEXT deep slice instead of
    /// re-attempting the same one, and nothing sent leaves the cursor where
    /// it was.
    pub(super) fn cursor_after(&self, pages_sent: usize) -> usize {
        debug_assert!(pages_sent <= self.len);
        if pages_sent == self.len {
            self.next_cursor
        } else if pages_sent == 0 {
            self.cursor
        } else {
            debug_assert_eq!(pages_sent, 1);
            next_page_cursor(self.cursor, self.history_count, MAX_NUM_ACK)
        }
    }
}

fn next_page_cursor(cursor: usize, history_count: usize, max_blocks: usize) -> usize {
    if cursor + max_blocks < history_count {
        cursor + max_blocks
    } else {
        max_blocks
    }
}
