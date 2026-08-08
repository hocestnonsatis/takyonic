//! Fixed-size page abstraction for the Buffer Pool Manager.
//!
//! Each [`Page`] holds a page-aligned byte frame plus metadata used by the BPM
//! (`page_id`, `pin_count`, `dirty`).

use std::fmt;

/// Logical page identifier (offset = `page_id * page_size` in the page file).
pub type PageId = u64;

/// Invalid / empty page id.
pub const INVALID_PAGE_ID: PageId = u64::MAX;

/// Default BPM page size (4 KiB) — must match O_DIRECT alignment on Linux.
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// In-memory page frame with pin / dirty metadata.
#[derive(Debug)]
pub struct Page {
    /// Logical page id currently occupying this frame (`INVALID_PAGE_ID` if free).
    pub page_id: PageId,
    /// Raw page bytes (exactly `page_size` long, page-size aligned).
    data: Box<[u8]>,
    /// Number of active pins; must be zero before eviction.
    pub pin_count: u32,
    /// Whether the frame has been mutated since the last flush.
    pub dirty: bool,
}

impl Page {
    /// Allocate a zeroed, page-size-aligned frame.
    pub fn new_aligned(page_size: usize) -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            data: aligned_zeroed(page_size),
            pin_count: 0,
            dirty: false,
        }
    }

    /// Page size in bytes.
    #[inline]
    pub fn page_size(&self) -> usize {
        self.data.len()
    }

    /// Immutable view of the page bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Mutable view of the page bytes (caller should mark dirty).
    #[inline]
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Reset frame to an empty state (keeps allocation).
    pub fn reset(&mut self) {
        self.page_id = INVALID_PAGE_ID;
        self.pin_count = 0;
        self.dirty = false;
        self.data.fill(0);
    }

    /// Whether this frame currently holds a valid page.
    #[inline]
    pub fn is_occupied(&self) -> bool {
        self.page_id != INVALID_PAGE_ID
    }
}

impl fmt::Display for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Page(id={}, pins={}, dirty={})",
            self.page_id, self.pin_count, self.dirty
        )
    }
}

/// Allocate `len` bytes aligned to `len` (for O_DIRECT page frames).
fn aligned_zeroed(len: usize) -> Box<[u8]> {
    use std::alloc::{Layout, alloc_zeroed};
    assert!(len.is_power_of_two(), "page size must be a power of two");
    let layout = Layout::from_size_align(len, len).expect("valid page layout");
    unsafe {
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_is_aligned() {
        let page = Page::new_aligned(DEFAULT_PAGE_SIZE);
        let addr = page.data().as_ptr() as usize;
        assert_eq!(addr % DEFAULT_PAGE_SIZE, 0);
        assert_eq!(page.page_size(), DEFAULT_PAGE_SIZE);
    }
}
