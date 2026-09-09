//! An allocator that counts bytes.
//!
//! Resident set size is not a usable instrument here: the process builds a 2 GB batch for the
//! naive layout early on, frees it, and the allocator keeps those pages mapped. Every later
//! allocation reuses them, so RSS stays flat even if a query materialises a gigabyte. Counting
//! allocations directly is immune to that.
//!
//! The cost is two atomics per allocation, paid identically by every layout, so comparisons
//! between them stay honest even though absolute timings are slightly inflated.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static IN_USE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

pub struct Counting;

impl Counting {
    /// Bytes currently allocated and not yet freed.
    pub fn in_use() -> usize {
        IN_USE.load(Ordering::Relaxed)
    }

    /// Start a fresh measurement from the current level.
    pub fn reset_peak() {
        PEAK.store(IN_USE.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// How far allocation rose above `baseline` since the last reset.
    pub fn peak_above(baseline: usize) -> usize {
        PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
    }
}

fn record_alloc(size: usize) {
    let now = IN_USE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() {
            IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
            record_alloc(new_size);
        }
        out
    }
}
