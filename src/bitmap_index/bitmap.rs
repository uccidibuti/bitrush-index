// This code is released under the
// General Public License (GPL), version 3
// http://www.gnu.org/licenses/gpl-3.0.en.html
// (c) Lorenzo Vannucci

//! # Bitmap
//!
//! A Trait that define the bitmap methods. If you want use you custom compressed-bitmap
//! in [`bitrush_index`] you must implement this trait for your bitmap.
//!
//! [`bitrush_index`]: ../lib.rs

use std::ops::BitAnd;
use std::fmt::Debug;

pub trait Bitmap
where Self: Clone + Debug, for <'a> &'a Self: BitAnd<&'a Self,Output=Self> {

    /// Return a new bitmap.
    fn new() -> Self;

    /// Set the ith bit (starting from zero).
    fn set(&mut self, i: u32);

    /// Return a Vec with all bit set positions. 
    fn unroll_bitmap(&self) -> Vec<u32>;

    /// Return the effective size to serialize the bitmap.
    fn size(&self) -> usize;

    /// Write bitamp content into buffer_out and return the numbers of bytes written.
    /// Return a generic error if buffer_out size is less then bitmap size. 
    fn write_to_buffer(&self, buffer_out: &mut [u8]) -> Result<usize, ()>;

    /// Read bitmap content from buffer_in, buffer_in must have the effettive
    /// bitmap content (the size returned from write_to_buffer method).
    /// If `check_bitmap == false` bitmap content is readed without
    /// any check on bitmap content integrity. Return a generic error an error occur.
    fn read_from_buffer(&mut self, buffer_in: &[u8], check_bitmap: bool) -> Result<(), ()>;
}

