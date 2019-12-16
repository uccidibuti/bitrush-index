// This code is released under the
// General Public License (GPL), version 3
// http://www.gnu.org/licenses/gpl-3.0.en.html
// (c) Lorenzo Vannucci

//! # OZBCBitmap
//!
//! A compressed bitmap for bitmap indexes.
//!
//! # Encoding
//! OZBCBitmap encodes bits in 16bits words. There are two types of words:
//!
//!  0:  |1bit word_type=0|7bit    bytes_zero|8bit    dirty_byte|
//!  1:  |1bit word_type=1|         15bit 128_bytes_zero        |
//!
//! Where:
//! - bytes_zero = number of consecutive sequence of 8bit zeros.
//! - dirty_byte = uncompressed 8bit.
//! - 128_bytes_zero = number of consecutive sequence of 1024bit of zeros.
//!
//! Note:
//! - The max size of this compressed bitmap is twice the size of the same uncompressed bitmap.
//! - The max number of consecutive zero bits that can be rapresented from
//! a single word is ((2^15) - 1) * (2^10) = (2^25 - 2^10) bits.
//!
//! A older version of OZBCBitmap encoding: https://github.com/uccidibuti/OZBCBitmap .

/// [`Debug`]: https://doc.rust-lang.org/std/fmt/trait.Debug.html
/// [`BitAnd`]: https://doc.rust-lang.org/std/ops/trait.BitAnd.html
/// [`Bitmap`]: ../bitmap_index/bitmap.rs
/// [`BitmapIndex`]: ../bitmap_index/mod.rs

use std::mem;
use std::ops::{BitAnd};
use std::result::Result;
use crate::bitmap_index::Bitmap;

const OZBC_MAX_128_BYTES_ZERO: u16 = ((1 << 15) - 1);
const OZBC_MAX_BYTES_ZERO: u32 = ((OZBC_MAX_128_BYTES_ZERO as u32) << 7);

macro_rules! get_bytes_from_word {
    (0 $input:expr) => {
        ((($input) >> 8) + 1)
    };
    (1 $input:expr) => {
        ((($input) & OZBC_MAX_128_BYTES_ZERO as u32) << 7)
    };
}

macro_rules! get_word_type {
    ($input:expr) => {
        ($input) >> 15
    };
}

macro_rules! get_dirty_byte {
    ($input:expr) => {
        ($input) & 255
    };
}

#[derive(Clone, PartialEq)]
pub struct OZBCBitmap {
    buffer: Vec<u16>,
    num_bytes: u32,
}

/// Impl [`Debug`] trait printing each bit of every bitmap words.
impl std::fmt::Debug for OZBCBitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut output: String = format!("Number of bits: {}\n", (self.num_bytes << 3));
        // let mut num_bytes = 0;
        for word in &self.buffer {
            let t: u16 = get_word_type!(word);
            for j in (0..16).rev() {
                let x: u16 = (word >> j) & (1 as u16);
                output.push_str(&x.to_string());
                if j == 15 || (j == 8 && t == 0) {
                    output.push_str("|");
                }
            }
            output.push_str("\n");
        }
        write!(f, "{}", output)
    }
}


/// Impl [`BitAnd`] running "logical and" bit operation between 2 bitmaps.
impl BitAnd for &OZBCBitmap {
    type Output = OZBCBitmap;

    fn bitand(self, b2: Self) -> OZBCBitmap {
        let mut bitmap_to_return = OZBCBitmap::new();
        let v0 = self.buffer.as_slice();
        let v1 = b2.buffer.as_slice();
        let mut i: usize = 0; // v0 index
        let mut j: usize = 0; // v1 index
        let mut count_bytes: u32 = 0;
        let mut scanned_bytes: (u32, u32) = (0, 0);

        while i < v0.len() && j < v1.len() {
            let w0: u32 = unsafe { *v0.get_unchecked(i) } as u32;
            let w1: u32 = unsafe { *v1.get_unchecked(j) } as u32;

            let word_type: u8 = ((get_word_type!(w1) << 1) | get_word_type!(w0)) as u8;
            let bytes_in_word = match word_type {
                // w0 and w1 are of type 0
                0b00 => (get_bytes_from_word!(0 w0), get_bytes_from_word!(0 w1)),
                // w0 is of type 1, w1 is of type 0
                0b01 => (get_bytes_from_word!(1 w0), get_bytes_from_word!(0 w1)),
                // w0 is of type 0, w1 is of type 1
                0b10 => (get_bytes_from_word!(0 w0), get_bytes_from_word!(1 w1)),
                // w0 and w1 are of type 1
                0b11 => (get_bytes_from_word!(1 w0), get_bytes_from_word!(1 w1)),
                _ => panic!("Error occured on bitmap and"),
            };
            i += 1;
            j += 1;
            scanned_bytes.0 += bytes_in_word.0;
            scanned_bytes.1 += bytes_in_word.1;

            if scanned_bytes.0 < scanned_bytes.1 {
                scanned_bytes.1 -= bytes_in_word.1;
                j -= 1;
            } else if scanned_bytes.0 > scanned_bytes.1 {
                scanned_bytes.0 -= bytes_in_word.0;
                i -= 1;
            } else if word_type == 0 {
                let mut bytes_zero = scanned_bytes.0 - count_bytes - 1;
                let dirty_byte =
                    unsafe { get_dirty_byte!(*v0.get_unchecked(i - 1) & *v1.get_unchecked(j - 1)) };

                if dirty_byte != 0 {
                    count_bytes = scanned_bytes.0;
                    if bytes_zero < (1 << 7) {
                        bitmap_to_return
                            .buffer
                            .push((bytes_zero << 8) as u16 | dirty_byte);
                    } else {
                        while bytes_zero > OZBC_MAX_BYTES_ZERO {
                            bitmap_to_return
                                .buffer
                                .push(OZBC_MAX_128_BYTES_ZERO | (1 << 15));
                            bytes_zero -= OZBC_MAX_BYTES_ZERO;
                        }
                        bitmap_to_return
                            .buffer
                            .push((bytes_zero >> 7) as u16 | (1 << 15));
                        bitmap_to_return
                            .buffer
                            .push(((bytes_zero as u16 & 127) << 8) | dirty_byte);
                    }
                } // end if dirty_bytes != 0
            } // end if word_type == 0
        } // end while

        bitmap_to_return.num_bytes = count_bytes;
        bitmap_to_return
    }
}

/// Impl [`Bitmap`] to allow to use OZBCBitmap in [`BitmapIndex`].
impl Bitmap for OZBCBitmap {
    
    /// Return new empty bitmap.
    fn new() -> OZBCBitmap {
        OZBCBitmap {
            buffer: Vec::new(),
            num_bytes: 0,
        }
    }

    /// Set the ith bit (starting from zero). You must set the bitmap in increasing
    /// order otherwise nothing happend:
    /// `set(0), set(16), set(1000)` is the same of `set(0), set(16), set(1000), set(50)`.
    ///
    /// # Example
    ///
    /// ```
    /// use bitrush_index::OZBCBitmap;
    /// use bitrush_index::Bitmap;
    ///
    /// fn main() {        
    ///     let mut b0 = OZBCBitmap::new();
    ///     let mut b1 = b0.clone();
    ///     let values = [0, 1, 100, 100000, 2,  100001];
    ///     let values_ok = [0, 1, 100, 100000, 100001];
    ///     for val in values.iter() {
    ///         b0.set(*val);
    ///     }
    ///     for val in values_ok.iter() {
    ///         b1.set(*val);
    ///     }
    ///     assert_eq!(b0, b1);
    /// }
    /// ```
    fn set(&mut self, i: u32) {
        let dirty_bit = (i & 7) as u16;
        let dirty_byte = 1 << dirty_bit;
        let mut bytes_zero: i32 = (i >> 3) as i32 - self.num_bytes as i32;
        if bytes_zero >= 0 {
            self.num_bytes += bytes_zero as u32 + 1;
            if bytes_zero < 128 {
                self.buffer.push(((bytes_zero as u16) << 8) | dirty_byte);
            } else {
                while bytes_zero as u32 > OZBC_MAX_BYTES_ZERO {
                    self.buffer.push((1 << 15) | OZBC_MAX_128_BYTES_ZERO);
                    bytes_zero -= OZBC_MAX_BYTES_ZERO as i32;
                }
                self.buffer.push((1 << 15) | ((bytes_zero >> 7) as u16));
                self.buffer
                    .push((((bytes_zero as u16) & 127) << 8) | dirty_byte);
            }
        } else if bytes_zero == -1 && get_dirty_byte!(*self.buffer.last_mut().unwrap()) < dirty_byte
        {
            *self.buffer.last_mut().unwrap() |= dirty_byte;
        }
    }

    /// Return a vector with all positions of set bit.
    fn unroll_bitmap(&self) -> Vec<u32> {
        let mut pos_set: u32 = 0;
        let mut unrolled_bitmap: Vec<u32> = Vec::with_capacity(self.buffer.len());

        for word in &self.buffer {
            if get_word_type!(word) as u32 == 0 {
                let bytes_zero = (word >> 8) as u32;
                pos_set += bytes_zero << 3;
                let dirty_byte = get_dirty_byte!(word);

                for j in 0..8 {
                    if (dirty_byte >> j) & 1 == 1 {
                        unrolled_bitmap.push(pos_set + j);
                    }
                }
                pos_set += 8;
            } else {
                let bytes_zero: u32 = ((word & OZBC_MAX_128_BYTES_ZERO) as u32) << 7;
                pos_set += bytes_zero << 3;
            }
        }
        unrolled_bitmap
    }

    /// Get bitmap content size.
    fn size(&self) -> usize {
        let word_size = mem::size_of::<u16>();
        let buffer_content_size = self.buffer.len() * word_size;
        let bitmap_size = buffer_content_size + mem::size_of::<u32>();
        bitmap_size
    }

    /// Write bitmap content into buffer_out and return the number of bytes written.
    fn write_to_buffer(&self, buffer_out: &mut [u8]) -> Result<usize, ()> {
        let bitmap_content_size = self.size();
        if buffer_out.len() < bitmap_content_size {
            return Err(());
        }
        let num_bytes_raw: [u8; 4] = unsafe { mem::transmute(self.num_bytes) };
        // let num_bytes_raw: [u8; 4] = self.num_bytes.to_ne_bytes();
        buffer_out[0..(num_bytes_raw.len())].copy_from_slice(&num_bytes_raw);

        let buffer_raw: &[u8] = unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const u8,
                self.buffer.len() * mem::size_of::<u16>(),
            )
        };
        let start_offset = num_bytes_raw.len();
        let end_offset = start_offset + buffer_raw.len();
        buffer_out[start_offset..end_offset].copy_from_slice(buffer_raw);
        Ok(end_offset)
    }

    /// Read bitmap content from buffer_in, buffer_in must have the exact length of bitmap content.
    fn read_from_buffer(&mut self, buffer_in: &[u8], check_bitmap: bool) -> Result<(), ()> {
        let num_bytes_pointer: *const u32 = buffer_in[0..4].as_ptr() as *const u32;
        let num_bytes: u32 = unsafe { *num_bytes_pointer };
        // let num_bytes: u32 = u32::from_ne_bytes(buffer_in[0..4].try_into().unwrap());

        let buffer_pointer: *const u16 = buffer_in[4..buffer_in.len()].as_ptr() as *const u16;
        let buffer_len: usize = (buffer_in.len() - 4) / mem::size_of::<u16>();

        let buffer_slice: &[u16] =
            unsafe { std::slice::from_raw_parts(buffer_pointer, buffer_len) };

        let mut buffer = vec![0; buffer_slice.len()];
        buffer.copy_from_slice(buffer_slice);

        if check_bitmap == true {
            let buffer_num_bytes = OZBCBitmap::get_buffer_num_bytes(&buffer);
            if buffer_num_bytes != num_bytes {
                return Err(());
            }
        }

        self.num_bytes = num_bytes;
        self.buffer = buffer;

        Ok(())
    }
}

impl OZBCBitmap {
    fn get_buffer_num_bytes(buffer: &Vec<u16>) -> u32 {
        buffer.iter().fold(0, |mut num_bytes, &word| {
            num_bytes += match get_word_type!(word) {
                0 => get_bytes_from_word!(0(word as u32)),
                1 => get_bytes_from_word!(1(word as u32)),
                _ => 0,
            };
            num_bytes
        })
    }
}
