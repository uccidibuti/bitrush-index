// This code is released under the
// General Public License (GPL), version 3
// http://www.gnu.org/licenses/gpl-3.0.en.html
// (c) Lorenzo Vannucci

//! # Bitrush-Index
//! Bitrush-Index is a Rust library that provides a serializable bitmap index
//! able to index millions values/sec on a single thread. On default this
//! library build bitmap-index using [`ozbcbitmap`] but if you want you can
//! also use another compressed/uncrompressed bitmap.
//! Only equality-query (A = X) are supported.
//!
//! ## Example
//!```
//! use bitrush_index::{
//!     BitmapIndex,
//!     OZBCBitmap,
//! };
//! 
//! use rand::Rng;
//! use std::time::Instant;
//! 
//! fn main() {
//!     // number of values to insert (defalut 10M)
//!     const N: usize = (1 << 20) * 10;
//!     let mut rng = rand::thread_rng();
//! 
//!     let build_options = bitrush_index::new_default_index_options::<u32>();
//!     let mut b_index = match BitmapIndex::<OZBCBitmap, u32>::new(build_options) {
//!         Ok(b_index) => b_index,
//!         Err(err) => panic!("Error occured creating bitmap index: {:?}", err)
//!     };
//!     let mut values: Vec<u32> = Vec::new();
//! 
//!     for _i in 0..N {
//!         let val: u32 = rng.gen::<u32>();
//!         values.push(val);
//!     }
//!     println!("--------------------------------------------------");
//!     println!("Inserting {} values in bitmap index...", N);
//!     let timer = Instant::now();
//!     //if index is opened in memory mode you can ignore the push_values result.
//!     let _result_index = b_index.push_values(&values); 
//!     let time_b_index_insert = timer.elapsed();
//!     println!("Bitmap index created in {:?}.", time_b_index_insert);
//!     println!("Insert per second = {}.", N as u128 / time_b_index_insert.as_millis() * 1000);
//!     println!("--------------------------------------------------");
//! 
//!     let random_index: usize = rng.gen::<usize>() % values.len();
//!     let val_to_find = values[random_index];
//!     let timer = Instant::now();
//!     let values_indexes = match b_index.run_query(val_to_find, None, None) {
//!         Ok(indexes) => indexes,
//!         Err(err) => panic!("Error occured running looking for value = {}, error: {:?}", val_to_find, err)
//!     };
//! 
//!     let time_linear_search = timer.elapsed();
//!     println!("Bitmap index search runned in {:?}, match values founded: {}.", time_linear_search, values_indexes.len());
//!     println!("--------------------------------------------------");
//! }
//!```
//!
//! [`ozbcbitmap`]: ./ozbcbitmap/mod.rs

mod bitmap_index;
pub use bitmap_index::{
    BitValue,
    BitmapIndex,
    StorageIdx,
    Bitmap,
    MetaData,
    BuildOptions,
    ChunkSize
};

mod ozbcbitmap;
pub use ozbcbitmap::OZBCBitmap;

/// Return default options to create a BitmapIndex.
pub fn new_default_index_options<U: BitValue>() -> BuildOptions {
    let value_size = std::mem::size_of::<U>();
    if value_size == 1 {
        return BuildOptions::new(8, ChunkSize::M32);
    }
    BuildOptions::new(16, ChunkSize::M16) 
}
