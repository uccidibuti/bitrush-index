# Bitrush-Index
Bitrush-Index is a Rust library that provides a serializable bitmap index able to index millions values/sec on a single thread. On default this library build bitmap-index using [ozbcbitmap] but if you want you can also use another compressed/uncrompressed bitmap. Only equality-query (A = X) are supported.

[ozbcbitmap]: ./src/ozbcbitmap/mod.rs

## Usage
Add this to your `Cargo.toml`:
```toml
[dependencies]
bitrush_index = "0.1.0"
```
See [memory_index](./examples/memory_index.rs) to use a Bitrush-Index in memory mode and [storage_index](./examples/storage_index.rs) to use a Bitrush-Index on persistent memory (storage mode).

### Run examples
```
cargo run --release --example memory_index
```
```
cargo run --release --example storage_index
```

### Test
```
cargo t
```

## Example and performance
```Rust
use bitrush_index::{
    BitmapIndex,
    OZBCBitmap,
};

use rand::Rng;
use std::time::Instant;

fn main() {
    const N: usize = 1 << 30; // 1GB
    const K: usize = (1 << 20) * 1; // 1M
    let mut rng = rand::thread_rng();
    let path = std::path::Path::new("bitrush_index_u32");

    let build_options = bitrush_index::new_default_index_options::<u32>();
    let mut b_index = match BitmapIndex::<OZBCBitmap, u32>::create(&path, build_options) {
        Ok(b_index) => b_index,
        Err(err) => panic!("Error occured creating bitmap index: {:?}", err)
    };

    let mut values: Vec<u32> = Vec::new();
    for _i in 0..K {
        let val: u32 = rng.gen::<u32>();
        values.push(val);
    }
    println!("--------------------------------------------------");
    println!("Inserting {} values in bitmap index...", N);
    let timer = Instant::now();

    for i in 0..N {
        match b_index.push_value(values[i % K]) {
            Ok(_) => {},
            Err(err) => panic!("Error occured inserting i = {}, val = {}, error: {:?}", i, values[i % K], err)
        }
    }
    let time_b_index_insert = timer.elapsed();
    println!("Bitmap index created in {:?}.", time_b_index_insert);
    println!("Insert per second = {}.", N as u128 / time_b_index_insert.as_millis() * 1000);
    println!("--------------------------------------------------");

    let random_index: usize = rng.gen::<usize>() % values.len();
    let val_to_find = values[random_index];

    let timer = Instant::now();

    let values_indexes = match b_index.run_query(val_to_find, None, None) {
        Ok(indexes) => indexes,
        Err(err) => panic!("Error occured running looking for value = {}, error: {:?}", val_to_find, err)
    };

    let time_linear_search = timer.elapsed();
    println!("Bitmap index search runned in {:?}, match values founded: {}.", time_linear_search, values_indexes.len());
    println!("--------------------------------------------------");
}
```
In the table are showed the performance of a u32 index created with N = 1G (2^30) random values with K cardinality on my Acer swift 3 laptop with Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz, 256GB SSD TOSHIBA THNSNK25 and 8GB Ram.

| N = 1G     | Query time | Insert per second | Size on storage |
|------------|------------|-------------------|-----------------|
| K = 1M     | 1.7s       | 8.568.000         | 8.1GB           |
| K = 10M    | 1.5s       | 9.527.000         | 8.0GB           |
| K = 100M   | 265ms      | 6.074.000         | 8.0GB           |
| K = N      | 542ms      | 6.810.000         | 8.0GB           |

Note: random values is the worse input distribution for index size.


## Motivation and purpose
Bitmap indexes have traditionally been considered to work well for low-cardinality columns, which have a modest number of distinct values. The simplest and most common method of bitmap indexing on attribute A with K cardinality associates a bitmap with every attribute value V then the Vth bitmap rapresent the predicate A=V. This approach ensures an efficient solution for performing search but on high-cardinality attributes the size of the bitmap index increase dramatically (i.e. on 32bit value you need 2^32 bitmap, one for each possible values, so you have a index composed from 2^32 bit for each values indexed). As you are understanding this approach is pratically impossible on high-cardinality columns.

The most common approach to fix high-cardinality columns size problem is very simple, for example it possible split a 32bit index in eight 4bit sub-index and then reduce the number of bitmaps from 2^32 to 8 * 2^4 (so you have 128bit for each value inserted instead 2^32bit), but the cons is that at query time and insertion time now you have to read/set eight bitmaps (one for each 4bit group) instead of only one and then the performance of the indexes in query and insertion time decrease dramatically. So to limit the size of a bitmap index without decrease dramatically query and insertion performance the best approach is split the index in sub-index and/or compress each bitmap with one bitmap compression method (some of these are [Roaring], Compax, [EWAH], WAH but there are an "infinite" number of them in literature).

At this point the main problem is choose the right number of bitmaps for a bitmap index with the right compression method to find the best tradeoff between index size and query/insert performance: with uncompressed bitmap more bitmaps imply better query/insert performance and worst bitmap size and vice versa for lower bitmaps, but with compressed bitmaps this is not true. With compressed bitmaps the compression ratio of each bitmap depends from bitmap input and bitmap input depends from your input data distribution and how many bitmaps compose your bitmap index (i.e. if you split a 16bit index in two 8bit sub-index, on a random input distribution for each bitmap you have in average 1 bit set each 2^8 values insead of 1 bit set each 2^16 values, so with compressed bitmaps is possible that a 16bit index composed of 2^16 bitmaps has small size then the sum of two 8bit index composed of 2^8 bitmaps each). 

For these reasons I have created Bitrush-Index, a Rust library that allow you to create a bitmap-index choosing the bitmap compression method and the number of bitmaps for each index/sub-index, so it's possible create the best bitmap-index on each indexed value for each input distribution. Bitrush-Index provides also a default bitmap index built with [ozbcbitmap] on each possibily signed/unsigned integer (from 8bit to 128bit integer).

[Roaring]: https://github.com/RoaringBitmap/CRoaring
[EWAH]: https://github.com/lemire/EWAHBoolArray

### About OZBCbitmap
I have designed and developed ozbc with the only aim to provide the best bitmap compression method only in the bitmap index scenario, here there is the first c++ version with some benchmark of ozbc, Roaring and EWAH16/32 that I have developed during my University thesis period: [WhyOZBC] .

[WHyOZBC]: https://github.com/uccidibuti/OZBCBitmap


## Documentation
[link](https://docs.rs/bitrush-index/) .


## Roadmap
- Improve tests and documentation.
- Write C API.
- Write backup function.
- Write bitmaps chunks in async mode.
- Add other Bitmap implementations.
- Improve insert/query performance.


### Suggestions
Suggestions to improve this library is well accepted, for any suggestion you can write to me in privete or open an issue.


## License
Copyright © 2019 Lorenzo Vannucci

Licensed under the General Public License (GPL), version 3 ([LICENSE] http://www.gnu.org/licenses/gpl-3.0.en.html).

[LICENSE]: ./LICENSE

### If you need a permissive or private license
Please contact me if you need a different license and really want to use my code. I am the only author and I can change the license.


## Contribution
Any contribution intentionally submitted for inclusion in bitrush-index by you, shall be licensed as GPLv3 or later, without any additional terms or conditions.
