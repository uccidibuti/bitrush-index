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
