use bitrush_index::{
    BitmapIndex,
    OZBCBitmap,
};

use rand::Rng;
use std::time::Instant;

fn main() {
    // number of values to insert (defalut 100M)
    const N: usize = (1 << 20) * 100;
    let mut rng = rand::thread_rng();

    let build_options = bitrush_index::new_default_index_options::<u32>();
    let mut b_index = match BitmapIndex::<OZBCBitmap, u32>::new(build_options) {
        Ok(b_index) => b_index,
        Err(err) => panic!("Error occured creating bitmap index: {:?}", err)
    };
    let mut values: Vec<u32> = Vec::new();

    for _i in 0..N {
        let val: u32 = rng.gen::<u32>();
        values.push(val);
    }
    println!("--------------------------------------------------");
    println!("Inserting {} values in bitmap index...", N);
    let timer = Instant::now();
    //if index is opened in memory mode you can ignore the push_values result.
    let _result_index = b_index.push_values(&values); 
    let time_b_index_insert = timer.elapsed();
    println!("Bitmap index created in {:?}.", time_b_index_insert);
    println!("Insert per second = {}.", N / (time_b_index_insert.as_millis() as usize)* 1000);
    println!("--------------------------------------------------");

    let random_index: usize = rng.gen::<usize>() % values.len();
    let val_to_find = values[random_index];
    let timer = Instant::now();
    let values_indexes: Vec<u64> = match b_index.run_query(val_to_find, None, None) {
        Ok(indexes) => indexes,
        Err(err) => panic!("Error occured running looking for value = {}, error: {:?}", val_to_find, err)
    };

    let time_linear_search = timer.elapsed();
    println!("Bitmap index search runned in {:?}, match values founded: {}.", time_linear_search, values_indexes.len());
    println!("--------------------------------------------------");
}
