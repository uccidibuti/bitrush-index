use bitrush_index::{
    BuildOptions,
    BitmapIndex,
    ChunkSize,
    OZBCBitmap
};
use rand::Rng;

fn create_random_number(n: usize) -> Vec<u32> {
    let mut values = Vec::new();
    let mut rng = rand::thread_rng();
    for _i in 0..n {
        let val: u32 = rng.gen::<u32>();
        values.push(val);
    }
    values
}

#[test]
fn memory_mode() {
    let n =  2 * 1000 * 1000;
    
    let build_options = BuildOptions::new(16, ChunkSize::M1);
    let b_index_r = BitmapIndex::<OZBCBitmap, u32>::new(build_options);
    assert!(b_index_r.is_ok());

    let mut b_index: BitmapIndex<OZBCBitmap, u32> = b_index_r.unwrap();
    let mut values: Vec<u32> = create_random_number(n);

    let result_insert = b_index.push_values(&values);
    assert!(result_insert.is_ok());

    let mut rng = rand::thread_rng();
    let random_index: usize = rng.gen::<usize>() % values.len();
    let val_to_find = values[random_index];
    values.push(val_to_find);
    let result_insert = b_index.push_value(val_to_find);
    assert!(result_insert.is_ok());
   
    let linear_search_result: Vec<u64> = values.iter().enumerate().filter(|(_i, v)| **v == val_to_find).map(|(i, _v)| i as u64).collect();
 
    let values_indexes: Vec<u64> = b_index.run_query(val_to_find, None, None).unwrap();

    assert_eq!(linear_search_result.len(), values_indexes.len());

    for i in 0..linear_search_result.len() {
        assert_eq!(linear_search_result[i], values_indexes[i]);
    }
}


#[test]
fn storage_mode() {
    let n =  3 * 1000 * 1000;
    
    let build_options = BuildOptions::new(16, ChunkSize::M1);
    let path = std::path::Path::new("test_storage_mode");
    let b_index_r = BitmapIndex::<OZBCBitmap, u32>::create(&path, build_options);
    assert!(b_index_r.is_ok());

    let mut b_index: BitmapIndex<OZBCBitmap, u32> = b_index_r.unwrap();
    let mut values: Vec<u32> = create_random_number(n);

    let result_insert = b_index.push_values(&values);
    assert!(result_insert.is_ok());

    let mut rng = rand::thread_rng();
    let random_index: usize = rng.gen::<usize>() % values.len();
    let val_to_find = values[random_index];
    values.push(val_to_find);
    let result_insert = b_index.push_value(val_to_find);
    assert!(result_insert.is_ok());
 
    let linear_search_result: Vec<u64> = values.iter().enumerate().filter(|(_i, v)| **v == val_to_find).map(|(i, _v)| i as u64).collect();

    let run_query_r  = b_index.run_query(val_to_find, None, None);
    assert!(run_query_r.is_ok());
    let values_indexes: Vec<u64> = run_query_r.unwrap();

    let _err = std::fs::remove_dir_all(&path);

    assert_eq!(linear_search_result.len(), values_indexes.len());

    for i in 0..linear_search_result.len() {        
        assert_eq!(linear_search_result[i], values_indexes[i]);
    }
}
