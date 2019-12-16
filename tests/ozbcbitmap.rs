use bitrush_index::{Bitmap, OZBCBitmap};
use rand::Rng;

#[test]
fn set_base() {
    let mut b0 = OZBCBitmap::new();
    let mut b1 = b0.clone();
    let values = [0, 1, 100, 100000, 99999, 2, 100001, 1000000];
    let values_ok = [0, 1, 100, 100000, 100001, 1000000];

    for val in values.iter() {
        b0.set(*val);
    }

    for val in values_ok.iter() {
        b1.set(*val);
    }

    assert_eq!(b0, b1);
}

#[test]
fn set_advanced() {
    let mut b0 = OZBCBitmap::new();
    let mut b1 = b0.clone();

    let mut rng = rand::thread_rng();
    let n = 100000;
    let mut last_val_ok: u32 = rng.gen::<u32>();
    b0.set(last_val_ok);
    b1.set(last_val_ok);

    for _i in 1..n {
        let val = rng.gen::<u32>();
        b0.set(val);
        if val > last_val_ok {
            b1.set(val);
            last_val_ok = val;
        }
    }

    assert_eq!(b0, b1);
}

#[test]
fn bitand_and_unroll() {
    let mut b0 = OZBCBitmap::new();
    let mut b1 = b0.clone();
    let values_0 = [0, 1, 100, 100000, 100009, 1000000, 1000100, 1060000];
    let values_1 = [
        1, 7, 9, 99999, 100000, 100001, 100101, 1060000, 1060001, 2060001,
    ];
    let values_and = [1, 100000, 1060000];

    for val in values_0.iter() {
        b0.set(*val);
    }

    for val in values_1.iter() {
        b1.set(*val);
    }

    let b_and = (&b0) & (&b1);
    let unrolled_values = b_and.unroll_bitmap();

    assert_eq!(unrolled_values, values_and);
}

#[test]
fn write_read_advanced() {
    let mut b0 = OZBCBitmap::new();

    let mut rng = rand::thread_rng();
    let n = 100000;
    let mut last_val_ok: u32 = rng.gen::<u32>();
    b0.set(last_val_ok);

    for _i in 1..n {
        let val = rng.gen::<u32>();
        if val > last_val_ok {
            b0.set(val);
            last_val_ok = val;
        }
    }

    let b1 = b0.clone();
    let mut buf: Vec<u8> = vec![0; b0.size()];
    let r_write = b0.write_to_buffer(&mut buf);
    assert!(r_write.is_ok());
    assert_eq!(r_write.unwrap(), buf.len());

    b0 = OZBCBitmap::new();
    let r_read = b0.read_from_buffer(&buf, true);
    assert!(r_read.is_ok());
    assert_eq!(b0, b1);
}
