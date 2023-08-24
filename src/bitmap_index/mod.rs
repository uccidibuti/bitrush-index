// This code is released under the
// General Public License (GPL), version 3
// http://www.gnu.org/licenses/gpl-3.0.en.html
// (c) Lorenzo Vannucci

//! # BitmapIndex
//!
//! A serializable bitmap-index for each value that implement BitValue trait
//! and for each bitmap that implement Bitmap trait.
//! On default `BitValue` is implemented for:
//! `u8, u16`, `u32`, `u64`, `u128`, `i8`, `i16`, `i32`, `i64`, `i128`.

/// [`Bitmap`]: ./bitmap.rs

use std::ops::{BitAnd, BitOr, Shr};
use std::marker::Copy;
use std::fmt::{Display};
use std::convert::From;
use std::mem;
use std::io::{Write, Error as IoError, SeekFrom, Seek, Read};
use std::fs;
use std::path::{Path, PathBuf};

mod bitmap;
pub use self::bitmap::Bitmap;

/// A trait that allow to convert `BitValue` to `usize`.
pub trait TransmuteToUsize {

    /// Return the right most bits of self as `usize` type.
    fn transmute_to_usize(self) -> usize;
}

macro_rules! impl_transmute_to_usize_for_number {
    ($ty:ident) => {
        impl TransmuteToUsize for $ty {
            fn transmute_to_usize(self) -> usize { self as usize }
        }
    };
}

/// `BitValue` trait.
/// On default `BitValue` is implemented for:
/// `u8, u16`, `u32`, `u64`, `u128`, `i8`, `i16`, `i32`, `i64`, `i128`.
pub trait BitValue: Copy + Display + BitAnd + BitOr + Shr<usize> + TransmuteToUsize {}

macro_rules! impl_bit_value_for_number {
    ($ty:ident) => {
        impl_transmute_to_usize_for_number!($ty);
        impl BitValue for $ty {}
    };
    ($ty1:ident, $($ty2:ident),+) => {
        impl_transmute_to_usize_for_number!($ty1);
        impl BitValue for $ty1 {}
        impl_bit_value_for_number!($($ty2),+);
    }    
}

impl_bit_value_for_number!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

/// `BitmapIndex` errors types.
#[derive(Debug)]
pub enum Error {
    ParametersError,
    FileError(IoError),
    BitmapError,
}

/// `BitmapIndex` works in chunks, each chunk represent `ChunkSize` values,
/// possibily values for `ChunkSize` are: 1 Mega, 2 Mega, 4 Mega, 8 Mega, 16 Mega, 32 Mega.
/// If `BitmapIndex` is created in storage mode, bitmaps are serialized every time a chunk
/// is full.
#[derive(Clone)]
pub enum ChunkSize {
    M1 = (1 << 20),
    M2 = (1 << 21),
    M4 = (1 << 22),
    M8 = (1 << 23),
    M16 = (1 << 24),
    M32 = (1 << 25),
}

/// `BitmapIndex` struct that requires a bitmap that implement [`Bitmap`] trait and
/// a type that implement `BitValue` trait.
pub struct BitmapIndex<T: Bitmap, U: BitValue>
where <U as Shr<usize>>::Output: TransmuteToUsize,
for <'a> &'a T: BitAnd<&'a T, Output=T> {
    num_values: u64,
    chunk_size: u64,
    chunk_size_mask: u64,
    
    bitmaps: Vec<T>,
    block_info: BlockInfo,
    
    storage_idx: Option<StorageIdx>,
    chunk_offset: u64,
    chunks: Option<Vec<Vec<T>>>,
    last_checkpoint: Option<MetaData>,
    
    _marker: std::marker::PhantomData<U>
}

struct BlockInfo {
    bit_block_size: usize,
    bit_block_mask: usize,
    num_blocks: usize,
    num_bitmaps_in_block: usize,
}

/// `BuildOptions` defines how many bitmap compose a `BitmapIndex` and how many values must
/// represent every chunk in `BitmapIndex`.
/// On a `BitmapIndex` of a `BitValue` 'U' with `size_in_bit(U) = L`, `bit_block_size` represent
/// the size in bit of each sub-index.
/// For example a `BitmapIndex` of a `BitValue` 'U=`u16`' with 'size_in_bit(u16) = 16'
/// and `bit_block_size = 8` is composed from '16 / 8 = 2' blocks of '2^8 = 256' bitmaps each,
/// so is composed from '2 * 256 = 512' bitmaps.
#[derive(Clone)]
#[repr(C)]
pub struct BuildOptions {
    bit_block_size: usize,
    chunk_size: ChunkSize
}

impl BuildOptions {
    /// Create a new `BuildOptions`.
    pub fn new(bit_block_size: usize, chunk_size: ChunkSize) -> Self {
        BuildOptions {
            bit_block_size,
            chunk_size
        }
    }
}

/// `StorageIdx` defines a `BitmapIndex` opened in read-only storage mode.
pub struct StorageIdx {
    meta_data_file: fs::File,
    offset_file: fs::File,
    data_file: fs::File
}

/// `MetaData` defines the meta data of a `BitmapIndex`.
#[repr(C)]
pub struct MetaData {
    num_values: u64,
    build_options: BuildOptions
}

impl<T: Bitmap, U: BitValue> BitmapIndex<T, U>
where <U as std::ops::Shr<usize>>::Output: TransmuteToUsize,
for <'a> &'a T: BitAnd<&'a T, Output=T> {

    /// Return a `BitmapIndex` in storage mode and create a folder with `bitmap_index_path` path.
    /// The created folder contain 3 files that represent a `BitmapIndex`:
    /// 1) A file with 'mbidx' extension that represent `BitmapIndex` meta data.
    /// 2) A file with 'obidx' extension that represent all offsets of all bitmaps chunks.
    /// 3) A file with 'dbix' extension that represent all bitmaps chunks content.
    pub fn create(bitmap_index_path: &Path, build_options: BuildOptions) -> Result<Self, Error> {
        if Self::check_if_path_exixsts(bitmap_index_path) {
            return Err(Error::ParametersError);
        }
        let storage_idx = Self::get_storage_idx(bitmap_index_path, Some(build_options.clone()))?;

        let mut bitmap_index: Self = Self::new_index(build_options, true)?;
        bitmap_index.storage_idx = Some(storage_idx);
        bitmap_index.last_checkpoint = Some(bitmap_index.get_meta_data());
        
        Ok(bitmap_index)
    }

    /// Open a `BitmapIndex` in storage mode previusly created.
    pub fn open(dir_path: &Path) -> Result<Self, Error> {
        let mut storage_idx = Self::get_storage_idx(dir_path, None)?;
        let m = Self::map_io_result(Self::read_meta_data(&mut storage_idx))?;
        let mut bitmap_index = Self::new_index(m.0.build_options, true)?;
        bitmap_index.num_values = m.0.num_values;

        let offsets_r = Self::read_chunk_offset(&mut storage_idx, bitmap_index.num_values, bitmap_index.chunk_size);
        let offsets = Self::map_io_result(offsets_r)?;
        bitmap_index.chunk_offset = offsets.0;
        if bitmap_index.num_values & bitmap_index.chunk_size_mask != 0 {
            let r_buf_chunk = Self::read_chunk(&mut storage_idx, offsets);
            let buf_chunk = Self::map_io_result(r_buf_chunk)?;
            Self::read_bitmaps(&buf_chunk, &mut bitmap_index.bitmaps)?;
        }
        bitmap_index.storage_idx = Some(storage_idx);
        bitmap_index.last_checkpoint = Some(m.1);

        Ok(bitmap_index)
    }

    fn read_bitmaps(buf: &[u8], bitmaps: &mut [T]) -> Result<(), Error> {
        let num_offsets = bitmaps.len() + 1;
        let buf_offsets_size = num_offsets * mem::size_of::<u32>();
        let v_offsets: &[u32] = Self::convert_slice(&buf[0..buf_offsets_size]);
        let v_bitmap: &[u8] = &buf[buf_offsets_size..];

        let mut i = 0;
        let mut start_offset = 0;
        
        for b in bitmaps.iter_mut() {
            let b_size: usize = (v_offsets[i + 1] - v_offsets[i]) as usize;
            let end_offset = start_offset + b_size;
            Self::read_bitmap(&v_bitmap[start_offset..end_offset], true, b)?;
            start_offset = end_offset;
            i += 1;
        }
        Ok(())
    }
    
    fn read_bitmap(buf: &[u8], check_bitmap: bool, bitmap: &mut T) -> Result<(), Error> {
        let r = bitmap.read_from_buffer(buf, check_bitmap);
        Self::map_bitmap_result(r)
    }

    fn map_bitmap_result(result: Result<(), ()>) -> Result<(), Error> {
        let r = match result {
            Err(_) => Err(Error::BitmapError),
            Ok(_) => Ok(())
        };
        r
    }

    fn read_chunk(storage_idx: &mut StorageIdx, offsets: (u64, u64)) -> Result<Vec<u8>, IoError> {
        let buf_chunk_size: usize = (offsets.1 - offsets.0) as usize;
        let mut buf_chunk: Vec<u8> = vec![0; buf_chunk_size];
        storage_idx.data_file.seek(SeekFrom::Start(offsets.0))?;
        storage_idx.data_file.read_exact(&mut buf_chunk)?;

        Ok(buf_chunk)
    }

    fn read_bitmap_offset(storage_idx: &mut StorageIdx, chunk_offset: u64, i_bitmap: usize) -> Result<(u64, u64), Error> {
        let i_bitmap_offset = chunk_offset + (i_bitmap * mem::size_of::<u32>()) as u64;
        let r_seek = storage_idx.data_file.seek(SeekFrom::Start(i_bitmap_offset));
        Self::map_io_result(r_seek)?;
        const BUF_SIZE: usize = mem::size_of::<u32>() * 2;
        let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];

        Self::map_io_result(storage_idx.data_file.read_exact(&mut buf))?;
        let start_offset: u32 = Self::copy_from_slice_u8(&buf[0..mem::size_of::<u32>()]);
        let end_offset: u32 = Self::copy_from_slice_u8(&buf[mem::size_of::<u32>()..]);
        
        Ok((chunk_offset + start_offset as u64, chunk_offset + end_offset as u64)) 
    }

    fn read_query_bitmaps(storage_idx: &mut StorageIdx, chunk_offset: u64, query_i_bitmaps: &[usize]) -> Result<Vec<T>, Error> {
        let vec_len = query_i_bitmaps.len();
        let mut bitmaps_offset: Vec<(u64, u64)> = Vec::with_capacity(vec_len);
        for i_bitmap in query_i_bitmaps {
            let bitmap_offset = Self::read_bitmap_offset(storage_idx, chunk_offset, *i_bitmap)?;
            bitmaps_offset.push(bitmap_offset);
        }

        let mut query_bitmaps: Vec<T> = Vec::with_capacity(vec_len);
        let mut buf: Vec<u8> = Vec::new();
        for offset in bitmaps_offset {
            let buf_len = (offset.1 - offset.0) as usize;
            let mut bitmap = T::new();
            if buf.len() < buf_len {
                buf = vec![0; buf_len];
            }
            Self::map_io_result(storage_idx.data_file.seek(SeekFrom::Start(offset.0)))?;
            let r_read = storage_idx.data_file.read_exact(&mut buf[0..buf_len]);
            Self::map_io_result(r_read)?;
            Self::read_bitmap(&buf[0..buf_len], true, &mut bitmap)?;
            query_bitmaps.push(bitmap);
        }
        
        Ok(query_bitmaps)
    }
    
    fn read_chunk_offset(storage_idx: &mut StorageIdx, num_values: u64, chunk_size: u64) -> Result<(u64, u64), IoError> {
        let chunk_size_mask = chunk_size - 1;
        let offsets = if num_values == 0 {
            (0, 0)
        } else {
            let start_index = num_values - (num_values & chunk_size_mask);
            let end_index = num_values;
            let offsets = Self::read_chunks_offsets(storage_idx, start_index, end_index, chunk_size, 2, 1)?;
            if offsets.len() == 1 {
                (offsets[0], offsets[0])
            } else {
                (offsets[0], offsets[1])
            }
        };
                
        Ok((offsets.0, offsets.1))
    }

    fn read_chunks_offsets(storage_idx: &mut StorageIdx, start_index: u64, end_index: u64, chunk_size: u64, max_num_offsets: usize, plus_chunk: usize) -> Result<Vec<u64>, IoError> {
        let start_offset = Self::get_chunk_id(start_index, chunk_size);
        let mut num_offsets = (Self::get_chunk_id(end_index, chunk_size) - start_offset) as usize + plus_chunk;
        if num_offsets > max_num_offsets
        {
            num_offsets = max_num_offsets;
        }
        let offsets: Vec<u64> = vec![0; num_offsets];
        
        let block_offset: u64 = Self::get_block_offset(start_offset);
        let buf: &mut [u8] = Self::convert_slice(&offsets);

        storage_idx.offset_file.seek(SeekFrom::Start(block_offset))?;
        storage_idx.offset_file.read_exact(buf)?;
        
        Ok(offsets)
    }


    fn read_meta_data(storage_idx: &mut StorageIdx) -> Result<(MetaData, MetaData), IoError> {
        const META_DATA_SIZE: usize = mem::size_of::<MetaData>();
        let mut meta_data_buf: [u8; META_DATA_SIZE] = [0; META_DATA_SIZE];
        storage_idx.meta_data_file.seek(SeekFrom::Start(0))?;
        storage_idx.meta_data_file.read_exact(&mut meta_data_buf)?;
        let meta_data: MetaData = Self::copy_from_slice_u8(&meta_data_buf);
        storage_idx.meta_data_file.read_exact(&mut meta_data_buf)?; 
        let last_check_point: MetaData = Self::copy_from_slice_u8(&meta_data_buf);
        Ok((meta_data, last_check_point))
    }

    fn check_if_path_exixsts(dir_path: &Path) -> bool {
        match fs::metadata(dir_path) {
            Ok(_) => true,
            Err(_) => false
        }
    }

    fn map_io_result<M>(result: Result<M, IoError>) -> Result<M, Error> {
        match result {
            Ok(r) => Ok(r),
            Err(err) => Err(Error::FileError(err))
        }
    }

    pub fn new_storage_idx(dir_path: &Path) -> Result<StorageIdx, Error> {
        Self::get_storage_idx(dir_path, None)
    }

    fn get_storage_idx(dir_path: &Path, build_options: Option<BuildOptions>) -> Result<StorageIdx, Error> {
        let storage_idx = match Self::open_storage_idx(dir_path, build_options.clone()) {
            Ok(s_idx) => s_idx,
            Err(err) => {
                if build_options.is_some() {
                    Self::map_io_result(fs::remove_dir(dir_path))?;
                }
                return Err(Error::FileError(err));
            }
        };

        Ok(storage_idx)
    }
    
    fn open_storage_idx(dir_path: &Path, build_options: Option<BuildOptions>) -> Result<StorageIdx, IoError> {
        if build_options.is_some() {
            fs::create_dir(dir_path)?;
        }
        let name = dir_path.file_name().unwrap();
        
        let mut meta_data_path = PathBuf::from(dir_path);
        meta_data_path.push(name);
        meta_data_path.set_extension("mbidx");

        let mut offset_path = PathBuf::from(dir_path);
        offset_path.push(name);
        offset_path.set_extension("obidx");
        
        let mut data_path = PathBuf::from(dir_path);
        data_path.push(name);
        data_path.set_extension("dbidx");

        let data_file = Self::open_file(data_path.as_path(), true)?;
        let offset_file = Self::open_file(offset_path.as_path(), true)?;
        let meta_data_file = Self::open_file(meta_data_path.as_path(), true)?;
        let mut storage_idx = StorageIdx {
            meta_data_file,
            offset_file,
            data_file
        };
        if build_options.is_some() {
            let meta_data = MetaData {
                num_values: 0,
                build_options: build_options.unwrap()
            };
            Self::write_empty_storage_idx(&mut storage_idx, &meta_data)?;
        }

        Ok(storage_idx)
    }

    fn write_empty_storage_idx(storage_idx: &mut StorageIdx, meta_data: &MetaData) -> Result<(), IoError> {
        storage_idx.meta_data_file.write_all(Self::to_slice_u8(meta_data))?;
        Ok(())
    }

    
    fn get_meta_data(&self) -> MetaData {
        MetaData {
            num_values: self.num_values,
            build_options: BuildOptions {
                bit_block_size: self.block_info.bit_block_size,
                chunk_size: unsafe { mem::transmute::<u32, ChunkSize>(self.chunk_size as u32) }
            }
        }
    }

    fn to_slice_u8<M>(m: &M) -> &[u8] {
        let m: *const u8 = Self::convert_pointer(m as *const M);
        unsafe {
            std::slice::from_raw_parts(m, mem::size_of::<M>())
        }
    }

    fn copy_from_slice_u8<M>(slice: &[u8]) -> M {
        let m: *const M = Self::convert_pointer(slice.as_ptr());
        unsafe { mem::transmute_copy::<M, M>(&*m) }
    }

    fn convert_slice<M,N>(slice: &[M]) -> &mut [N] {
        let n: *mut N = Self::convert_pointer::<M,N>(slice.as_ptr()) as *mut N;
        let len = slice.len() * mem::size_of::<M>() / mem::size_of::<N>();
        unsafe {
            std::slice::from_raw_parts_mut(n, len)
        }
    }

    fn convert_pointer<M, N>(m: *const M) -> *const N {
        m as *const N
    }

    fn open_file(path: &Path, create: bool) -> Result<fs::File, IoError> {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(path)
    }


    fn new_block_info(bit_block_size: usize) -> Result<BlockInfo, Error> {
        let bit_value_size = mem::size_of::<U>() << 3;
        if bit_value_size % bit_block_size != 0 || bit_block_size > 16 || bit_block_size == 1 {
            return Err(Error::ParametersError);
        }

        let num_blocks = bit_value_size / bit_block_size;
        let num_bitmaps_in_block = 1 << bit_block_size;
        Ok(BlockInfo {
            bit_block_size,
            bit_block_mask: num_bitmaps_in_block - 1,
            num_blocks,
            num_bitmaps_in_block            
        })
    }

    /// Return a new `BitmapIndex` in memory mode. A `BitmapIndex` created with this
    /// function works in memory and can't be serialized.
    pub fn new(build_options: BuildOptions) -> Result<Self, Error> {
        Self::new_index(build_options, false)
    }

    fn new_index(build_options: BuildOptions, is_storage_idx: bool) -> Result<Self, Error> {
        let block_info = Self::new_block_info(build_options.bit_block_size)?;
        let num_bitmaps = block_info.num_blocks * block_info.num_bitmaps_in_block;
        let chunk_size: u64 = build_options.chunk_size as u64;

        let mut b_index = BitmapIndex {
            num_values: 0,
            chunk_size,
            chunk_size_mask: chunk_size - 1,
            
            bitmaps: vec![T::new(); num_bitmaps],
            block_info,

            storage_idx: None,
            chunk_offset: 0,
            chunks: None,
            last_checkpoint: None,
            
            _marker: std::marker::PhantomData,
        };
        if is_storage_idx == false {
            b_index.chunks = Some(Vec::new());
        }
        Ok(b_index)
    }

    fn run_f_on_i_bitmaps(block_info: &BlockInfo, value: U, mut f: impl FnMut(usize)) {
        let mut i_block: usize = 0;
        let mut i_bitmap = value.transmute_to_usize() & block_info.bit_block_mask;
        let mut shift_value: usize = 0;
        f(i_bitmap);

        for _i in 1..block_info.num_blocks {
            shift_value += block_info.bit_block_size;
            i_block += block_info.num_bitmaps_in_block;
            i_bitmap = i_block + ((value >> shift_value).transmute_to_usize() & block_info.bit_block_mask);
            f(i_bitmap);
        }
    }

    /// Insert (in append) a `value` into the index. If `BitmapIndex` is opened in
    /// storage mode and the chunk is full, automatically the chunk is flushed on
    /// persistent memory.
    pub fn push_value(&mut self, value: U) -> Result<(), Error> {
        let num_values_in_chunk = self.num_values & self.chunk_size_mask;
        let bitmaps = &mut self.bitmaps;
        let f = |i_bitmap: usize| {
            bitmaps[i_bitmap].set(num_values_in_chunk as u32);
        };
        Self::run_f_on_i_bitmaps(&self.block_info, value, f);
        self.num_values += 1;

        if self.num_values & self.chunk_size_mask != 0 {
            return Ok(());
        }
        if self.storage_idx.is_some() {
            self.write_chunk()?;
            self.bitmaps = vec![T::new(); self.bitmaps.len()];
        } else if self.chunks.is_some() {
            let chunks = self.chunks.as_mut().unwrap();
            let mut bitmaps = vec![T::new(); self.bitmaps.len()];
            mem::swap(&mut bitmaps, &mut self.bitmaps);
            chunks.push(bitmaps);
        }
        Ok(())
    }

    fn get_chunk_id(num_values: u64, chunk_size: u64) -> u64 {
        num_values / chunk_size
    }

    fn get_block_offset(chunk_id: u64) -> u64 {
        chunk_id * mem::size_of::<u64>() as u64
    }

    /// Serialize current bitmaps chunk. Error occur if `BitmapIndex` is opened in memory mode.
    pub fn flush_chunk(&mut self) -> Result<(), Error> {
        if self.storage_idx.is_none() {
            return Err(Error::ParametersError);
        }
        self.write_chunk()
    }

    pub fn memory_bitmaps_size(&self) -> usize {
        let mut bitmaps_size = 0;
        for b in &self.bitmaps {
            bitmaps_size += b.size();
        }
        bitmaps_size
    }
    
    fn write_chunk(&mut self) -> Result<(), Error> {
        let num_bitmaps: usize = self.bitmaps.len();
        let mut bitmaps_size: usize = 0;
        let mut bitmaps_offset: Vec<u32> = vec![0; num_bitmaps + 1];
        let bitmap_start_offset: u32 = (bitmaps_offset.len() * mem::size_of::<u32>()) as u32;
        let mut i: usize = 1;
        bitmaps_offset[0] = bitmap_start_offset;
        for b in &self.bitmaps {
            bitmaps_size += b.size();
            bitmaps_offset[i] = bitmap_start_offset + bitmaps_size as u32;
            i += 1;
        }
        
        let mut bitmaps_content: Vec<u8> = vec![0; bitmaps_size];
        if let Err(_) = self.write_bitmaps_into_buffer(&mut bitmaps_content) {
            return Err(Error::BitmapError);
        };
        let chunk_id = Self::get_chunk_id(self.num_values, self.chunk_size);
        let block_offset: u64 = Self::get_block_offset(chunk_id);
        let b_offsets = Self::convert_slice(&bitmaps_offset);
        
        let storage_idx: &mut StorageIdx = self.storage_idx.as_mut().unwrap();
        let storage_idx2: &'static mut StorageIdx = unsafe { &mut*(storage_idx as *mut StorageIdx)};
        self.chunk_offset = Self::map_io_result(
            self.write_bitmaps_sync(storage_idx2, &b_offsets, &bitmaps_content, block_offset)
        )?;
        self.last_checkpoint = Some(self.get_meta_data());

        Ok(())
    }

    fn write_bitmaps_sync(&mut self, storage_idx: &mut StorageIdx, bitmaps_offsets: &[u8], bitmaps_content: &[u8], block_offset: u64) -> Result<u64, IoError> {
        storage_idx.data_file.seek(SeekFrom::Start(self.chunk_offset))?;
        storage_idx.data_file.write_all(bitmaps_offsets)?;
        storage_idx.data_file.write_all(bitmaps_content)?;

        let chunk_data_size: u64 = (bitmaps_offsets.len() + bitmaps_content.len()) as u64;
        let chunk_next_offset: u64 = self.chunk_offset + chunk_data_size;
        storage_idx.offset_file.seek(SeekFrom::Start(block_offset))?;        
        storage_idx.offset_file.write_all(&chunk_next_offset.to_ne_bytes())?;

        let meta_data: MetaData = self.get_meta_data();
        storage_idx.meta_data_file.seek(SeekFrom::Start(0))?;
        storage_idx.meta_data_file.write_all(Self::to_slice_u8(&meta_data))?;
        storage_idx.meta_data_file.write_all(Self::to_slice_u8(&self.last_checkpoint))?;
        
        Ok(chunk_next_offset)
    }


    fn write_bitmaps_into_buffer(&mut self, buf: &mut [u8]) -> Result<(), ()> {
        let mut start_offest: usize = 0;
        for b in &self.bitmaps {
            let b_size = b.write_to_buffer(&mut buf[start_offest..])?;
            start_offest += b_size;
        }
        Ok(())
    }

    /// Insert (in append) values into the index.
    pub fn push_values(&mut self, values: &[U]) -> Result<(), Error> {
        for v in values {
            self.push_value(*v)?;
        }
        Ok(())
    }

    /// Return a `Vec<u64>` that contains all indexes of values pushed
    /// in `BitmapIndex` equal to `value`. The parameters `start_index` and `end_index`
    /// are optional and if specified define the range where query is runned.
    pub fn run_query(&mut self, value: U, start_index: Option<u64>, end_index: Option<u64>) -> Result<Vec<u64>, Error> {
        let query_i_bitmaps: Vec<usize> = Self::get_query_i_bitmaps(&self.block_info, value);

        let start_index: u64 = start_index.unwrap_or(0);
        let end_index: u64 = end_index.unwrap_or(self.num_values);
        let mut chunk_id = 0;
        let mut indexes: Vec<u64> = Vec::new();
        
        if self.chunks.is_some() {
            let chunks: &Vec<Vec<T>> = self.chunks.as_ref().unwrap();
            for bitmaps in chunks {
                let query_bitmaps: Vec<&T> = query_i_bitmaps.iter()
                    .map(|i_bitmap| &bitmaps[*i_bitmap]).collect();
                Self::push_indexes(&query_bitmaps, chunk_id, self.chunk_size, start_index, end_index, &mut indexes);
                chunk_id += 1;
            }
        } else if self.storage_idx.is_some() {
            let meta_data = self.get_meta_data(); 
            let storage_idx = self.storage_idx.as_mut().unwrap();
            chunk_id = self.num_values / self.chunk_size;
            let end_index = end_index - (end_index & self.chunk_size_mask);
            let s_indexes = Self::run_query_from_storage_idx(storage_idx, value, Some(start_index), Some(end_index), Some(meta_data))?;
            indexes = s_indexes;
        }
        let query_bitmaps: Vec<&T> = query_i_bitmaps.iter()
            .map(|i_bitmap| &self.bitmaps[*i_bitmap]).collect();
        Self::push_indexes(&query_bitmaps, chunk_id, self.chunk_size, start_index, end_index, &mut indexes);

        Ok(indexes)
    }

    /// Return a `Vec<u64>` that contains all indexes of values pushed in a storage `BitmapIndex`
    /// equal to `value`. Differently from `run_query` method allow to run a query only on
    /// the chunks already flushed of a `BitmapIndex`.
    pub fn run_query_from_storage_idx(storage_idx: &mut StorageIdx, value: U, start_index: Option<u64>, end_index: Option<u64>, meta_data: Option<MetaData>) -> Result<Vec<u64>, Error> {
        let m_data = if meta_data.is_some() {
            meta_data.unwrap()
        } else {
            let meta_data_r = Self::read_meta_data(storage_idx);
            let meta_data = Self::map_io_result(meta_data_r)?;
            meta_data.0
        };
        let block_info = Self::new_block_info(m_data.build_options.bit_block_size)?;
        let mut start_index = start_index.unwrap_or(0);
        if start_index > m_data.num_values {
            return Ok(Vec::new())
        }
        let mut end_index = end_index.unwrap_or(m_data.num_values);
        if end_index > m_data.num_values {
            end_index = m_data.num_values;
        }
        let query_i_bitmaps: Vec<usize> = Self::get_query_i_bitmaps(&block_info, value);

        let mut indexes: Vec<u64> = Vec::new();
        const MAX_NUM_OFFSETS: usize = 1 << 13;
        let chunk_size = m_data.build_options.chunk_size as u64;
        let mut chunk_id = 0;
        while start_index < end_index {
            let chunks_offsets_r = Self::read_chunks_offsets(storage_idx, start_index, end_index, chunk_size, MAX_NUM_OFFSETS, 0);
            let chunks_offsets: Vec<u64> = Self::map_io_result(chunks_offsets_r)?;
            for chunk_offset in chunks_offsets.iter() {
                let query_bitmaps = Self::read_query_bitmaps(storage_idx, *chunk_offset, &query_i_bitmaps)?;
                let query_bitmaps_ref: Vec<&T> = query_bitmaps.iter().map(|q| q).collect();
                Self::push_indexes(&query_bitmaps_ref, chunk_id, chunk_size, start_index, end_index, &mut indexes);
                chunk_id += 1;
            }

            start_index += (chunks_offsets.len() as u64) * chunk_size;
        }

        Ok(indexes)
    }

    fn push_indexes(query_bitmaps: &[&T], chunk_id: u64, chunk_size: u64, start_index: u64, end_index: u64, indexes: &mut Vec<u64>)
    {
        let offset_index: u64 = chunk_id * chunk_size;
        if offset_index + chunk_size < start_index || offset_index > end_index {
            return;
        }
        let mut b_result: T = query_bitmaps[0].clone();
        for i in 1..query_bitmaps.len() {
            b_result = (&b_result) & query_bitmaps[i];
        }
        // let new_indexes: Vec<u64> = b_result.unroll_bitmap().iter()
        //                .map(|idx| offset_index + *idx as u64)
        //                .filter(|idx| *idx >= start_index && *idx <= end_index).collect();
        indexes.extend(b_result.unroll_bitmap().iter()
                       .map(|idx| offset_index + *idx as u64)
                       .filter(|idx| *idx >= start_index && *idx <= end_index)
        );
    }

    fn get_query_i_bitmaps(block_info: &BlockInfo, value: U) -> Vec<usize> {
        let mut query_i_bitmaps: Vec<usize> = Vec::new();
        
        let f = |i_bitmap| {
            query_i_bitmaps.push(i_bitmap);
        };
        Self::run_f_on_i_bitmaps(block_info, value, f);
        query_i_bitmaps
    }

}
