use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use hudi::table::Table;
use hudi_test::QuickstartTripsTable;

#[cxx::bridge]
mod ffi {
    // Define a struct to hold the raw pointer to ArrowArrayStream
    unsafe extern "C++" {
        include!("arrow/c/abi.h");

        type ArrowArrayStream;
    }

    // Function to export to C++
    extern "Rust" {
        fn read_file_slice() -> *mut ArrowArrayStream;
    }
}

pub fn read_file_slice() -> *mut ffi::ArrowArrayStream {
    let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
    let hudi_table = Table::new_sync(base_url.path()).unwrap();

    let batches = hudi_table.read_snapshot_sync(&[]).unwrap();
    let schema = batches[0].schema();

    let batch_iterator = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    // Convert to FFI_ArrowArrayStream and get raw pointer
    let ffi_array_stream = FFI_ArrowArrayStream::new(Box::new(batch_iterator));
    let raw_ptr = Box::into_raw(Box::new(ffi_array_stream));

    raw_ptr as *mut ffi::ArrowArrayStream
}