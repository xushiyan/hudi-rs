use hudi::table::Table;
use hudi_test::QuickstartTripsTable;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("arrow_bridge.h");
        type ArrowArrayPtr;
        type ArrowSchemaPtr;
    }

    #[derive(Debug)]
    struct ArrowRecordBatch {
        array: *mut ArrowArrayPtr,
        schema: *mut ArrowSchemaPtr,
    }

    extern "Rust" {
        fn read_file_slice() -> Vec<ArrowRecordBatch>;
    }
}

pub fn read_file_slice() -> Vec<ffi::ArrowRecordBatch> {
    let base_url = QuickstartTripsTable::V6Trips8I1U.url_to_mor_avro();
    let hudi_table = Table::new_sync(base_url.path()).unwrap();

    let records = hudi_table.read_snapshot_sync(&[]).unwrap();
    let batch = records.get(0).unwrap();

    // Convert to FFI representation
    let array_data = batch.column(0).to_data();
    let (array_ffi, schema_ffi) = arrow::ffi::to_ffi(&array_data).unwrap();

    // Box the FFI objects and get raw pointers to pass to C++
    let array_ptr = Box::into_raw(Box::new(array_ffi));
    let schema_ptr = Box::into_raw(Box::new(schema_ffi));

    // Create the ArrowRecordBatch and return
    vec![ffi::ArrowRecordBatch {
        array: array_ptr as *mut ffi::ArrowArrayPtr,
        schema: schema_ptr as *mut ffi::ArrowSchemaPtr,
    }]
}