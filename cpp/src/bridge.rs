// src/bridge.rs
use arrow::array::RecordBatch;
use arrow_array::Int32Array;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

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
    // Create a simple Int32Array with values [1, 2, 3]
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
    let array = Arc::new(Int32Array::from(vec![1, 2, 3]));

    // Create a RecordBatch from the array
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();

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