mod util;

use cxx::{CxxString, CxxVector};
use std::future::Future;
use hudi::table::Table;
use cxx_async::CxxAsyncException;

#[cxx::bridge(namespace = "hudi::ffi")]
mod ffi {
    struct OptionTuple {
        key: String,
        value: String,
    }

    unsafe extern "C++" {
        include!("hudi.h");
        type RustFutureHudiTable = super::RustFutureHudiTable;
        type RustFutureBatchVec = super::RustFutureBatchVec;
    }

    extern "Rust" {
        type HudiTable;
        type RecordBatchVec;

        fn new_hudi_table(
            base_uri: &CxxString,
            options: &CxxVector<OptionTuple>
        ) -> RustFutureHudiTable;

        fn hudi_table_read_snapshot(
            table: &HudiTable,
        ) -> RustFutureBatchVec;
    }
}

#[cxx_async::bridge(namespace = hudi::ffi)]
unsafe impl Future for RustFutureHudiTable {
    type Output = HudiTable;
}

#[cxx_async::bridge(namespace = hudi::ffi)]
unsafe impl Future for RustFutureBatchVec {
    type Output = RecordBatchVec;
}

pub struct HudiTable {
    inner: Table
}

pub struct RecordBatchVec {
    batch_vec: Vec<Vec<u8>>,
}

fn new_hudi_table(
    base_uri: &CxxString,
    options: &CxxVector<ffi::OptionTuple>
) -> RustFutureHudiTable {
    let base_uri = base_uri.to_string();
    let options = options.iter()
        .map(|opt| (opt.key.to_string(), opt.value.to_string()))
        .collect::<Vec<_>>();

    RustFutureHudiTable::fallible(async move {

        return Table::new_with_options(&base_uri, options).await
            .map(|table| HudiTable { inner: table })
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()));
    })
}

fn hudi_table_read_snapshot(
    table: &HudiTable,
) -> RustFutureBatchVec {
    let table = table.inner.clone();

    RustFutureBatchVec::fallible(async move {
        let batches = table.read_snapshot(&[]).await
            .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;

        let mut batch_vec = Vec::with_capacity(batches.len());
        for batch in batches {
            let serialized_batch = util::serialize_batch_to_ipc(&batch)
                .map_err(|e| CxxAsyncException::new(e.to_string().into_boxed_str()))?;
            batch_vec.push(serialized_batch);
        }

        Ok(RecordBatchVec { batch_vec })
    })
}
