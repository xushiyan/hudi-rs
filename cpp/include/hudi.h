#pragma once

#include "rust/cxx.h"
#include "rust/cxx_async.h"

struct HudiTable;
struct RecordBatchVec;

CXXASYNC_DEFINE_FUTURE(HudiTable, hudi, ffi, RustFutureHudiTable);
CXXASYNC_DEFINE_FUTURE(RecordBatchVec, hudi, ffi, RustFutureBatchVec);
