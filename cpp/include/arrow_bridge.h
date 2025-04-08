#pragma once

#include <cstdint>
#include "arrow/c/api.h"  // For ArrowArray and ArrowSchema

#ifdef __cplusplus
extern "C" {
#endif

// Wrapper structs to match Rust side
struct ArrowArrayPtr {
    struct ArrowArray array;
};

struct ArrowSchemaPtr {
    struct ArrowSchema schema;
};

#ifdef __cplusplus
}
#endif