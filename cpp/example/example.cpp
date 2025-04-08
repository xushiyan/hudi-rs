#include <iostream>
#include <vector>
#include <memory>
#include "rust/cxx.h"
#include "hudi-cpp/src/bridge.rs.h"  // Generated header from cxx
#include "arrow/c/api.h"  // Arrow C Data Interface

// Utility function to print ArrowArray data
void print_arrow_array(const ArrowArray* array, const ArrowSchema* schema) {
    std::cout << "Schema format: " << schema->format << std::endl;
    std::cout << "Array length: " << array->length << std::endl;

    // Only handle int32 arrays for this example
    if (strcmp(schema->format, "i") == 0) {
        const int32_t* data = reinterpret_cast<const int32_t*>(array->buffers[1]);
        std::cout << "Values: ";
        for (int64_t i = 0; i < array->length; ++i) {
            std::cout << data[i] << " ";
        }
        std::cout << std::endl;
    } else {
        std::cout << "Unsupported format: " << schema->format << std::endl;
    }
}

int main() {
    try {
        std::cout << "Calling Rust function read_file_slice()..." << std::endl;

        // Call the Rust function
        auto result = read_file_slice();
        std::cout << "Received " << result.size() << " record batch(es)" << std::endl;

        if (result.size() > 0) {
            for (size_t i = 0; i < result.size(); ++i) {
                std::cout << "\nProcessing batch " << i << ":" << std::endl;

                // Get the raw pointers from the FFI struct
                ArrowArrayPtr* array_ptr = reinterpret_cast<ArrowArrayPtr*>(result[i].array);
                ArrowSchemaPtr* schema_ptr = reinterpret_cast<ArrowSchemaPtr*>(result[i].schema);

                // Print the array data
                print_arrow_array(&array_ptr->array, &schema_ptr->schema);

                // Release the memory (this will call the release callbacks)
                if (array_ptr->array.release) {
                    array_ptr->array.release(&array_ptr->array);
                }
                if (schema_ptr->schema.release) {
                    schema_ptr->schema.release(&schema_ptr->schema);
                }

                // Free the wrapper objects
                delete array_ptr;
                delete schema_ptr;
            }
        }

        std::cout << "\nSuccessfully processed Arrow data from Rust!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}