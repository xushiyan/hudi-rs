// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::arrow_array_reader::AvroArrowArrayReader;
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use std::io::{Read, Seek};
use std::sync::Arc;

/// Avro file reader builder
#[derive(Debug)]
pub struct ReaderBuilder {
    /// Optional schema for the Avro file
    ///
    /// If the schema is not supplied, the reader will try to read the schema.
    schema: Option<SchemaRef>,
    /// Batch size (number of records to load each time)
    ///
    /// The default batch size when using the `ReaderBuilder` is 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (zero-based column indices)
    projection: Option<Vec<String>>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        Self {
            schema: None,
            batch_size: 1024,
            projection: None,
        }
    }
}

impl ReaderBuilder {
    /// Create a new builder for configuring Avro parsing options.
    ///
    /// To convert a builder into a reader, call `Reader::from_builder`
    ///
    /// # Example
    ///
    /// ```
    /// use std::fs::File;
    ///
    /// use datafusion::datasource::avro_to_arrow::{Reader, ReaderBuilder};
    ///
    /// fn example() -> Reader<'static, File> {
    ///     let file = File::open("test/data/basic.avro").unwrap();
    ///
    ///     // create a builder, inferring the schema with the first 100 records
    ///     let builder = ReaderBuilder::new()
    ///       .read_schema()
    ///       .with_batch_size(100);
    ///
    ///     let reader = builder
    ///       .build::<File>(file)
    ///       .unwrap();
    ///
    ///     reader
    /// }
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Avro file's schema
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the Avro reader to infer the schema of the file
    pub fn read_schema(mut self) -> Self {
        // remove any schema that is set
        self.schema = None;
        self
    }

    /// Set the batch size (number of records to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the reader's column projection
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Create a new `Reader` from the `ReaderBuilder`
    pub fn build<'a, R>(self, source: R) -> Result<Reader<'a, R>>
    where
        R: Read + Seek,
    {
        let mut source = source;

        // check if schema should be inferred
        let schema = match self.schema {
            Some(schema) => schema,
            None => Arc::new(super::read_avro_schema_from_reader(&mut source)?),
        };
        source.rewind()?;
        Reader::try_new(source, schema, self.batch_size, self.projection)
    }
}

/// Avro file record  reader
pub struct Reader<'a, R: Read> {
    array_reader: AvroArrowArrayReader<'a, R>,
    schema: SchemaRef,
    batch_size: usize,
}

impl<R: Read> Reader<'_, R> {
    /// Create a new Avro Reader from any value that implements the `Read` trait.
    ///
    /// If reading a `File`, you can customise the Reader, such as to enable schema
    /// inference, use `ReaderBuilder`.
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        batch_size: usize,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        Ok(Self {
            array_reader: AvroArrowArrayReader::try_new(reader, Arc::clone(&schema), projection)?,
            schema,
            batch_size,
        })
    }

    /// Returns the schema of the reader, useful for getting the schema without reading
    /// record batches
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl<R: Read> Iterator for Reader<'_, R> {
    type Item = ArrowResult<RecordBatch>;

    /// Returns the next batch of results (defined by `self.batch_size`), or `None` if there
    /// are no more results.
    fn next(&mut self) -> Option<Self::Item> {
        self.array_reader.next_batch(self.batch_size)
    }
}
