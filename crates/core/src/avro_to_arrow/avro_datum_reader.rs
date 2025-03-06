/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use apache_avro::schema::Schema;
use apache_avro::types::Value;
use apache_avro::{from_avro_datum, AvroResult};
use std::io::Read;

pub struct AvroDatumReader<'a, R> {
    reader: R,
    writer_schema: &'a Schema,
    reader_schema: Option<&'a Schema>,
    errored: bool,
}

impl<'a, R: Read> AvroDatumReader<'a, R> {
    pub fn new(reader: R, writer_schema: &'a Schema, reader_schema: Option<&'a Schema>) -> Self {
        Self {
            reader,
            writer_schema,
            reader_schema,
            errored: false,
        }
    }

    fn read_next(&mut self) -> AvroResult<Value> {
        from_avro_datum(self.writer_schema, &mut self.reader, self.reader_schema)
    }
}

impl<'a, R: Read> Iterator for AvroDatumReader<'a, R> {
    type Item = AvroResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        let result = self.read_next();
        if result.is_err() {
            self.errored = true;
        }
        Some(result)
    }
}
