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
use crate::Result;
use apache_avro::schema::Schema as AvroSchema;
use std::fs::File;
use std::path::PathBuf;


pub fn schema_for_delete_record() -> Result<AvroSchema> {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("delete_record.avsc");
    let schema = AvroSchema::parse_reader(&mut File::open(schema_path)?)?;
    Ok(schema)
}

pub fn schema_for_delete_record_list() -> Result<AvroSchema> {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecordList.avsc");
    let schema = AvroSchema::parse_reader(&mut File::open(schema_path)?)?;
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use apache_avro::schema::{Name, ResolvedSchema};
    use super::*;

    #[test]
    fn test_schema_for_delete_record_list() {
        let schema = schema_for_delete_record_list().unwrap();
        let schema = ResolvedSchema::try_from(&schema).unwrap();
        let name = Name::from("org.apache.hudi.avro.model.HoodieDeleteRecord");
        let item_schema = schema.get_names().get(&name).unwrap().clone();
        let item_schema = ResolvedSchema::try_from(item_schema).unwrap();
        assert_eq!(item_schema.get_schemata().len(), 1);
    }

}