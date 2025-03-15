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
use crate::error::CoreError;
use crate::error::Result;
use apache_avro::schema::{Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema};
use apache_avro::types::Value;
use apache_avro::{from_avro_datum, Schema};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

/// Derive the schema for delete record in the delete block content.
///
/// # Notes
/// Current avro-arrow conversion does not support union type with more than 1 non-null types,
/// therefore we need to programmatically create the schema based on the ordering value's schema.
pub fn schema_for_delete_record(ordering_val_schema: &Schema) -> Result<Schema> {
    let name = Name {
        name: "HoodieDeleteRecord".to_string(),
        namespace: Some("org.apache.hudi.avro.model".to_string()),
    };

    // Create nullable string type for recordKey and partitionPath
    let nullable_string = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::String])?);

    // Create record fields
    let fields = vec![
        RecordField {
            name: "recordKey".to_string(),
            doc: Some("Record key in String".to_string()),
            default: Some(serde_json::Value::Null),
            schema: nullable_string.clone(),
            order: RecordFieldOrder::Ascending,
            position: 0,
            custom_attributes: BTreeMap::new(),
            aliases: None,
        },
        RecordField {
            name: "partitionPath".to_string(),
            doc: Some("Partition path in String".to_string()),
            default: Some(serde_json::Value::Null),
            schema: nullable_string,
            order: RecordFieldOrder::Ascending,
            position: 1,
            custom_attributes: BTreeMap::new(),
            aliases: None,
        },
        RecordField {
            name: "orderingVal".to_string(),
            doc: Some(
                "Ordering value determining the order of merging on the same key".to_string(),
            ),
            default: Some(serde_json::Value::Null),
            schema: ordering_val_schema.clone(),
            order: RecordFieldOrder::Ascending,
            position: 2,
            custom_attributes: BTreeMap::new(),
            aliases: None,
        },
    ];

    Ok(Schema::Record(RecordSchema {
        name,
        doc: Some(
            "Schema for Hudi delete record with key, partition and ordering fields".to_string(),
        ),
        fields,
        lookup: BTreeMap::from([
            ("recordKey".to_string(), 0),
            ("partitionPath".to_string(), 1),
            ("orderingVal".to_string(), 2),
        ]),
        attributes: BTreeMap::new(),
        aliases: None,
    }))
}

pub fn schema_for_delete_record_list() -> Result<Schema> {
    let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("HoodieDeleteRecordList.avsc");
    let schema = Schema::parse_reader(&mut File::open(schema_path)?)?;
    Ok(schema)
}
pub struct AvroDeleteBlockContentReader {
    datum_values: Vec<Value>,
}

impl AvroDeleteBlockContentReader {
    pub fn try_new(reader: &mut impl Read) -> Result<Self> {
        let writer_schema = schema_for_delete_record_list()?;
        let list_datum_value = from_avro_datum(&writer_schema, reader, None)?;
        match list_datum_value {
            Value::Record(fields) if fields.len() == 1 => {
                let (name, value) = &fields[0];

                if name != "deleteRecordList" {
                    Err(CoreError::InvalidValue(format!(
                        "Expected field name to be `deleteRecordList`, got `{}`",
                        name
                    )))
                } else if let Value::Array(vec) = value {
                    Ok(Self {
                        datum_values: vec.to_vec(),
                    })
                } else {
                    Err(CoreError::InvalidValue(
                        "Expected field `HoodieDeleteRecord` to be an array".to_string(),
                    ))
                }
            }
            Value::Record(fields) => Err(CoreError::InvalidValue(format!(
                "Expected exactly 1 field, got {}",
                fields.len()
            ))),
            _ => Err(CoreError::InvalidValue(
                "Expected a single record in the Avro file".to_string(),
            )),
        }
    }
}

impl Iterator for AvroDeleteBlockContentReader {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.datum_values.is_empty() {
            None
        } else {
            Some(self.datum_values.remove(0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_for_delete_record_list() {
        let schema = schema_for_delete_record_list().unwrap();
        match schema {
            Schema::Record(record_schema) => {
                assert_eq!(record_schema.name.name, "HoodieDeleteRecordList");
                assert_eq!(
                    record_schema.name.namespace,
                    Some("org.apache.hudi.avro.model".to_string())
                );
            }
            _ => panic!("Expected record schema"),
        }
    }
}
