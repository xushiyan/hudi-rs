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
use crate::file_group::log_file::log_format::LogFormatVersion;
use crate::Result;
use crate::schema::schema_for_delete_record_list;
use arrow_array::RecordBatch;
use bytes::{Buf, Bytes};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read, Seek};
use std::path::PathBuf;
use std::str::FromStr;
use arrow_schema::SchemaRef;
use parquet::data_type::AsBytes;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum BlockType {
    Command = 0,
    Delete = 1,
    Corrupted = 2,
    AvroData = 3,
    HfileData = 4,
    ParquetData = 5,
    CdcData = 6,
}

impl AsRef<str> for BlockType {
    fn as_ref(&self) -> &str {
        match self {
            BlockType::Command => ":command",
            BlockType::Delete => ":delete",
            BlockType::Corrupted => ":corrupted",
            BlockType::AvroData => "avro",
            BlockType::HfileData => "hfile",
            BlockType::ParquetData => "parquet",
            BlockType::CdcData => "cdc",
        }
    }
}

impl FromStr for BlockType {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ":command" => Ok(BlockType::Command),
            ":delete" => Ok(BlockType::Delete),
            ":corrupted" => Ok(BlockType::Corrupted),
            "avro_data" => Ok(BlockType::AvroData),
            "hfile" => Ok(BlockType::HfileData),
            "parquet" => Ok(BlockType::ParquetData),
            "cdc" => Ok(BlockType::CdcData),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid block type: {s}"
            ))),
        }
    }
}

impl TryFrom<[u8; 4]> for BlockType {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        match value {
            0 => Ok(BlockType::Command),
            1 => Ok(BlockType::Delete),
            2 => Ok(BlockType::Corrupted),
            3 => Ok(BlockType::AvroData),
            4 => Ok(BlockType::HfileData),
            5 => Ok(BlockType::ParquetData),
            6 => Ok(BlockType::CdcData),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid block type: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockMetadataType {
    Header,
    Footer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BlockMetadataKey {
    InstantTime = 0,
    TargetInstantTime = 1,
    Schema = 2,
    CommandBlockType = 3,
    CompactedBlockTimes = 4,
    RecordPositions = 5,
    BlockIdentifier = 6,
}

impl TryFrom<[u8; 4]> for BlockMetadataKey {
    type Error = CoreError;

    fn try_from(value_bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let value = u32::from_be_bytes(value_bytes);
        match value {
            0 => Ok(Self::InstantTime),
            1 => Ok(Self::TargetInstantTime),
            2 => Ok(Self::Schema),
            3 => Ok(Self::CommandBlockType),
            4 => Ok(Self::CompactedBlockTimes),
            5 => Ok(Self::RecordPositions),
            6 => Ok(Self::BlockIdentifier),
            _ => Err(CoreError::LogFormatError(format!(
                "Invalid header key: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum CommandBlock {
    Rollback = 0,
}

impl FromStr for CommandBlock {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u32>() {
            Ok(0) => Ok(CommandBlock::Rollback),
            Ok(val) => Err(CoreError::LogFormatError(format!(
                "Invalid command block type value: {val}"
            ))),
            Err(e) => Err(CoreError::LogFormatError(format!(
                "Failed to parse command block type: {e}"
            ))),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LogBlock {
    pub format_version: LogFormatVersion,
    pub block_type: BlockType,
    pub header: HashMap<BlockMetadataKey, String>,
    pub record_batches: Vec<RecordBatch>,
    pub footer: HashMap<BlockMetadataKey, String>,
    pub skipped: bool,
}

impl LogBlock {
    pub fn decode_content(block_type: &BlockType, content: Vec<u8>) -> Result<Vec<RecordBatch>> {
        match block_type {
            BlockType::Command => Ok(Vec::new()),
            BlockType::ParquetData => {
                let content_bytes = Bytes::from(content);
                let parquet_reader = ParquetRecordBatchReader::try_new(content_bytes, 1024)?;
                let mut batches = Vec::new();
                for item in parquet_reader {
                    let batch = item.map_err(CoreError::ArrowError)?;
                    batches.push(batch);
                }
                Ok(batches)
            },
            BlockType::AvroData => {
                use arrow_avro::schema::Schema as AvroSchema;
                use serde_json;

                let schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("schemas")
                    .join("HoodieDeleteRecordList.avsc");
                let schema_content = std::fs::read(schema_path)?;
                let avro_schema: AvroSchema = serde_json::from_slice(schema_content.as_bytes())
                    .map_err(|e| CoreError::Schema(e.to_string()))?;
                println!("avro_schema: {:?}", avro_schema);

                use arrow_avro::codec::AvroField;
                use arrow_avro::reader::record::RecordDecoder;
                use arrow_avro::reader::Decoder;

                let root_field = AvroField::try_from(&avro_schema)
                    .map_err(|e| CoreError::Schema(e.to_string()))?;
                let record_decoder = RecordDecoder::try_new(root_field.data_type(), true)
                    .map_err(|e| CoreError::Schema(e.to_string()))?;
                let mut decoder = Decoder::new(record_decoder, 100);

                // get avro bytes
                let content_bytes = Bytes::from(content);
                println!("{:?}", &content_bytes[8..]);
                println!("total length {}", content_bytes.len());
                let mut content_reader = BufReader::new(Cursor::new(content_bytes));
                let mut size_buf = [0u8; 4];
                content_reader.read_exact(&mut size_buf)?;
                let format_version = u32::from_be_bytes(size_buf) as usize;
                println!("format_version: {}", format_version);

                let mut size_buf = [0u8; 4];
                content_reader.read_exact(&mut size_buf)?;
                let length = u32::from_be_bytes(size_buf) as usize;
                println!("length: {}", length);

                println!("pos to start decoding: {:?}", content_reader.stream_position());
                let mut size_buf = vec![0u8; length];
                content_reader.read_exact(&mut size_buf)?;

                let consumed = decoder.decode(size_buf.as_slice(), 100).map_err(|e| CoreError::Schema(e.to_string()))?;
                println!("consumed: {}", consumed);

                let batch = decoder.flush().map_err(|e| CoreError::Schema(e.to_string()))?;
                println!("batch: {:?}", batch);

                Ok(Vec::new())
            },
            #[cfg(feature = "datafusion")]
            BlockType::Delete => {
                use datafusion::datasource::avro_to_arrow::ReaderBuilder;
                use datafusion::datasource::avro_to_arrow::to_arrow_schema;
                use apache_avro::Reader;
                use apache_avro::from_avro_datum;
                use arrow_avro::reader::Decoder;

                let avro_schema = schema_for_delete_record_list()?;
                let schema = to_arrow_schema(&avro_schema)?;
                let schema = SchemaRef::new(schema);

                let content_bytes = Bytes::from(content);
                println!("{:?}", &content_bytes[8..]);
                println!("total length {}", content_bytes.len());
                let mut content_reader = BufReader::new(Cursor::new(content_bytes));
                let mut size_buf = [0u8; 4];
                content_reader.read_exact(&mut size_buf)?;
                let format_version = u32::from_be_bytes(size_buf) as usize;
                println!("format_version: {}", format_version);

                let mut size_buf = [0u8; 4];
                content_reader.read_exact(&mut size_buf)?;
                let length = u32::from_be_bytes(size_buf) as usize;
                println!("length: {}", length);

                println!("pos: {:?}", content_reader.stream_position());

                let avro_value = from_avro_datum(&avro_schema, &mut content_reader, None)?;
                println!("avro_value: {:?}", avro_value);

                // TODO: apply projection to avro reader
                // let reader = ReaderBuilder::new()
                //     .with_schema(schema)
                //     .with_batch_size(1024)
                //     .build(&mut content_reader)?;
                // let mut batches = Vec::new();
                // for batch in reader {
                //     batches.push(batch?);
                // }
                Ok(Vec::new())

            },
            _ => Err(CoreError::LogBlockError(format!(
                "Unsupported block type: {block_type:?}"
            ))),
        }
    }

    pub fn instant_time(&self) -> Result<&str> {
        let v = self
            .header
            .get(&BlockMetadataKey::InstantTime)
            .ok_or_else(|| CoreError::LogBlockError("Instant time not found".to_string()))?;
        Ok(v)
    }

    pub fn target_instant_time(&self) -> Result<&str> {
        if self.block_type != BlockType::Command {
            return Err(CoreError::LogBlockError(
                "Target instant time is only available for command blocks".to_string(),
            ));
        }
        let v = self
            .header
            .get(&BlockMetadataKey::TargetInstantTime)
            .ok_or_else(|| CoreError::LogBlockError("Target instant time not found".to_string()))?;
        Ok(v)
    }

    pub fn schema(&self) -> Result<&str> {
        let v = self
            .header
            .get(&BlockMetadataKey::Schema)
            .ok_or_else(|| CoreError::LogBlockError("Schema not found".to_string()))?;
        Ok(v)
    }

    pub fn command_block_type(&self) -> Result<CommandBlock> {
        if self.block_type != BlockType::Command {
            return Err(CoreError::LogBlockError(
                "Command block type is only available for command blocks".to_string(),
            ));
        }
        let v = self
            .header
            .get(&BlockMetadataKey::CommandBlockType)
            .ok_or_else(|| {
                CoreError::LogBlockError(
                    "Command block type not found for command block".to_string(),
                )
            })?;
        v.parse::<CommandBlock>()
    }

    pub fn is_rollback_block(&self) -> bool {
        matches!(self.command_block_type(), Ok(CommandBlock::Rollback))
    }

    pub fn is_delete_block(&self) -> bool {
        self.block_type == BlockType::Delete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    #[test]
    fn test_block_type_as_ref() {
        assert_eq!(BlockType::Command.as_ref(), ":command");
        assert_eq!(BlockType::Delete.as_ref(), ":delete");
        assert_eq!(BlockType::Corrupted.as_ref(), ":corrupted");
        assert_eq!(BlockType::AvroData.as_ref(), "avro");
        assert_eq!(BlockType::HfileData.as_ref(), "hfile");
        assert_eq!(BlockType::ParquetData.as_ref(), "parquet");
        assert_eq!(BlockType::CdcData.as_ref(), "cdc");
    }

    #[test]
    fn test_block_type_from_str() {
        assert_eq!(BlockType::from_str(":command").unwrap(), BlockType::Command);
        assert_eq!(BlockType::from_str(":delete").unwrap(), BlockType::Delete);
        assert_eq!(
            BlockType::from_str(":corrupted").unwrap(),
            BlockType::Corrupted
        );
        assert_eq!(
            BlockType::from_str("avro_data").unwrap(),
            BlockType::AvroData
        );
        assert_eq!(BlockType::from_str("hfile").unwrap(), BlockType::HfileData);
        assert_eq!(
            BlockType::from_str("parquet").unwrap(),
            BlockType::ParquetData
        );
        assert_eq!(BlockType::from_str("cdc").unwrap(), BlockType::CdcData);

        // Test invalid block type
        assert!(BlockType::from_str("invalid").is_err());
    }

    #[test]
    fn test_block_type_try_from_bytes() {
        assert_eq!(
            BlockType::try_from([0, 0, 0, 0]).unwrap(),
            BlockType::Command
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 1]).unwrap(),
            BlockType::Delete
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 2]).unwrap(),
            BlockType::Corrupted
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 3]).unwrap(),
            BlockType::AvroData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 4]).unwrap(),
            BlockType::HfileData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 5]).unwrap(),
            BlockType::ParquetData
        );
        assert_eq!(
            BlockType::try_from([0, 0, 0, 6]).unwrap(),
            BlockType::CdcData
        );

        // Test invalid block type
        assert!(BlockType::try_from([0, 0, 0, 7]).is_err());
    }

    #[test]
    fn test_block_metadata_key_try_from_bytes() {
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 0]).unwrap(),
            BlockMetadataKey::InstantTime
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 1]).unwrap(),
            BlockMetadataKey::TargetInstantTime
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 2]).unwrap(),
            BlockMetadataKey::Schema
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 3]).unwrap(),
            BlockMetadataKey::CommandBlockType
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 4]).unwrap(),
            BlockMetadataKey::CompactedBlockTimes
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 5]).unwrap(),
            BlockMetadataKey::RecordPositions
        );
        assert_eq!(
            BlockMetadataKey::try_from([0, 0, 0, 6]).unwrap(),
            BlockMetadataKey::BlockIdentifier
        );

        // Test invalid metadata key
        assert!(BlockMetadataKey::try_from([0, 0, 0, 7]).is_err());
    }

    #[test]
    fn test_decode_parquet_content() -> Result<()> {
        // Create sample parquet bytes
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Int64Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["a", "b", "c"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(names) as ArrayRef],
        )?;

        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Test decoding the parquet content
        let batches = LogBlock::decode_content(&BlockType::ParquetData, buf)?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

        // Test decoding with unsupported block type
        assert!(LogBlock::decode_content(&BlockType::AvroData, vec![]).is_err());

        Ok(())
    }

    #[test]
    fn test_valid_rollback_block() {
        assert_eq!(CommandBlock::from_str("0").unwrap(), CommandBlock::Rollback);
    }

    #[test]
    fn test_invalid_rollback_block() {
        assert!(matches!(
            CommandBlock::from_str("1"),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Invalid command block type value: 1")
        ));
        assert!(matches!(
            CommandBlock::from_str("invalid"),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Failed to parse command block type")
        ));
        assert!(matches!(
            CommandBlock::from_str(""),
            Err(CoreError::LogFormatError(msg)) if msg.contains("Failed to parse command block type")
        ));
    }
}
