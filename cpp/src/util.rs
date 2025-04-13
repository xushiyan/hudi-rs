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
use arrow_array::RecordBatch;
use arrow::ipc::writer;
use anyhow::anyhow;

// Helper function to serialize a batch to IPC format
pub fn serialize_batch_to_ipc(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();

    let mut writer = writer::StreamWriter::try_new(&mut buffer, &batch.schema())
        .map_err(|e| anyhow!("Failed to create batch: {}", e))?;

    writer.write(batch)
        .map_err(|e| anyhow!("Failed to write batch: {}", e))?;

    writer.finish()
        .map_err(|e| anyhow!("Failed to finish batch: {}", e))?;

    Ok(buffer)
}