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

/// Cached ratios derived from a single Parquet footer sample.
/// Used to estimate `byte_size` and `num_records` for all files.
#[derive(Clone, Debug)]
pub(crate) struct FileStatsEstimator {
    /// Average row size on disk in bytes (compressed).
    avg_row_size_on_disk: f64,
    /// Ratio of uncompressed to compressed size.
    compression_ratio: f64,
}

impl FileStatsEstimator {
    pub(crate) fn new(avg_row_size_on_disk: f64, compression_ratio: f64) -> Self {
        Self {
            avg_row_size_on_disk,
            compression_ratio,
        }
    }

    /// Estimate metadata fields from on-disk size.
    pub(crate) fn estimate(&self, size: u64) -> (i64, i64) {
        let byte_size = (size as f64 * self.compression_ratio) as i64;
        let num_records = if self.avg_row_size_on_disk > 0.0 {
            (size as f64 / self.avg_row_size_on_disk) as i64
        } else {
            0
        };
        (byte_size, num_records)
    }
}
