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

/// Cached ratios derived from a single base file sample.
/// Used to estimate `byte_size` and `num_records` for all files.
///
/// TODO: this assumes base file format is Parquet. It should support other base file format as the table supports.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_normal() {
        // 100 bytes/row on disk, 2.5x compression ratio
        let estimator = FileStatsEstimator::new(100.0, 2.5);
        let (byte_size, num_records) = estimator.estimate(1000);
        assert_eq!(byte_size, 2500); // 1000 * 2.5
        assert_eq!(num_records, 10); // 1000 / 100

        // No compression (ratio = 1.0)
        let estimator = FileStatsEstimator::new(200.0, 1.0);
        let (byte_size, num_records) = estimator.estimate(2000);
        assert_eq!(byte_size, 2000); // same as on-disk
        assert_eq!(num_records, 10); // 2000 / 200
    }

    #[test]
    fn test_estimate_edge_cases() {
        // Zero avg_row_size → num_records should be 0
        let estimator = FileStatsEstimator::new(0.0, 2.0);
        let (byte_size, num_records) = estimator.estimate(500);
        assert_eq!(byte_size, 1000);
        assert_eq!(num_records, 0);

        // Zero size file
        let estimator = FileStatsEstimator::new(100.0, 2.0);
        let (byte_size, num_records) = estimator.estimate(0);
        assert_eq!(byte_size, 0);
        assert_eq!(num_records, 0);
    }
}
