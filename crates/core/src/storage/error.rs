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
use thiserror::Error;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Failed to create storage: {0}")]
    Creation(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePathError(#[from] object_store::path::Error),

    #[error(transparent)]
    ReaderError(#[from] std::io::Error),

    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}
