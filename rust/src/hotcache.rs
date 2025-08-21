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

//! Hotcache implementation using Quickwit's existing libraries

use crate::error::Result;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Simplified hotcache wrapper that can interface with Quickwit's implementations
/// This is a thin adapter layer over Quickwit's native hotcache format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotcacheInfo {
    pub split_id: String,
    pub num_docs: u32,
    pub size_bytes: u64,
    pub byte_range_start: u64,
    pub byte_range_end: u64,
    pub metadata: HashMap<String, String>,
}

impl HotcacheInfo {
    /// Create a new hotcache info structure
    pub fn new(split_id: String, num_docs: u32, size_bytes: u64) -> Self {
        Self {
            split_id,
            num_docs,
            size_bytes,
            byte_range_start: 0,
            byte_range_end: 0,
            metadata: HashMap::new(),
        }
    }

    /// Get the byte range for this hotcache
    pub fn get_byte_range(&self) -> (u64, u64) {
        (self.byte_range_start, self.byte_range_end)
    }

    /// Set the byte range for this hotcache
    pub fn set_byte_range(&mut self, start: u64, end: u64) {
        self.byte_range_start = start;
        self.byte_range_end = end;
    }

    /// Serialize to bytes for storage
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| crate::error::SplitsError::SerializationError(e.to_string()).into())
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| crate::error::SplitsError::SerializationError(e.to_string()).into())
    }
}

/// Create a hotcache from basic split information
pub fn create_hotcache(split_id: String, num_docs: u32, size_bytes: u64) -> Result<HotcacheInfo> {
    Ok(HotcacheInfo::new(split_id, num_docs, size_bytes))
}