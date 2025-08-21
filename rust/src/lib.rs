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

//! Quickwit Splits4Java - JNI bindings for Quickwit split generation and reading
//! 
//! This library provides Java Native Interface (JNI) bindings for creating and reading
//! Quickwit splits from Tantivy indices. It enables Java applications to generate
//! optimized split files with embedded hotcache metadata for efficient distributed search.

use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::collections::HashMap;

pub mod split_generator;
pub mod split_reader;
pub mod hotcache;
pub mod jni_bridge;
pub mod error;

use split_generator::QuickwitSplitGenerator;
use split_reader::QuickwitSplitReader;

/// Global registry for managing native object handles
/// This ensures proper cleanup and prevents memory leaks
static GENERATOR_REGISTRY: Lazy<Mutex<HashMap<i64, Box<QuickwitSplitGenerator>>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

static READER_REGISTRY: Lazy<Mutex<HashMap<i64, Box<QuickwitSplitReader>>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Generate a unique handle for native objects
fn generate_handle() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};
    static COUNTER: AtomicI64 = AtomicI64::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Register a split generator and return its handle
pub(crate) fn register_generator(generator: QuickwitSplitGenerator) -> i64 {
    let handle = generate_handle();
    let mut registry = GENERATOR_REGISTRY.lock().unwrap();
    registry.insert(handle, Box::new(generator));
    handle
}

/// Get a split generator by handle
pub(crate) fn get_generator(handle: i64) -> Option<std::sync::MutexGuard<'static, HashMap<i64, Box<QuickwitSplitGenerator>>>> {
    let registry = GENERATOR_REGISTRY.lock().ok()?;
    if registry.contains_key(&handle) {
        Some(registry)
    } else {
        None
    }
}

/// Unregister and destroy a split generator
pub(crate) fn unregister_generator(handle: i64) -> bool {
    let mut registry = GENERATOR_REGISTRY.lock().unwrap();
    registry.remove(&handle).is_some()
}

/// Register a split reader and return its handle
pub(crate) fn register_reader(reader: QuickwitSplitReader) -> i64 {
    let handle = generate_handle();
    let mut registry = READER_REGISTRY.lock().unwrap();
    registry.insert(handle, Box::new(reader));
    handle
}

/// Get a split reader by handle
pub(crate) fn get_reader(handle: i64) -> Option<std::sync::MutexGuard<'static, HashMap<i64, Box<QuickwitSplitReader>>>> {
    let registry = READER_REGISTRY.lock().ok()?;
    if registry.contains_key(&handle) {
        Some(registry)
    } else {
        None
    }
}

/// Unregister and destroy a split reader
pub(crate) fn unregister_reader(handle: i64) -> bool {
    let mut registry = READER_REGISTRY.lock().unwrap();
    registry.remove(&handle).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_generation() {
        let handle1 = generate_handle();
        let handle2 = generate_handle();
        assert_ne!(handle1, handle2);
        assert!(handle1 > 0);
        assert!(handle2 > 0);
    }

    #[test]
    fn test_generator_registry() {
        // This is a placeholder test - would need actual generator instance
        let handle = generate_handle();
        assert!(handle > 0);
    }
}