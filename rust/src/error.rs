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

//! Error handling for Quickwit Splits4Java operations

use std::fmt;

/// Result type for Quickwit splits operations
pub type Result<T> = std::result::Result<T, SplitsError>;

/// Error types for Quickwit splits operations
#[derive(Debug)]
pub enum SplitsError {
    /// IO operation failed
    Io(std::io::Error),
    
    /// Tantivy operation failed
    Tantivy(tantivy::TantivyError),
    
    /// Serialization/deserialization failed
    Serialization(String),
    
    /// Invalid split format or structure
    InvalidSplit(String),
    
    /// Field not found or invalid
    FieldError(String),
    
    /// Invalid operation or state
    InvalidOperation(String),
    
    /// JNI operation failed
    Jni(String),
}

impl fmt::Display for SplitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SplitsError::Io(err) => write!(f, "IO error: {}", err),
            SplitsError::Tantivy(err) => write!(f, "Tantivy error: {}", err),
            SplitsError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            SplitsError::InvalidSplit(msg) => write!(f, "Invalid split: {}", msg),
            SplitsError::FieldError(msg) => write!(f, "Field error: {}", msg),
            SplitsError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            SplitsError::Jni(msg) => write!(f, "JNI error: {}", msg),
        }
    }
}

impl std::error::Error for SplitsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SplitsError::Io(err) => Some(err),
            SplitsError::Tantivy(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SplitsError {
    fn from(err: std::io::Error) -> Self {
        SplitsError::Io(err)
    }
}

impl From<tantivy::TantivyError> for SplitsError {
    fn from(err: tantivy::TantivyError) -> Self {
        SplitsError::Tantivy(err)
    }
}

impl From<bincode::Error> for SplitsError {
    fn from(err: bincode::Error) -> Self {
        SplitsError::Serialization(err.to_string())
    }
}

impl From<serde_json::Error> for SplitsError {
    fn from(err: serde_json::Error) -> Self {
        SplitsError::Serialization(err.to_string())
    }
}

/// Convert SplitsError to a JNI exception class name
pub fn error_to_exception_class(err: &SplitsError) -> &'static str {
    match err {
        SplitsError::Io(_) => "java/io/IOException",
        SplitsError::Tantivy(_) => "java/lang/RuntimeException",
        SplitsError::Serialization(_) => "java/lang/RuntimeException",
        SplitsError::InvalidSplit(_) => "java/io/IOException",
        SplitsError::FieldError(_) => "java/lang/IllegalArgumentException",
        SplitsError::InvalidOperation(_) => "java/lang/IllegalStateException",
        SplitsError::Jni(_) => "java/lang/RuntimeException",
    }
}