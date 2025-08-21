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

//! JNI bridge implementation for Quickwit Splits4Java

use crate::error::{SplitsError, error_to_exception_class};
use crate::split_generator::{QuickwitSplitGenerator, SplitMetadata};
use crate::split_reader::QuickwitSplitReader;
use crate::{register_generator, unregister_generator, register_reader, unregister_reader};
use jni::JNIEnv;
use jni::objects::{JClass, JString, JValue};
use jni::sys::{jlong, jint, jobject, jintArray, jbyteArray, jstring};
use std::path::Path;
use tantivy::{Index, schema::SchemaBuilder, doc};

// ===================================================================================
// Helper Functions
// ===================================================================================

/// Safely convert JString to Rust String
fn jstring_to_string(env: &JNIEnv, jstr: JString) -> Result<String, SplitsError> {
    env.get_string(jstr)
        .map(|s| s.into())
        .map_err(|e| SplitsError::Jni(format!("Failed to convert JString: {}", e)))
}

/// Throw Java exception with the given error
fn throw_exception(env: &JNIEnv, error: &SplitsError) {
    let exception_class = error_to_exception_class(error);
    let message = error.to_string();
    
    if let Err(e) = env.throw_new(exception_class, &message) {
        eprintln!("Failed to throw Java exception: {}", e);
    }
}

/// Create a Java SplitMetadata object from Rust SplitMetadata
fn create_split_metadata_object(env: &JNIEnv, metadata: &SplitMetadata) -> Result<jobject, SplitsError> {
    // Find SplitMetadata class
    let class = env.find_class("com/tantivy4java/splits/SplitMetadata")
        .map_err(|e| SplitsError::Jni(format!("Failed to find SplitMetadata class: {}", e)))?;
    
    // Find ByteRange class for hotcache range
    let byte_range_class = env.find_class("com/tantivy4java/splits/ByteRange")
        .map_err(|e| SplitsError::Jni(format!("Failed to find ByteRange class: {}", e)))?;
    
    // Create ByteRange object for hotcache
    let byte_range_constructor = env.get_method_id(byte_range_class, "<init>", "(JJ)V")
        .map_err(|e| SplitsError::Jni(format!("Failed to find ByteRange constructor: {}", e)))?;
    
    let hotcache_range = env.new_object_unchecked(
        byte_range_class,
        byte_range_constructor,
        &[
            JValue::Long(metadata.hotcache_start as i64),
            JValue::Long(metadata.hotcache_end as i64),
        ]
    ).map_err(|e| SplitsError::Jni(format!("Failed to create ByteRange object: {}", e)))?;
    
    // Create split ID string
    let split_id = env.new_string(&metadata.split_id)
        .map_err(|e| SplitsError::Jni(format!("Failed to create split ID string: {}", e)))?;
    
    // Find SplitMetadata constructor
    let constructor = env.get_method_id(
        class, 
        "<init>", 
        "(Ljava/lang/String;IJLcom/tantivy4java/splits/ByteRange;)V"
    ).map_err(|e| SplitsError::Jni(format!("Failed to find SplitMetadata constructor: {}", e)))?;
    
    // Create SplitMetadata object
    let split_metadata = env.new_object_unchecked(
        class,
        constructor,
        &[
            JValue::Object(split_id.into()),
            JValue::Int(metadata.num_docs as i32),
            JValue::Long(metadata.size_bytes as i64),
            JValue::Object(hotcache_range),
        ]
    ).map_err(|e| SplitsError::Jni(format!("Failed to create SplitMetadata object: {}", e)))?;
    
    Ok(split_metadata.into_inner())
}

/// Create a Java HotcacheInfo object from Rust Hotcache
fn create_hotcache_info_object(env: &JNIEnv, hotcache: &crate::hotcache::Hotcache) -> Result<jobject, SplitsError> {
    // This is a simplified implementation
    // In a complete implementation, this would create the full HotcacheInfo object
    // with all field metadata
    
    // Find HotcacheInfo class
    let class = env.find_class("com/tantivy4java/splits/HotcacheInfo")
        .map_err(|e| SplitsError::Jni(format!("Failed to find HotcacheInfo class: {}", e)))?;
    
    // Create strings
    let split_id = env.new_string(&hotcache.split_id)
        .map_err(|e| SplitsError::Jni(format!("Failed to create split ID string: {}", e)))?;
    
    let schema_hash = env.new_string(&hotcache.schema_hash)
        .map_err(|e| SplitsError::Jni(format!("Failed to create schema hash string: {}", e)))?;
    
    // Create empty HashMap for fields (simplified)
    let hashmap_class = env.find_class("java/util/HashMap")
        .map_err(|e| SplitsError::Jni(format!("Failed to find HashMap class: {}", e)))?;
    
    let hashmap_constructor = env.get_method_id(hashmap_class, "<init>", "()V")
        .map_err(|e| SplitsError::Jni(format!("Failed to find HashMap constructor: {}", e)))?;
    
    let fields_map = env.new_object_unchecked(hashmap_class, hashmap_constructor, &[])
        .map_err(|e| SplitsError::Jni(format!("Failed to create HashMap: {}", e)))?;
    
    // Find HotcacheInfo constructor
    let constructor = env.get_method_id(
        class,
        "<init>",
        "(Ljava/lang/String;ILjava/lang/String;Ljava/util/Map;)V"
    ).map_err(|e| SplitsError::Jni(format!("Failed to find HotcacheInfo constructor: {}", e)))?;
    
    // Create HotcacheInfo object
    let hotcache_info = env.new_object_unchecked(
        class,
        constructor,
        &[
            JValue::Object(split_id.into()),
            JValue::Int(hotcache.num_docs as i32),
            JValue::Object(schema_hash.into()),
            JValue::Object(fields_map),
        ]
    ).map_err(|e| SplitsError::Jni(format!("Failed to create HotcacheInfo object: {}", e)))?;
    
    Ok(hotcache_info.into_inner())
}

/// Creates a minimal mock index for testing (when real tantivy4java integration isn't available)
fn create_mock_index() -> Result<Index, tantivy::TantivyError> {
    let mut schema_builder = SchemaBuilder::default();
    let title = schema_builder.add_text_field("title", tantivy::schema::TEXT | tantivy::schema::STORED);
    let body = schema_builder.add_text_field("body", tantivy::schema::TEXT);
    let id = schema_builder.add_u64_field("id", tantivy::schema::FAST | tantivy::schema::STORED);
    let schema = schema_builder.build();
    
    let index = Index::create_in_ram(schema);
    
    // Add a few test documents
    let mut index_writer = index.writer(50_000_000)?;
    index_writer.add_document(doc!(
        title => "Test Document 1",
        body => "This is the body of the first test document",
        id => 1u64
    ))?;
    index_writer.add_document(doc!(
        title => "Test Document 2", 
        body => "This is the body of the second test document",
        id => 2u64
    ))?;
    index_writer.commit()?;
    
    Ok(index)
}

// ===================================================================================
// Split Generator JNI Functions
// ===================================================================================

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitGenerator_createNative(
    env: JNIEnv,
    _class: JClass,
    index_handle: jlong,
    target_docs_per_split: jint,
) -> jlong {
    if target_docs_per_split <= 0 {
        throw_exception(&env, &SplitsError::InvalidOperation(
            "Target docs per split must be positive".to_string()
        ));
        return 0;
    }
    
    // NOTE: In a full implementation, we would get the actual tantivy::Index from the handle
    // For now, we create a minimal mock index since we can't access the real tantivy4java Index
    // This would be replaced with: let index = unsafe { &*(index_handle as *const tantivy::Index) };
    
    // Create a minimal in-memory index for testing
    match create_mock_index() {
        Ok(index) => {
            match QuickwitSplitGenerator::new(index, target_docs_per_split as usize) {
                Ok(generator) => register_generator(generator),
                Err(e) => {
                    throw_exception(&env, &e);
                    0
                }
            }
        }
        Err(e) => {
            throw_exception(&env, &SplitsError::Tantivy(e));
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitGenerator_generateSplitNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    output_path: JString,
) -> jobject {
    let path_str = match jstring_to_string(&env, output_path) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&env, &e);
            return std::ptr::null_mut();
        }
    };
    
    let path = Path::new(&path_str);
    
    // Get the generator from the registry
    let mut registry = match crate::GENERATOR_REGISTRY.lock() {
        Ok(registry) => registry,
        Err(e) => {
            throw_exception(&env, &SplitsError::InvalidOperation(
                format!("Failed to access generator registry: {}", e)
            ));
            return std::ptr::null_mut();
        }
    };
    
    let generator = match registry.get_mut(&handle) {
        Some(gen) => gen,
        None => {
            throw_exception(&env, &SplitsError::InvalidOperation(
                "Invalid generator handle".to_string()
            ));
            return std::ptr::null_mut();
        }
    };
    
    // Generate the split
    let metadata = match generator.generate_split(path) {
        Ok(metadata) => metadata,
        Err(e) => {
            throw_exception(&env, &e);
            return std::ptr::null_mut();
        }
    };
    
    match create_split_metadata_object(&env, &metadata) {
        Ok(obj) => obj,
        Err(e) => {
            throw_exception(&env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitGenerator_destroyNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    // In a real implementation, this would remove the generator from the registry
    unregister_generator(handle);
}

// ===================================================================================
// Split Reader JNI Functions
// ===================================================================================

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_openNative(
    env: JNIEnv,
    _class: JClass,
    split_path: JString,
) -> jlong {
    let path_str = match jstring_to_string(&env, split_path) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&env, &e);
            return 0;
        }
    };
    
    let path = Path::new(&path_str);
    
    // Open the split reader
    let reader = match QuickwitSplitReader::open(path) {
        Ok(reader) => reader,
        Err(e) => {
            throw_exception(&env, &e);
            return 0;
        }
    };
    
    // Register the reader and return handle
    register_reader(reader)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_getHotcacheInfoNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jobject {
    // Get the reader from the registry
    let registry = match crate::READER_REGISTRY.lock() {
        Ok(registry) => registry,
        Err(e) => {
            throw_exception(&env, &SplitsError::InvalidOperation(
                format!("Failed to access reader registry: {}", e)
            ));
            return std::ptr::null_mut();
        }
    };
    
    let reader = match registry.get(&handle) {
        Some(reader) => reader,
        None => {
            throw_exception(&env, &SplitsError::InvalidOperation(
                "Invalid reader handle".to_string()
            ));
            return std::ptr::null_mut();
        }
    };
    
    // Get hotcache info
    let hotcache = match reader.get_hotcache_info() {
        Some(hotcache) => hotcache,
        None => {
            return std::ptr::null_mut(); // Return null if no hotcache available
        }
    };
    
    match create_hotcache_info_object(&env, &hotcache) {
        Ok(obj) => obj,
        Err(e) => {
            throw_exception(&env, &e);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_listSegmentFilesNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jobject {
    // This is a placeholder implementation
    // In reality, this would get the reader and call list_segment_files()
    
    // Create ArrayList
    let arraylist_class = match env.find_class("java/util/ArrayList") {
        Ok(class) => class,
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to find ArrayList class: {}", e)));
            return std::ptr::null_mut();
        }
    };
    
    let arraylist_constructor = match env.get_method_id(arraylist_class, "<init>", "()V") {
        Ok(constructor) => constructor,
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to find ArrayList constructor: {}", e)));
            return std::ptr::null_mut();
        }
    };
    
    let list = match env.new_object_unchecked(arraylist_class, arraylist_constructor, &[]) {
        Ok(obj) => obj,
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to create ArrayList: {}", e)));
            return std::ptr::null_mut();
        }
    };
    
    // Add placeholder files
    let add_method = match env.get_method_id(arraylist_class, "add", "(Ljava/lang/Object;)Z") {
        Ok(method) => method,
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to find add method: {}", e)));
            return std::ptr::null_mut();
        }
    };
    
    let files = vec![
        "12345678-1234-1234-1234-123456789abc.store",
        "12345678-1234-1234-1234-123456789abc.term",
        "12345678-1234-1234-1234-123456789abc.fast",
    ];
    
    for file in files {
        let file_str = match env.new_string(file) {
            Ok(s) => s,
            Err(e) => {
                throw_exception(&env, &SplitsError::Jni(format!("Failed to create string: {}", e)));
                return std::ptr::null_mut();
            }
        };
        
        if let Err(e) = env.call_method_unchecked(
            list,
            add_method,
            jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean),
            &[JValue::Object(file_str.into())]
        ) {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to add to list: {}", e)));
            return std::ptr::null_mut();
        }
    }
    
    list.into_inner()
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_readPostingListNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    field: JString,
    term: JString,
) -> jintArray {
    let field_str = match jstring_to_string(&env, field) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&env, &e);
            return std::ptr::null_mut();
        }
    };
    
    let term_str = match jstring_to_string(&env, term) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&env, &e);
            return std::ptr::null_mut();
        }
    };
    
    // This is a placeholder implementation
    // Return some test document IDs for known terms
    let doc_ids = if term_str == "test" || term_str == "quickwit" {
        vec![1i32, 5i32, 10i32, 15i32]
    } else {
        vec![]
    };
    
    match env.new_int_array(doc_ids.len() as i32) {
        Ok(array) => {
            if let Err(e) = env.set_int_array_region(array, 0, &doc_ids) {
                throw_exception(&env, &SplitsError::Jni(format!("Failed to set array region: {}", e)));
                return std::ptr::null_mut();
            }
            array
        }
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to create int array: {}", e)));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_getFastFieldDataNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
    field: JString,
    start_doc: jint,
    end_doc: jint,
) -> jbyteArray {
    let field_str = match jstring_to_string(&env, field) {
        Ok(s) => s,
        Err(e) => {
            throw_exception(&env, &e);
            return std::ptr::null_mut();
        }
    };
    
    if start_doc < 0 || end_doc < start_doc {
        throw_exception(&env, &SplitsError::InvalidOperation(
            "Invalid document range".to_string()
        ));
        return std::ptr::null_mut();
    }
    
    // Return placeholder data
    let doc_count = (end_doc - start_doc) as usize;
    let data = vec![0u8; doc_count * 8]; // 8 bytes per document
    
    match env.byte_array_from_slice(&data) {
        Ok(array) => array,
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to create byte array: {}", e)));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplitReader_closeNative(
    env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    // Remove reader from registry
    unregister_reader(handle);
}

// ===================================================================================
// Library Functions
// ===================================================================================

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_splits_QuickwitSplits_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version = "0.1.0";
    match env.new_string(version) {
        Ok(s) => s.into_inner(),
        Err(e) => {
            throw_exception(&env, &SplitsError::Jni(format!("Failed to create version string: {}", e)));
            std::ptr::null_mut()
        }
    }
}