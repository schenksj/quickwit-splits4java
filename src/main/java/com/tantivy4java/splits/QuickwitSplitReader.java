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

package com.tantivy4java.splits;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

/**
 * Reader for accessing Quickwit split data and metadata.
 * 
 * <p>A QuickwitSplitReader provides efficient access to:
 * <ul>
 *   <li>Hotcache metadata for fast field information lookup</li>
 *   <li>Posting lists for term-based queries</li>
 *   <li>Fast field data for document ranges</li>
 *   <li>Split file information and structure</li>
 * </ul>
 * 
 * <p>This class is thread-safe for concurrent read operations after
 * successful construction and hotcache loading.
 * 
 * <p>Example usage:
 * <pre>{@code
 * try (QuickwitSplitReader reader = new QuickwitSplitReader(Paths.get("path/to/split"))) {
 *     HotcacheInfo hotcache = reader.getHotcacheInfo();
 *     System.out.println("Split ID: " + hotcache.getSplitId());
 *     
 *     int[] docs = reader.readPostingList("title", "quickwit");
 *     System.out.println("Found " + docs.length + " documents");
 * }
 * }</pre>
 */
public class QuickwitSplitReader implements AutoCloseable {
    
    private long nativeHandle;
    private final Path splitPath;
    private boolean closed = false;
    
    /**
     * Opens a Quickwit split for reading.
     * 
     * <p>This constructor:
     * <ol>
     *   <li>Validates the split directory structure</li>
     *   <li>Loads hotcache metadata from the split footer</li>
     *   <li>Initializes native resources for efficient data access</li>
     * </ol>
     * 
     * @param splitPath Path to the split directory
     * @throws NullPointerException if splitPath is null
     * @throws IOException if split cannot be opened or is invalid
     */
    public QuickwitSplitReader(Path splitPath) throws IOException {
        Objects.requireNonNull(splitPath, "Split path cannot be null");
        
        if (!Files.exists(splitPath)) {
            throw new IOException("Split path does not exist: " + splitPath);
        }
        if (!Files.isDirectory(splitPath)) {
            throw new IOException("Split path is not a directory: " + splitPath);
        }
        
        this.splitPath = splitPath;
        this.nativeHandle = openNative(splitPath.toString());
        
        if (this.nativeHandle == 0) {
            throw new IOException("Failed to open split: " + splitPath);
        }
    }
    
    /**
     * Gets the hotcache information for this split.
     * 
     * <p>The hotcache contains metadata about fields, document counts,
     * and byte ranges for efficient data access. This information is
     * loaded when the split is opened and cached for subsequent calls.
     * 
     * @return Hotcache information, or null if not available
     * @throws IllegalStateException if reader is closed
     */
    public HotcacheInfo getHotcacheInfo() {
        ensureNotClosed();
        return getHotcacheInfoNative(nativeHandle);
    }
    
    /**
     * Lists all segment files in this split.
     * 
     * <p>Returns the names of all files that make up the split's segment,
     * including store files, term dictionaries, fast fields, and indexes.
     * 
     * @return List of segment file names
     * @throws IllegalStateException if reader is closed
     */
    public List<String> listSegmentFiles() {
        ensureNotClosed();
        return listSegmentFilesNative(nativeHandle);
    }
    
    /**
     * Reads the posting list for a given field and term.
     * 
     * <p>A posting list contains the document IDs where the specified term
     * appears in the given field. The returned array is sorted in ascending
     * order by document ID.
     * 
     * @param field Field name to search in
     * @param term Term to find
     * @return Array of document IDs containing the term, empty if none found
     * @throws NullPointerException if field or term is null
     * @throws IllegalArgumentException if field or term is empty, or field doesn't exist
     * @throws IllegalStateException if reader is closed
     */
    public int[] readPostingList(String field, String term) {
        Objects.requireNonNull(field, "Field cannot be null");
        Objects.requireNonNull(term, "Term cannot be null");
        
        if (field.isEmpty()) {
            throw new IllegalArgumentException("Field cannot be empty");
        }
        if (term.isEmpty()) {
            throw new IllegalArgumentException("Term cannot be empty");
        }
        
        ensureNotClosed();
        return readPostingListNative(nativeHandle, field, term);
    }
    
    /**
     * Gets fast field data for a document range.
     * 
     * <p>Fast fields provide efficient access to stored field values by
     * document ID. The returned data format depends on the field type
     * and encoding used in the original index.
     * 
     * @param field Field name to read
     * @param startDoc Starting document ID (inclusive)
     * @param endDoc Ending document ID (exclusive)
     * @return Raw field data as byte array
     * @throws NullPointerException if field is null
     * @throws IllegalArgumentException if field is empty, doesn't exist, or document range is invalid
     * @throws IllegalStateException if reader is closed
     */
    public byte[] getFastFieldData(String field, int startDoc, int endDoc) {
        Objects.requireNonNull(field, "Field cannot be null");
        
        if (field.isEmpty()) {
            throw new IllegalArgumentException("Field cannot be empty");
        }
        if (startDoc < 0) {
            throw new IllegalArgumentException("Start document cannot be negative: " + startDoc);
        }
        if (endDoc < startDoc) {
            throw new IllegalArgumentException("End document cannot be less than start: " + endDoc + " < " + startDoc);
        }
        
        ensureNotClosed();
        return getFastFieldDataNative(nativeHandle, field, startDoc, endDoc);
    }
    
    /**
     * Gets the split directory path.
     * 
     * @return Path to the split directory
     */
    public Path getSplitPath() {
        return splitPath;
    }
    
    /**
     * Checks if this reader has been closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Closes this reader and releases native resources.
     * 
     * <p>After calling this method, no other methods should be called on this instance.
     * This method is idempotent and safe to call multiple times.
     */
    @Override
    public void close() {
        if (!closed && nativeHandle != 0) {
            closeNative(nativeHandle);
            nativeHandle = 0;
            closed = true;
        }
    }
    
    /**
     * Ensures this reader has not been closed.
     * 
     * @throws IllegalStateException if the reader is closed
     */
    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("Split reader has been closed");
        }
    }
    
    // Native method declarations
    private native long openNative(String splitPath) throws IOException;
    private native HotcacheInfo getHotcacheInfoNative(long handle);
    private native List<String> listSegmentFilesNative(long handle);
    private native int[] readPostingListNative(long handle, String field, String term);
    private native byte[] getFastFieldDataNative(long handle, String field, int startDoc, int endDoc);
    private native void closeNative(long handle);
    
    /**
     * Finalizer to ensure native resources are cleaned up.
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }
}