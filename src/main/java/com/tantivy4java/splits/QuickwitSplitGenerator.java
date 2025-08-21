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

import com.tantivy4java.Index;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Generator for creating Quickwit splits from Tantivy indices.
 * 
 * <p>A Quickwit split is a self-contained unit of indexed data that includes:
 * <ul>
 *   <li>All segments merged into a single segment</li>
 *   <li>Hotcache metadata for efficient field access</li>
 *   <li>Proper file organization for distributed search</li>
 * </ul>
 * 
 * <p>This class is thread-safe for read operations but split generation
 * should be synchronized externally if multiple threads need to generate
 * splits from the same generator instance.
 * 
 * <p>Example usage:
 * <pre>{@code
 * try (Index index = Index.open("path/to/index")) {
 *     try (QuickwitSplitGenerator generator = new QuickwitSplitGenerator(index, 1_000_000)) {
 *         SplitMetadata metadata = generator.generateSplit(Paths.get("output/split"));
 *         System.out.println("Generated split: " + metadata.getSplitId());
 *     }
 * }
 * }</pre>
 */
public class QuickwitSplitGenerator implements AutoCloseable {
    
    private long nativeHandle;
    private final Index index;
    private final int targetDocsPerSplit;
    private boolean closed = false;
    
    /**
     * Creates a new split generator for the given index.
     * 
     * @param index The Tantivy index to generate splits from
     * @param targetDocsPerSplit Target number of documents per split (must be > 0)
     * @throws NullPointerException if index is null
     * @throws IllegalArgumentException if targetDocsPerSplit <= 0
     * @throws RuntimeException if native initialization fails
     */
    public QuickwitSplitGenerator(Index index, int targetDocsPerSplit) {
        Objects.requireNonNull(index, "Index cannot be null");
        if (targetDocsPerSplit <= 0) {
            throw new IllegalArgumentException("Target docs per split must be positive, got: " + targetDocsPerSplit);
        }
        
        this.index = index;
        this.targetDocsPerSplit = targetDocsPerSplit;
        this.nativeHandle = createNative(index.getNativePtr(), targetDocsPerSplit);
        
        if (this.nativeHandle == 0) {
            throw new RuntimeException("Failed to initialize native split generator");
        }
    }
    
    /**
     * Generates a Quickwit split from the current state of the index.
     * 
     * <p>This method:
     * <ol>
     *   <li>Merges all segments in the index into a single segment</li>
     *   <li>Generates hotcache metadata for efficient field access</li>
     *   <li>Copies segment files to the output directory</li>
     *   <li>Embeds hotcache as a footer in the appropriate files</li>
     * </ol>
     * 
     * @param outputPath Directory where the split files will be written
     * @return Metadata describing the generated split
     * @throws NullPointerException if outputPath is null
     * @throws IOException if split generation fails or output path is invalid
     * @throws IllegalStateException if generator is closed
     */
    public SplitMetadata generateSplit(Path outputPath) throws IOException {
        Objects.requireNonNull(outputPath, "Output path cannot be null");
        ensureNotClosed();
        
        // Ensure parent directory exists
        Path parent = outputPath.getParent();
        if (parent != null && !parent.toFile().exists()) {
            throw new IOException("Parent directory does not exist: " + parent);
        }
        
        return generateSplitNative(nativeHandle, outputPath.toString());
    }
    
    /**
     * Gets the target number of documents per split.
     * 
     * @return The target docs per split configured for this generator
     */
    public int getTargetDocsPerSplit() {
        return targetDocsPerSplit;
    }
    
    /**
     * Gets the index associated with this generator.
     * 
     * @return The index this generator operates on
     */
    public Index getIndex() {
        return index;
    }
    
    /**
     * Checks if this generator has been closed.
     * 
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Closes this generator and releases native resources.
     * 
     * <p>After calling this method, no other methods should be called on this instance.
     * This method is idempotent and safe to call multiple times.
     */
    @Override
    public void close() {
        if (!closed && nativeHandle != 0) {
            destroyNative(nativeHandle);
            nativeHandle = 0;
            closed = true;
        }
    }
    
    /**
     * Ensures this generator has not been closed.
     * 
     * @throws IllegalStateException if the generator is closed
     */
    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("Split generator has been closed");
        }
    }
    
    // Native method declarations
    private native long createNative(long indexHandle, int targetDocsPerSplit);
    private native SplitMetadata generateSplitNative(long handle, String outputPath) throws IOException;
    private native void destroyNative(long handle);
    
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