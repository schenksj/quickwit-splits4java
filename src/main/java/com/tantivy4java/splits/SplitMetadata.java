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

import java.util.Objects;

/**
 * Metadata describing a generated Quickwit split.
 * 
 * <p>Contains essential information about the split including:
 * <ul>
 *   <li>Unique split identifier</li>
 *   <li>Document count</li>
 *   <li>Size information</li>
 *   <li>Hotcache location for efficient access</li>
 * </ul>
 */
public class SplitMetadata {
    
    private final String splitId;
    private final int numDocs;
    private final long sizeBytes;
    private final ByteRange hotcacheRange;
    
    /**
     * Creates new split metadata.
     * 
     * @param splitId Unique identifier for the split
     * @param numDocs Number of documents in the split
     * @param sizeBytes Total size of split files in bytes
     * @param hotcacheRange Byte range where hotcache metadata is stored
     * @throws NullPointerException if splitId or hotcacheRange is null
     * @throws IllegalArgumentException if numDocs or sizeBytes is negative
     */
    public SplitMetadata(String splitId, int numDocs, long sizeBytes, ByteRange hotcacheRange) {
        this.splitId = Objects.requireNonNull(splitId, "Split ID cannot be null");
        this.hotcacheRange = Objects.requireNonNull(hotcacheRange, "Hotcache range cannot be null");
        
        if (numDocs < 0) {
            throw new IllegalArgumentException("Number of docs cannot be negative: " + numDocs);
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("Size bytes cannot be negative: " + sizeBytes);
        }
        
        this.numDocs = numDocs;
        this.sizeBytes = sizeBytes;
    }
    
    /**
     * Gets the unique split identifier.
     * 
     * @return The split ID (typically a UUID)
     */
    public String getSplitId() {
        return splitId;
    }
    
    /**
     * Gets the number of documents in this split.
     * 
     * @return Document count
     */
    public int getNumDocs() {
        return numDocs;
    }
    
    /**
     * Gets the total size of split files in bytes.
     * 
     * @return Size in bytes
     */
    public long getSizeBytes() {
        return sizeBytes;
    }
    
    /**
     * Gets the byte range where hotcache metadata is stored.
     * 
     * @return Hotcache byte range
     */
    public ByteRange getHotcacheRange() {
        return hotcacheRange;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        SplitMetadata that = (SplitMetadata) obj;
        return numDocs == that.numDocs &&
               sizeBytes == that.sizeBytes &&
               Objects.equals(splitId, that.splitId) &&
               Objects.equals(hotcacheRange, that.hotcacheRange);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(splitId, numDocs, sizeBytes, hotcacheRange);
    }
    
    @Override
    public String toString() {
        return String.format("SplitMetadata{splitId='%s', numDocs=%d, sizeBytes=%d, hotcacheRange=%s}",
                           splitId, numDocs, sizeBytes, hotcacheRange);
    }
}