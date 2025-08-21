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
 * Represents a byte range within a file, defined by start and end positions.
 * 
 * <p>Used to describe locations of data within split files, particularly
 * for hotcache metadata and field-specific data ranges.
 */
public class ByteRange {
    
    private final long start;
    private final long end;
    
    /**
     * Creates a new byte range.
     * 
     * @param start Starting byte position (inclusive)
     * @param end Ending byte position (exclusive)
     * @throws IllegalArgumentException if start < 0, end < 0, or start > end
     */
    public ByteRange(long start, long end) {
        if (start < 0) {
            throw new IllegalArgumentException("Start position cannot be negative: " + start);
        }
        if (end < 0) {
            throw new IllegalArgumentException("End position cannot be negative: " + end);
        }
        if (start > end) {
            throw new IllegalArgumentException("Start position cannot be greater than end: " + start + " > " + end);
        }
        
        this.start = start;
        this.end = end;
    }
    
    /**
     * Gets the starting byte position (inclusive).
     * 
     * @return Start position
     */
    public long getStart() {
        return start;
    }
    
    /**
     * Gets the ending byte position (exclusive).
     * 
     * @return End position
     */
    public long getEnd() {
        return end;
    }
    
    /**
     * Gets the size of this byte range.
     * 
     * @return Number of bytes in the range
     */
    public long getSize() {
        return end - start;
    }
    
    /**
     * Checks if this range is empty (size = 0).
     * 
     * @return true if range is empty, false otherwise
     */
    public boolean isEmpty() {
        return start == end;
    }
    
    /**
     * Checks if this range contains the given byte position.
     * 
     * @param position Byte position to check
     * @return true if position is within this range [start, end)
     */
    public boolean contains(long position) {
        return position >= start && position < end;
    }
    
    /**
     * Checks if this range overlaps with another range.
     * 
     * @param other The other byte range
     * @return true if ranges overlap, false otherwise
     */
    public boolean overlaps(ByteRange other) {
        Objects.requireNonNull(other, "Other range cannot be null");
        return start < other.end && end > other.start;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        ByteRange byteRange = (ByteRange) obj;
        return start == byteRange.start && end == byteRange.end;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }
    
    @Override
    public String toString() {
        return String.format("ByteRange{start=%d, end=%d, size=%d}", start, end, getSize());
    }
}