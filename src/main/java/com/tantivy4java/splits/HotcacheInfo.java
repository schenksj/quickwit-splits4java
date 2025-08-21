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

import java.util.Map;
import java.util.Objects;

/**
 * Hotcache information for a Quickwit split.
 * 
 * <p>The hotcache contains metadata that enables efficient access to split data:
 * <ul>
 *   <li>Split identification and document count</li>
 *   <li>Field metadata with type information and byte ranges</li>
 *   <li>Schema hash for compatibility validation</li>
 *   <li>Segment metadata for file organization</li>
 * </ul>
 * 
 * <p>This information is embedded as a footer in split files and loaded
 * when a split is opened for reading.
 */
public class HotcacheInfo {
    
    private final String splitId;
    private final int numDocs;
    private final String schemaHash;
    private final Map<String, FieldInfo> fields;
    
    /**
     * Creates new hotcache information.
     * 
     * @param splitId Unique identifier for the split
     * @param numDocs Number of documents in the split
     * @param schemaHash Hash of the schema for compatibility checking
     * @param fields Map of field names to field metadata
     * @throws NullPointerException if any parameter is null
     * @throws IllegalArgumentException if numDocs is negative
     */
    public HotcacheInfo(String splitId, int numDocs, String schemaHash, Map<String, FieldInfo> fields) {
        this.splitId = Objects.requireNonNull(splitId, "Split ID cannot be null");
        this.schemaHash = Objects.requireNonNull(schemaHash, "Schema hash cannot be null");
        this.fields = Objects.requireNonNull(fields, "Fields cannot be null");
        
        if (numDocs < 0) {
            throw new IllegalArgumentException("Number of docs cannot be negative: " + numDocs);
        }
        
        this.numDocs = numDocs;
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
     * Gets the schema hash for compatibility validation.
     * 
     * @return Schema hash string
     */
    public String getSchemaHash() {
        return schemaHash;
    }
    
    /**
     * Gets field metadata for all fields in the split.
     * 
     * @return Map from field names to field information
     */
    public Map<String, FieldInfo> getFields() {
        return fields;
    }
    
    /**
     * Gets metadata for a specific field.
     * 
     * @param fieldName Name of the field
     * @return Field information, or null if field doesn't exist
     */
    public FieldInfo getFieldInfo(String fieldName) {
        return fields.get(fieldName);
    }
    
    /**
     * Checks if a field exists in this split.
     * 
     * @param fieldName Name of the field to check
     * @return true if field exists, false otherwise
     */
    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        HotcacheInfo that = (HotcacheInfo) obj;
        return numDocs == that.numDocs &&
               Objects.equals(splitId, that.splitId) &&
               Objects.equals(schemaHash, that.schemaHash) &&
               Objects.equals(fields, that.fields);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(splitId, numDocs, schemaHash, fields);
    }
    
    @Override
    public String toString() {
        return String.format("HotcacheInfo{splitId='%s', numDocs=%d, fields=%d, schemaHash='%s'}",
                           splitId, numDocs, fields.size(), schemaHash);
    }
    
    /**
     * Information about a field in the split.
     */
    public static class FieldInfo {
        
        private final FieldType fieldType;
        private final ByteRange postingRange;
        private final ByteRange fastFieldRange;
        private final ByteRange positionsRange;
        private final ByteRange termDictRange;
        
        /**
         * Creates new field information.
         * 
         * @param fieldType Type of the field
         * @param postingRange Byte range for posting lists (nullable)
         * @param fastFieldRange Byte range for fast field data (nullable)
         * @param positionsRange Byte range for position data (nullable)
         * @param termDictRange Byte range for term dictionary (nullable)
         * @throws NullPointerException if fieldType is null
         */
        public FieldInfo(FieldType fieldType, ByteRange postingRange, ByteRange fastFieldRange,
                        ByteRange positionsRange, ByteRange termDictRange) {
            this.fieldType = Objects.requireNonNull(fieldType, "Field type cannot be null");
            this.postingRange = postingRange;
            this.fastFieldRange = fastFieldRange;
            this.positionsRange = positionsRange;
            this.termDictRange = termDictRange;
        }
        
        /**
         * Gets the field type.
         * 
         * @return Field type
         */
        public FieldType getFieldType() {
            return fieldType;
        }
        
        /**
         * Gets the byte range for posting list data.
         * 
         * @return Posting range, or null if not available
         */
        public ByteRange getPostingRange() {
            return postingRange;
        }
        
        /**
         * Gets the byte range for fast field data.
         * 
         * @return Fast field range, or null if not available
         */
        public ByteRange getFastFieldRange() {
            return fastFieldRange;
        }
        
        /**
         * Gets the byte range for position data.
         * 
         * @return Position range, or null if not available
         */
        public ByteRange getPositionsRange() {
            return positionsRange;
        }
        
        /**
         * Gets the byte range for term dictionary data.
         * 
         * @return Term dictionary range, or null if not available
         */
        public ByteRange getTermDictRange() {
            return termDictRange;
        }
        
        /**
         * Checks if this field has posting list data.
         * 
         * @return true if posting data is available
         */
        public boolean hasPostingData() {
            return postingRange != null;
        }
        
        /**
         * Checks if this field has fast field data.
         * 
         * @return true if fast field data is available
         */
        public boolean hasFastFieldData() {
            return fastFieldRange != null;
        }
        
        /**
         * Checks if this field has position data.
         * 
         * @return true if position data is available
         */
        public boolean hasPositionData() {
            return positionsRange != null;
        }
        
        /**
         * Checks if this field has term dictionary data.
         * 
         * @return true if term dictionary data is available
         */
        public boolean hasTermDictData() {
            return termDictRange != null;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            
            FieldInfo fieldInfo = (FieldInfo) obj;
            return fieldType == fieldInfo.fieldType &&
                   Objects.equals(postingRange, fieldInfo.postingRange) &&
                   Objects.equals(fastFieldRange, fieldInfo.fastFieldRange) &&
                   Objects.equals(positionsRange, fieldInfo.positionsRange) &&
                   Objects.equals(termDictRange, fieldInfo.termDictRange);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(fieldType, postingRange, fastFieldRange, positionsRange, termDictRange);
        }
        
        @Override
        public String toString() {
            return String.format("FieldInfo{type=%s, posting=%s, fastField=%s, positions=%s, termDict=%s}",
                               fieldType, postingRange, fastFieldRange, positionsRange, termDictRange);
        }
    }
}