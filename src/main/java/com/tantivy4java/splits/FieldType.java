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

/**
 * Enumeration of field types supported in Quickwit splits.
 * 
 * <p>These field types correspond to the Tantivy field types and
 * determine how data is indexed, stored, and accessed within splits.
 */
public enum FieldType {
    
    /** Text field with tokenization and full-text search capabilities */
    TEXT("text"),
    
    /** Unsigned integer field for numeric values */
    UNSIGNED("unsigned"),
    
    /** Signed integer field for numeric values */
    SIGNED("signed"),
    
    /** Floating-point field for decimal numeric values */
    FLOAT("float"),
    
    /** Boolean field for true/false values */
    BOOLEAN("boolean"),
    
    /** Date/timestamp field for temporal data */
    DATE("date"),
    
    /** IP address field for network addresses */
    IP_ADDR("ip_addr"),
    
    /** Raw bytes field for binary data */
    BYTES("bytes"),
    
    /** JSON object field for structured data */
    JSON("json"),
    
    /** Facet field for hierarchical categorization */
    FACET("facet");
    
    private final String typeName;
    
    FieldType(String typeName) {
        this.typeName = typeName;
    }
    
    /**
     * Gets the string representation of this field type.
     * 
     * @return Type name string
     */
    public String getTypeName() {
        return typeName;
    }
    
    /**
     * Creates a FieldType from its string representation.
     * 
     * @param typeName String representation of the field type
     * @return Corresponding FieldType enum value
     * @throws IllegalArgumentException if typeName is not recognized
     */
    public static FieldType fromString(String typeName) {
        if (typeName == null) {
            throw new IllegalArgumentException("Type name cannot be null");
        }
        
        for (FieldType type : values()) {
            if (type.typeName.equals(typeName)) {
                return type;
            }
        }
        
        throw new IllegalArgumentException("Unknown field type: " + typeName);
    }
    
    /**
     * Checks if this field type supports full-text search.
     * 
     * @return true if field supports text search operations
     */
    public boolean isTextSearchable() {
        return this == TEXT || this == JSON;
    }
    
    /**
     * Checks if this field type supports range queries.
     * 
     * @return true if field supports range operations
     */
    public boolean isRangeQueryable() {
        return this == UNSIGNED || this == SIGNED || this == FLOAT || this == DATE;
    }
    
    /**
     * Checks if this field type supports exact matching.
     * 
     * @return true if field supports exact match queries
     */
    public boolean isExactMatchable() {
        return this != TEXT; // TEXT fields are tokenized, others support exact matching
    }
    
    /**
     * Checks if this field type is numeric.
     * 
     * @return true if field represents numeric data
     */
    public boolean isNumeric() {
        return this == UNSIGNED || this == SIGNED || this == FLOAT;
    }
    
    @Override
    public String toString() {
        return typeName;
    }
}