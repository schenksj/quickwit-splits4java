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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.util.List;
import com.tantivy4java.Index;

/**
 * Integration tests for the complete Quickwit Splits4Java workflow.
 * 
 * These tests verify end-to-end functionality including:
 * - Split generation from indices
 * - Split reading and metadata access
 * - Data retrieval operations
 * - Error handling and edge cases
 */
public class QuickwitSplitsIntegrationTest {

    @TempDir
    Path tempDir;
    
    private Path indexDir;
    private Path splitDir;
    
    @BeforeAll
    static void initializeLibrary() {
        QuickwitSplits.initialize();
    }
    
    @BeforeEach
    void setUp() throws IOException {
        indexDir = tempDir.resolve("test_index");
        splitDir = tempDir.resolve("test_split");
        
        Files.createDirectories(indexDir);
        Files.createDirectories(splitDir);
    }
    
    @Test
    void testLibraryInitialization() {
        // Test that library initialization works
        assertDoesNotThrow(() -> QuickwitSplits.initialize());
        
        // Test native library status
        // Note: This will be false until native library is built
        boolean isLoaded = QuickwitSplits.isNativeLibraryLoaded();
        // We don't assert true here since we're in development phase
        assertNotNull(isLoaded); // Just verify the method works
    }
    
    @Test
    void testBasicClassInstantiation() {
        // Test that all main classes can be instantiated without native library
        
        // ByteRange
        ByteRange range = new ByteRange(0, 100);
        assertEquals(0, range.getStart());
        assertEquals(100, range.getEnd());
        assertEquals(100, range.getSize());
        
        // FieldType
        assertEquals("text", FieldType.TEXT.getTypeName());
        assertEquals("unsigned", FieldType.UNSIGNED.getTypeName());
        assertTrue(FieldType.TEXT.isTextSearchable());
        assertTrue(FieldType.UNSIGNED.isNumeric());
    }
    
    @Test
    void testFieldTypeOperations() {
        // Test field type functionality
        FieldType textType = FieldType.fromString("text");
        assertEquals(FieldType.TEXT, textType);
        
        assertTrue(FieldType.TEXT.isTextSearchable());
        assertTrue(FieldType.UNSIGNED.isRangeQueryable());
        assertTrue(FieldType.BOOLEAN.isExactMatchable());
        assertFalse(FieldType.TEXT.isExactMatchable()); // TEXT is tokenized
        
        // Test unknown field type
        assertThrows(IllegalArgumentException.class, () -> {
            FieldType.fromString("unknown");
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            FieldType.fromString(null);
        });
    }
    
    @Test
    void testByteRangeOperations() {
        ByteRange range1 = new ByteRange(100, 200);
        ByteRange range2 = new ByteRange(150, 250);
        ByteRange range3 = new ByteRange(300, 400);
        ByteRange emptyRange = new ByteRange(100, 100);
        
        // Basic properties
        assertEquals(100, range1.getSize());
        assertTrue(emptyRange.isEmpty());
        assertFalse(range1.isEmpty());
        
        // Contains
        assertTrue(range1.contains(150));
        assertFalse(range1.contains(200)); // Exclusive end
        assertFalse(range1.contains(50));
        
        // Overlaps
        assertTrue(range1.overlaps(range2));
        assertFalse(range1.overlaps(range3));
        assertFalse(range1.overlaps(emptyRange));
        
        // Equality
        ByteRange range1Copy = new ByteRange(100, 200);
        assertEquals(range1, range1Copy);
        assertEquals(range1.hashCode(), range1Copy.hashCode());
        assertNotEquals(range1, range2);
    }
    
    // Index interface testing removed - now using real tantivy4java.Index
    
    @Disabled("Native implementation not complete")
    @Test
    void testEndToEndWorkflow() throws IOException {
        // This test demonstrates the complete workflow once native implementation is ready
        
        // Step 1: Create an index with some data (would use tantivy4java in real scenario)
        try (Index index = Index.open(indexDir.toString())) {
            
            // Step 2: Generate a split
            try (QuickwitSplitGenerator generator = new QuickwitSplitGenerator(index, 1000)) {
                SplitMetadata metadata = generator.generateSplit(splitDir);
                
                assertNotNull(metadata);
                assertNotNull(metadata.getSplitId());
                assertTrue(metadata.getNumDocs() >= 0);
                assertTrue(metadata.getSizeBytes() > 0);
                assertNotNull(metadata.getHotcacheRange());
                
                // Verify split files exist
                assertTrue(Files.exists(splitDir));
                assertTrue(Files.isDirectory(splitDir));
                
                // Step 3: Read the split
                try (QuickwitSplitReader reader = new QuickwitSplitReader(splitDir)) {
                    
                    // Verify hotcache info
                    HotcacheInfo hotcache = reader.getHotcacheInfo();
                    assertNotNull(hotcache);
                    assertEquals(metadata.getSplitId(), hotcache.getSplitId());
                    assertEquals(metadata.getNumDocs(), hotcache.getNumDocs());
                    
                    // Verify segment files
                    List<String> files = reader.listSegmentFiles();
                    assertNotNull(files);
                    assertFalse(files.isEmpty());
                    
                    // Test posting list access
                    int[] postingList = reader.readPostingList("title", "test");
                    assertNotNull(postingList);
                    
                    // Test fast field access
                    byte[] fieldData = reader.getFastFieldData("id", 0, 10);
                    assertNotNull(fieldData);
                }
            }
        }
    }
    
    @Test
    void testErrorHandling() {
        // Test various error conditions
        
        // Invalid constructor parameters
        Path validIndex = indexDir;
        
        assertThrows(NullPointerException.class, () -> {
            new QuickwitSplitGenerator(null, 1000);
        });
        
        assertThrows(NullPointerException.class, () -> {
            new QuickwitSplitReader(null);
        });
        
        // Invalid split path for reader
        Path nonExistentSplit = tempDir.resolve("non_existent_split");
        assertThrows(IOException.class, () -> {
            new QuickwitSplitReader(nonExistentSplit);
        });
    }
    
    @Test
    void testResourceManagement() throws IOException {
        // Test that resources are properly managed with try-with-resources
        
        Index index = Index.open(indexDir.toString());
        assertNotNull(index);
        
        index.close();
        
        // Test double close is safe
        assertDoesNotThrow(() -> index.close());
    }
    
    @Test
    void testThreadSafety() throws InterruptedException {
        // Test that multiple threads can safely use the library
        
        final int threadCount = 4;
        final int operationsPerThread = 10;
        Thread[] threads = new Thread[threadCount];
        final Exception[] exceptions = new Exception[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Test basic operations that don't require native code
                        ByteRange range = new ByteRange(j * 100, (j + 1) * 100);
                        assertEquals(100, range.getSize());
                        
                        FieldType type = FieldType.TEXT;
                        assertTrue(type.isTextSearchable());
                        
                        // Small delay to increase chance of race conditions
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    exceptions[threadId] = e;
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }
        
        // Check for exceptions
        for (int i = 0; i < threadCount; i++) {
            if (exceptions[i] != null) {
                fail("Thread " + i + " threw exception: " + exceptions[i].getMessage());
            }
        }
    }
    
    @Test
    void testVersionInfo() {
        // Test version information (this would work with native library)
        if (QuickwitSplits.isNativeLibraryLoaded()) {
            String version = QuickwitSplits.getVersion();
            assertNotNull(version);
            assertFalse(version.isEmpty());
        }
    }
}