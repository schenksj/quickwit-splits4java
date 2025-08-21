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

import com.tantivy4java.splits.*;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating how to read a Quickwit split and access its data.
 * 
 * This example shows:
 * 1. Opening a split for reading
 * 2. Accessing hotcache metadata
 * 3. Listing segment files
 * 4. Reading posting lists for terms
 * 5. Accessing fast field data
 */
public class ReadSplit {
    
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ReadSplit <split-path>");
            System.err.println("  split-path: Path to Quickwit split directory");
            System.exit(1);
        }
        
        Path splitPath = Paths.get(args[0]);
        
        System.out.println("Quickwit Split Reading Example");
        System.out.println("==============================");
        System.out.println("Split path: " + splitPath);
        System.out.println();
        
        try {
            // Initialize the native library
            QuickwitSplits.initialize();
            
            // Open the split for reading
            System.out.println("Opening split for reading...");
            try (QuickwitSplitReader reader = new QuickwitSplitReader(splitPath)) {
                
                System.out.println("Split opened successfully");
                System.out.println("Split path: " + reader.getSplitPath());
                System.out.println();
                
                // Get and display hotcache information
                displayHotcacheInfo(reader);
                
                // List and display segment files
                displaySegmentFiles(reader);
                
                // Demonstrate posting list access
                demonstratePostingLists(reader);
                
                // Demonstrate fast field access
                demonstrateFastFields(reader);
                
            } catch (Exception e) {
                System.err.println("Failed to read split: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("Split reading completed successfully!");
    }
    
    /**
     * Displays hotcache metadata information
     */
    private static void displayHotcacheInfo(QuickwitSplitReader reader) {
        System.out.println("Hotcache Information:");
        System.out.println("--------------------");
        
        HotcacheInfo hotcache = reader.getHotcacheInfo();
        if (hotcache == null) {
            System.out.println("  No hotcache information available");
            System.out.println();
            return;
        }
        
        System.out.println("  Split ID: " + hotcache.getSplitId());
        System.out.println("  Documents: " + hotcache.getNumDocs());
        System.out.println("  Schema hash: " + hotcache.getSchemaHash());
        System.out.println("  Fields: " + hotcache.getFields().size());
        
        // Display field information
        Map<String, HotcacheInfo.FieldInfo> fields = hotcache.getFields();
        if (!fields.isEmpty()) {
            System.out.println();
            System.out.println("  Field Details:");
            for (Map.Entry<String, HotcacheInfo.FieldInfo> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                HotcacheInfo.FieldInfo fieldInfo = entry.getValue();
                
                System.out.println("    " + fieldName + ":");
                System.out.println("      Type: " + fieldInfo.getFieldType());
                System.out.println("      Has posting data: " + fieldInfo.hasPostingData());
                System.out.println("      Has fast field data: " + fieldInfo.hasFastFieldData());
                System.out.println("      Has position data: " + fieldInfo.hasPositionData());
                System.out.println("      Has term dict data: " + fieldInfo.hasTermDictData());
            }
        }
        
        System.out.println();
    }
    
    /**
     * Displays segment files in the split
     */
    private static void displaySegmentFiles(QuickwitSplitReader reader) {
        System.out.println("Segment Files:");
        System.out.println("--------------");
        
        try {
            List<String> files = reader.listSegmentFiles();
            
            if (files.isEmpty()) {
                System.out.println("  No segment files found");
            } else {
                for (String file : files) {
                    System.out.println("  " + file);
                }
            }
            
            System.out.println("  Total files: " + files.size());
            
        } catch (Exception e) {
            System.err.println("  Failed to list segment files: " + e.getMessage());
        }
        
        System.out.println();
    }
    
    /**
     * Demonstrates posting list access
     */
    private static void demonstratePostingLists(QuickwitSplitReader reader) {
        System.out.println("Posting List Access:");
        System.out.println("-------------------");
        
        // Try some common field/term combinations
        String[][] searchTerms = {
            {"title", "test"},
            {"title", "quickwit"},
            {"title", "document"},
            {"body", "content"},
            {"body", "search"},
            {"title", "nonexistent"}
        };
        
        for (String[] term : searchTerms) {
            String field = term[0];
            String termValue = term[1];
            
            try {
                long startTime = System.nanoTime();
                int[] docs = reader.readPostingList(field, termValue);
                long endTime = System.nanoTime();
                double duration = (endTime - startTime) / 1_000_000.0; // Convert to milliseconds
                
                System.out.printf("  %s:'%s' -> %d documents (%.2f ms)%n", 
                                field, termValue, docs.length, duration);
                
                if (docs.length > 0 && docs.length <= 10) {
                    System.out.print("    Document IDs: ");
                    for (int i = 0; i < docs.length; i++) {
                        if (i > 0) System.out.print(", ");
                        System.out.print(docs[i]);
                    }
                    System.out.println();
                } else if (docs.length > 10) {
                    System.out.printf("    Document IDs: %d, %d, ... %d, %d (showing first/last 2)%n",
                                    docs[0], docs[1], docs[docs.length - 2], docs[docs.length - 1]);
                }
                
            } catch (Exception e) {
                System.out.printf("  %s:'%s' -> Error: %s%n", field, termValue, e.getMessage());
            }
        }
        
        System.out.println();
    }
    
    /**
     * Demonstrates fast field data access
     */
    private static void demonstrateFastFields(QuickwitSplitReader reader) {
        System.out.println("Fast Field Access:");
        System.out.println("------------------");
        
        // Try accessing different fields with document ranges
        String[] fields = {"timestamp", "score", "id", "count"};
        int[][] docRanges = {{0, 10}, {0, 5}, {10, 20}, {0, 100}};
        
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            int startDoc = docRanges[i][0];
            int endDoc = docRanges[i][1];
            
            try {
                long startTime = System.nanoTime();
                byte[] data = reader.getFastFieldData(field, startDoc, endDoc);
                long endTime = System.nanoTime();
                double duration = (endTime - startTime) / 1_000_000.0; // Convert to milliseconds
                
                int docCount = endDoc - startDoc;
                System.out.printf("  %s[%d:%d] -> %d bytes for %d docs (%.2f ms)%n",
                                field, startDoc, endDoc, data.length, docCount, duration);
                
                if (data.length > 0) {
                    System.out.printf("    Avg bytes per doc: %.1f%n", (double) data.length / docCount);
                }
                
            } catch (Exception e) {
                System.out.printf("  %s[%d:%d] -> Error: %s%n", field, startDoc, endDoc, e.getMessage());
            }
        }
        
        System.out.println();
    }
}