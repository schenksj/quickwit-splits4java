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

/**
 * Example demonstrating how to generate a Quickwit split from a Tantivy index.
 * 
 * This example shows:
 * 1. Opening an existing Tantivy index
 * 2. Creating a split generator with target document count
 * 3. Generating the split with embedded hotcache metadata
 * 4. Displaying split information
 */
public class GenerateSplit {
    
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: GenerateSplit <index-path> <output-path>");
            System.err.println("  index-path: Path to existing Tantivy index directory");
            System.err.println("  output-path: Path where split will be generated");
            System.exit(1);
        }
        
        Path indexPath = Paths.get(args[0]);
        Path outputPath = Paths.get(args[1]);
        
        System.out.println("Quickwit Split Generation Example");
        System.out.println("==================================");
        System.out.println("Index path: " + indexPath);
        System.out.println("Output path: " + outputPath);
        System.out.println();
        
        try {
            // Initialize the native library
            QuickwitSplits.initialize();
            
            // Open the Tantivy index
            System.out.println("Opening Tantivy index...");
            try (Index index = Index.open(indexPath)) {
                
                System.out.println("Index opened successfully");
                System.out.println("Index path: " + index.getIndexPath());
                
                // Create split generator
                // Target 1 million documents per split (adjust based on your needs)
                int targetDocsPerSplit = 1_000_000;
                System.out.println("Creating split generator (target: " + targetDocsPerSplit + " docs per split)...");
                
                try (QuickwitSplitGenerator generator = new QuickwitSplitGenerator(index, targetDocsPerSplit)) {
                    
                    System.out.println("Split generator created successfully");
                    System.out.println("Target docs per split: " + generator.getTargetDocsPerSplit());
                    System.out.println();
                    
                    // Generate the split
                    System.out.println("Generating split...");
                    long startTime = System.currentTimeMillis();
                    
                    SplitMetadata metadata = generator.generateSplit(outputPath);
                    
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    
                    // Display results
                    System.out.println("Split generated successfully!");
                    System.out.println();
                    System.out.println("Split Information:");
                    System.out.println("  Split ID: " + metadata.getSplitId());
                    System.out.println("  Documents: " + metadata.getNumDocs());
                    System.out.println("  Size: " + formatBytes(metadata.getSizeBytes()));
                    System.out.println("  Hotcache range: " + metadata.getHotcacheRange().getStart() + 
                                     "-" + metadata.getHotcacheRange().getEnd() + 
                                     " (" + metadata.getHotcacheRange().getSize() + " bytes)");
                    System.out.println("  Generation time: " + duration + " ms");
                    System.out.println();
                    
                    // Calculate throughput
                    if (duration > 0) {
                        double docsPerSecond = (metadata.getNumDocs() * 1000.0) / duration;
                        double mbPerSecond = (metadata.getSizeBytes() * 1000.0) / (duration * 1024 * 1024);
                        
                        System.out.println("Performance:");
                        System.out.printf("  Throughput: %.1f docs/sec%n", docsPerSecond);
                        System.out.printf("  I/O rate: %.2f MB/sec%n", mbPerSecond);
                    }
                    
                    System.out.println();
                    System.out.println("Split files written to: " + outputPath.toAbsolutePath());
                    
                } catch (Exception e) {
                    System.err.println("Failed to generate split: " + e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                
            } catch (Exception e) {
                System.err.println("Failed to open index: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("Split generation completed successfully!");
    }
    
    /**
     * Formats byte count in human-readable format
     */
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "";
        return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
    }
}