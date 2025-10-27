/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.*;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ParquetTieredMergePolicy implements MergePolicy.MergeContext {
    private final TieredMergePolicy luceneMergePolicy;
    private final InfoStream infoStream;

    private static final Set<SegmentCommitInfo> mergingSegments = new HashSet<>();
    private static final Set<String> mergingFileNames = new HashSet<>();

    public ParquetTieredMergePolicy() {
        this.luceneMergePolicy = new TieredMergePolicy();
        this.infoStream = new InfoStream() {
            @Override
            public void message(String s, String s1) {
                System.out.println("parquet merge: s =" + s + " s1 = " + s1);
            }

            @Override
            public boolean isEnabled(String s) {
                return true;
            }

            @Override
            public void close() throws IOException {
            }
        };;
    }

    public List<List<ParquetFileInfo>> findMergeCandidates(
        List<ParquetFileInfo> segments) throws IOException {

        System.out.println("Segments ============== " + segments);
        // Convert Parquet segments to Lucene-style segments
        List<SegmentCommitInfo> luceneSegments = new ArrayList<>();
        Map<SegmentCommitInfo, ParquetFileInfo> segmentMap = new HashMap<>();

        for (ParquetFileInfo parquetSegment : segments) {
            ParquetSegmentWrapper wrapper = new ParquetSegmentWrapper(parquetSegment);
            luceneSegments.add(wrapper);
            segmentMap.put(wrapper, parquetSegment);
        }

        // Create SegmentInfos (required by Lucene 10)
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        luceneSegments.forEach(segmentInfos::add);

        // Find merge candidates using Lucene's policy
        List<List<ParquetFileInfo>> merges = new ArrayList<>();
        System.out.println("LuceneSegments ======== " + luceneSegments + " " + luceneSegments.size());
        System.out.println("segment Infos ======== " + segmentInfos + " " + segmentInfos.asList());
        try {
            // Get merge candidates from Lucene's policy
            MergePolicy.MergeSpecification mergeSpecification = luceneMergePolicy.findMerges(MergeTrigger.COMMIT, segmentInfos
                , this);
            System.out.println("Merge spec ============ " + mergeSpecification);

            if(mergeSpecification != null) {
                List<MergePolicy.OneMerge> luceneMerges = mergeSpecification.merges;

                // Convert back to Parquet segments
                for (MergePolicy.OneMerge merge : luceneMerges) {
                    boolean validMerge = true;
                    List<ParquetFileInfo> parquetMerge = new ArrayList<>();
                    for (SegmentCommitInfo segment : merge.segments) {
                        System.out.println("MergingFIleNames ==== " + mergingFileNames + " segment name == " + segment.info.name + " contains check === " + mergingFileNames.contains(segment.info.name));
                        if(mergingFileNames.contains(segment.info.name)) {
                            validMerge = false;
                        }
                        parquetMerge.add(segmentMap.get(segment));
                    }
                    if(validMerge) {
                        for(SegmentCommitInfo segment : merge.segments) {
                            mergingSegments.add(segment);
                            mergingFileNames.add(segment.info.name);
                        }
                        merges.add(parquetMerge);
                    } else {
                        System.out.println("not valid merge already in file!! Rejecting !!!");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error finding merge candidates", e);
        }

        return merges;
    }

    @Override
    public int numDeletesToMerge(SegmentCommitInfo segmentCommitInfo) throws IOException {
        return 0;
    }

    @Override
    public int numDeletedDocs(SegmentCommitInfo segmentCommitInfo) {
        return 0;
    }

    @Override
    public InfoStream getInfoStream() {
        return this.infoStream;
    }

    @Override
    public Set<SegmentCommitInfo> getMergingSegments() {
        System.out.println("In get merging segments =====  " + mergingSegments);
        return Collections.unmodifiableSet(mergingSegments);
    }


    // Configuration methods
    public void setMaxMergedSegmentMB(double mb) {
        luceneMergePolicy.setMaxMergedSegmentMB(mb);
    }

    public void setSegmentsPerTier(double segments) {
        luceneMergePolicy.setSegmentsPerTier(segments);
    }

    public void setMaxMergeAtOnce(int count) {
        luceneMergePolicy.setMaxMergeAtOnce(count);
    }

    public void setFloorSegmentMB(double mb) {
        luceneMergePolicy.setFloorSegmentMB(mb);
    }

    public static class ParquetFileInfo {
        private final Path path;
        private final long sizeBytes;
        private final long docCount;
        private final String segmentName;  // Added for Lucene compatibility

        public ParquetFileInfo(String path) throws IOException {
            this.path = Paths.get(path);
            this.sizeBytes = Files.size(this.path);
            this.docCount = Files.size(this.path)/1000;
            this.segmentName = path.split("/")[path.split("/").length - 1];
        }

        public Path getPath() { return path; }
        public long getSizeBytes() { return sizeBytes; }
        public int getDocCount() { return (int)docCount; }
        public String getSegmentName() { return segmentName; }
    }

    private static class ParquetSegmentWrapper extends SegmentCommitInfo {
        private final ParquetFileInfo parquetInfo;

        public ParquetSegmentWrapper(ParquetFileInfo parquetInfo) throws IOException {
            super(
                new SegmentInfo(
                    // directory - not used for our purpose
                    new NIOFSDirectory(Paths.get("/tmp")),
                    //version
                    Version.LATEST,
                    Version.LATEST,
                    // segment name - using file name without extension
                    parquetInfo.getSegmentName(),
//                    parquetInfo.getPath().getName().replaceFirst("\\.parquet$", ""),
                    // maxDoc - number of rows in parquet file
                    parquetInfo.getDocCount(),
                    // isCompound - false as we don't need compound file format
                    false,
                    // has block
                    false,
                    // codec - using default
                    Codec.getDefault(),
                    // diagnostics - empty map
                    new HashMap<String, String>(Map.of("test", "test")),
                    // segmentID - generate unique ID
                    UUID.randomUUID().toString().substring(0,16).getBytes(),
                    // map of field numbers - empty as not needed
                    new HashMap<String, String>(Map.of("test", "test")),
                    // sort - no specific sort
                    null
                ),
                // Del Count
                0,
                // softDelCount
                0,
                // delGen - no deletions in parquet
                0,
                // fieldInfosGen - no separate field infos
                -1,
                // docValuesGen - no doc values updates
                -1,
                // id - no soft deletes
                UUID.randomUUID().toString().substring(0,16).getBytes());
            this.parquetInfo = parquetInfo;
        }

        @Override
        public long sizeInBytes() {
            System.out.println("in Parquet size in bytes ======== " + parquetInfo.getSizeBytes());
            return parquetInfo.getSizeBytes();
        }

        @Override
        public int getDelCount() {
            return 0;
        }
    }

//    public static class ParquetMergeContext implements MergePolicy.MergeContext {
////        private final List<ParquetSegmentInfo> parquetFiles;
////        private final long totalMaxDoc;
////        private final Set<SegmentCommitInfo> mergingSegments;
//        private final InfoStream infoStream;
//
//        public ParquetMergeContext() {
////            this.parquetFiles = parquetFiles;
////            this.totalMaxDoc = parquetFiles.stream()
////                .mapToLong(ParquetSegmentInfo::getDocCount)
////                .sum();
////            this.mergingSegments = ConcurrentHashMap.newKeySet();
//            this.infoStream = new InfoStream() {
//                @Override
//                public void message(String s, String s1) {
//                    System.out.println("parquet merge: s =" + s + " s1 = " + s1);
//                }
//
//                @Override
//                public boolean isEnabled(String s) {
//                    return true;
//                }
//
//                @Override
//                public void close() throws IOException {
//
//                }
//            };
//        }
//
//        @Override
//        public int numDeletesToMerge(SegmentCommitInfo info) {
//            return 0;
//        }
//
//        @Override
//        public int numDeletedDocs(SegmentCommitInfo info) {
//            return 0;
//        }
//
//        @Override
//        public Set<SegmentCommitInfo> getMergingSegments() {
//            System.out.println("In get merging segments =====  " + mergingSegments);
//            return Collections.unmodifiableSet(mergingSegments);
//        }
//
//        @Override
//        public InfoStream getInfoStream() {
//            return infoStream;
//        }
////
////        // Helper method to register segments that are being merged
//        public void registerMergingSegments(Collection<SegmentCommitInfo> segments) {
//            mergingSegments.addAll(segments);
//        }
////
////        // Helper method to unregister segments after merge is complete
////        public void unregisterMergingSegments(Collection<SegmentCommitInfo> segments) {
////            mergingSegments.removeAll(segments);
////        }
//    }

//    public static void main() {
//        System.out.println("In ParquetTieredMergePolicy test ==== ");
//        ParquetTieredMergePolicy mergePolicy = new ParquetTieredMergePolicy();
//
//        // Configure the merge policy
////        mergePolicy.setMaxMergedSegmentMB(5 * 1024);  // 5GB
////        mergePolicy.setSegmentsPerTier(2.0);
////        mergePolicy.setMaxMergeAtOnce(3);
////        mergePolicy.setFloorSegmentMB(1.0);
//
//        // List of Parquet files to consider for merging
//        List<ParquetSegmentInfo> parquetFiles = new ArrayList<>();
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file1.parquet"), 5*1024*1024, 10000, "seg_1"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file2.parquet"), 7*1024*1024, 20000, "seg_2"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file1.parquet"), 5*1024*1024, 10000, "seg_4"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file2.parquet"), 7*1024*1024, 20000, "seg_5"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file3.parquet"), 4*1024*1024, 30000, "seg_6"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file2.parquet"), 7*1024*1024, 20000, "seg_7"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file3.parquet"), 4*1024*1024, 30000, "seg_8"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file2.parquet"), 7*1024*1024, 20000, "seg_9"));
//        parquetFiles.add(new ParquetSegmentInfo(Paths.get("/path/to/file3.parquet"), 4*1024*1024, 30000, "seg_10"));
//
//        try {
////            MergePolicy.MergeContext mergeContext = new ParquetMergeContext();
//            // Find merge candidates
//            for(int k = 0 ; k < 3 ; k++ ) {
//
//                System.out.println("TRIGGERING ======================= " + k);
//                List<List<ParquetSegmentInfo>> mergeCandidates =
//                    mergePolicy.findMergeCandidates(parquetFiles);
//
//
//                // Process merge candidates
//                for (int i = 0; i < mergeCandidates.size(); i++) {
//                    List<ParquetSegmentInfo> mergeGroup = mergeCandidates.get(i);
//                    System.out.printf("Merge group %d:%n", i + 1);
//
//                    for (ParquetSegmentInfo file : mergeGroup) {
//                        System.out.printf("  - %s (%.2f MB, %d rows)%n",
//                            file.getPath(),
//                            file.getSizeBytes() / (1024.0 * 1024.0),
//                            file.getDocCount());
//                    }
//                }
//
////                mergeContext.registerMergingSegments()
//            }
//
//
//        } catch (IOException e) {
//            System.err.println("Error during merge candidate selection: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
}

