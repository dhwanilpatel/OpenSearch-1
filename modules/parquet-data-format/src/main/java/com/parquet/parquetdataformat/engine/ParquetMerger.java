/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.util.*;

public class ParquetMerger implements Merger {
    @Override
    public MergeResult merge(Collection<FileMetadata> fileMetadataList) {
        System.out.println("In ParquetMerger primary merge ================ ");
        RowIdMapping rowIdMapping = new RowIdMapping(Map.of());
        Map<DataFormat, Collection<FileMetadata>> mergedFileMapping = new HashMap<>();

        List<FileMetadata> mergedFiles = new ArrayList<>();
        mergedFiles.add(new FileMetadata("tmp", "merged-file-name"));

        mergedFileMapping.put(ParquetDataFormat.PARQUET_DATA_FORMAT, mergedFiles);

        MergeResult mergeResult = new MergeResult(rowIdMapping, mergedFileMapping);
        return mergeResult;
    }

    @Override
    public MergeResult merge(Collection<FileMetadata> fileMetadataList, RowIdMapping rowIdMapping) {
        throw new UnsupportedOperationException("Not supported parquet as secondary data format yet.");
    }
}
