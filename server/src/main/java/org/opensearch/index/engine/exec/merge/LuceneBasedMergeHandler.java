/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.*;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.*;

public class LuceneBasedMergeHandler extends MergeHandler implements Merger {

    CustomIndexWriter writer;
    MergePolicy mergePolicy;
    Map<List<FileMetadata>, MergePolicy.OneMerge> listFileMetadataToOneMerge = new HashMap<>();

    public LuceneBasedMergeHandler(PluginsService pluginsService, Any dataFormats, CustomIndexWriter writer) {
        super(pluginsService, dataFormats);
        this.writer = writer;
        mergePolicy = new TieredMergePolicy();
    }

    public List<Merge> findMerges() throws IOException {

        MergePolicy.MergeSpecification spec = mergePolicy.findMerges(org.apache.lucene.index.MergeTrigger.COMMIT, SegmentInfos.readLatestCommit(writer.getDirectory()), writer);
        List<Merge> merges = new ArrayList<>();
        for(MergePolicy.OneMerge merge : spec.merges) {
            List<FileMetadata> fileMetadataList = convertOneMergeToFileMetadataList(merge);
            merges.add(new Merge(DataFormat.LUCENE, fileMetadataList));
            listFileMetadataToOneMerge.put(fileMetadataList, merge);
        }
        return merges;
    }

    public List<FileMetadata> convertOneMergeToFileMetadataList(MergePolicy.OneMerge merge) {
        List<FileMetadata> fileMetadataList = new ArrayList<>();
        // get file information and convert in file metadata and update in list

        listFileMetadataToOneMerge.put(fileMetadataList, merge);
        return fileMetadataList;
    }

    public MergePolicy.OneMerge convertFileMetadataListToOneMerge(List<FileMetadata> fileMetadataList) {
        return listFileMetadataToOneMerge.get(fileMetadataList);
    }

    @Override
    public MergeResult merge(List<FileMetadata> fileMetadataList) {
        try {
            writer.merge(new MergePolicy.OneMerge(getSegmentInfos(fileMetadataList)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public FileMetadata merge(List<FileMetadata> fileMetadataList, RowIdMapping rowIdMapping) {
        try {
            writer.merge(new MergePolicy.OneMerge(getSegmentInfos(fileMetadataList)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // TODO Need to provide conversion from data format specific file to global file format
    // For e.g. for lucene it needs to provide the conversion from list of files to list of segmentInfos
    public List<SegmentCommitInfo> getSegmentInfos(List<FileMetadata> fileMetadataList) throws IOException {
        // for completeness reading latest commit and returning

        List<String> segmentNameList = new ArrayList<>();
        for(FileMetadata fileMetadata : fileMetadataList) {
            segmentNameList.add(fileMetadata.fileName());
        }

        return SegmentInfos.readLatestCommit(writer.getDirectory()).asList().stream().filter(sci -> segmentNameList.contains(sci.info.name)).toList();
    }
}
