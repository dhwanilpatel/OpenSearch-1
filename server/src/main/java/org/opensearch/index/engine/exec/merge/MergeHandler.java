/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MergeHandler {

    private final Any dataFormats;
    private final PluginsService pluginsService;

    public abstract List<Merge> findMerges() throws IOException;

    public MergeHandler(PluginsService pluginsService, Any dataFormats) {
        this.dataFormats = dataFormats;
        this.pluginsService = pluginsService;
    }

    public void doMerge(Merge merge) throws IOException {
        Map<DataFormat, Merger> mergers = new HashMap<>();
        Map<DataFormat, FileMetadata> mergedFiles = new HashMap<>();

        for (DataFormat dataFormat : dataFormats.getDataFormats()) {
            DataFormatPlugin plugin = pluginsService.filterPlugins(DataFormatPlugin.class).stream()
                .filter(curr -> curr.getDataFormat().equals(dataFormat))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("dataformat [" + dataFormat + "] is not registered."));

            mergers.put(plugin.getDataFormat(), plugin.indexingEngine().getMerger());
        }

        // Merging for primary data format
        Merger primaryDataFormatMerger = mergers.get(dataFormats.getPrimaryDataFormat());
        List<FileMetadata> filesToMerge = getFilesToMerge(merge, dataFormats.getPrimaryDataFormat());
        Tuple<Map<Tuple<String, String>, String>, FileMetadata> primaryMerge = primaryDataFormatMerger.merge(filesToMerge);
        mergedFiles.put(dataFormats.getPrimaryDataFormat(), primaryMerge.v2());

        Map<Tuple<String, String>, String> oldSegRowIdToNewRowId = primaryMerge.v1();

        // Merging other format as per the old segment + row id -> new row id mapping.
        mergers.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(dataFormats.getPrimaryDataFormat()))
            .forEach(entry -> {
                List<FileMetadata> files = getFilesToMerge(merge, entry.getKey());
                FileMetadata mergedFile = entry.getValue().merge(files, oldSegRowIdToNewRowId);
                mergedFiles.put(entry.getKey(), mergedFile);
            });
    }

    public List<FileMetadata> getFilesToMerge(Merge merge, DataFormat dataFormat) {
        List<FileMetadata> files = new ArrayList<>();
        // Check file names of merges for other data format and find correct file names and return
        return files;
    }
}
