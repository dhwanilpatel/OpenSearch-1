package org.opensearch.index.engine.exec.merge;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.engine.DataFormatPlugin;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class OpensearchMultipleEngineMergeScheduler extends ConcurrentMergeScheduler {

    private final Any dataFormats;
    private final PluginsService pluginsService;

    public OpensearchMultipleEngineMergeScheduler(PluginsService pluginsService, Any dataFormats) {
        this.dataFormats = dataFormats;
        this.pluginsService = pluginsService;
    }

    public void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
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

        // TODO: Need to update merged segment info with merged file metadata for each data format

        // TODO: Need to update in memory mapping of old id to new id in doc value consumer of row id,
        //  so in lucene those ids are updated. POC is pending for this lucene flow.

        super.doMerge(mergeSource, merge); // Merge on lucene side with doc value consume with row id mapping
    }

    public List<FileMetadata> getFilesToMerge(MergePolicy.OneMerge merge, DataFormat dataFormat) {
        List<FileMetadata> files = new ArrayList<>();
        merge.segments.forEach(segment -> {
            files.add(new FileMetadata(dataFormat, segment.info.getAttribute(dataFormat.name())));
        });
        return files;
    }
}
