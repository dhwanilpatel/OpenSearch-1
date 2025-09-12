/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.List;

public class MergeScheduler {

    private MergeHandler mergeHandler;

    public MergeScheduler(PluginsService pluginsService, Any dataFormats, CustomIndexWriter customIndexWriter) {
        this.mergeHandler = new LuceneBasedMergeHandler(pluginsService, dataFormats, customIndexWriter);
    }


    public void triggerMerges() throws IOException {
        // TODO: Move the merge to seperate thread

        List<Merge> merges = mergeHandler.findMerges();

        // TODO: We can keep it as serial or concurrent similar to concurrent/serial merge scheduler

        for(Merge merge : merges) {
            mergeHandler.doMerge(merge);
        }

        // TODO: Call commit/refresh to update catalog snapshot with updated file and ref counts
    }
}
