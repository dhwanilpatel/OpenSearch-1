/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.engine.exec.coord.IndexingManager;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MergeScheduler {

    private MergeHandler mergeHandler;
    private CompositeEngine compositeEngine;

    public MergeScheduler(MergeHandler mergeHandler, CompositeEngine compositeEngine) {
        this.mergeHandler = mergeHandler;
        this.compositeEngine = compositeEngine;
    }


    public void triggerMerges() throws IOException {
        // TODO: Move the merge to seperate thread
        System.out.println("In merge scheduler trigger merge ===== ");
        Collection<Merge> merges = mergeHandler.findMerges();

        // TODO: We can keep it as serial or concurrent similar to concurrent/serial merge scheduler

        for(Merge merge : merges) {
            MergeResult mergeResult = mergeHandler.doMerge(merge);
            System.out.println("Merge ====== " + merge + " merge result ==== " + mergeResult + " mapping === " + mergeResult.getMergedFileMetadata());
            this.compositeEngine.applyMergeChanges(mergeResult);
        }
    }
}
