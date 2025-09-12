/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.merge.*;

import java.io.IOException;

public class CommitHandler {

    MergeScheduler mergeScheduler;
    public CommitHandler(MergeScheduler mergeScheduler) {
        this.mergeScheduler = mergeScheduler;
    }

    public void commit() throws IOException {
        // Other logic for commit like doing refresh/fsync/etc

        // at the end trigger merge
        this.mergeScheduler.triggerMerges();
    }
}
