/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.util.Collection;

public interface Merger {
    /**
     *
     * @param fileMetadataList List of FileMetadata to merge
     * @return {@code Tuple<Map<Tuple<String, String>, String>}
     *     First item -> Mapping of old segment + old rowId to new rowId
     *     Second item -> Merged FileMetadata
     */
    MergeResult merge(Collection<FileMetadata> fileMetadataList);

    /**
     *
     * @param fileMetadataList List of FileMetadata to merge
     * @param rowIdMapping Mapping of old segment + old rowId to new rowId
     * @return FileMetadata Merged FileMetadata
     */
    MergeResult merge(Collection<FileMetadata> fileMetadataList, RowIdMapping rowIdMapping);
}
