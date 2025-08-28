/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.collect.Tuple;

import java.util.List;
import java.util.Map;

public interface Merger {
    /**
     *
     * @param fileMetadataList List of FileMetadata to merge
     * @return {@code Tuple<Map<Tuple<String, String>, String>}
     *     First item -> Mapping of old segment + old rowId to new rowId
     *     Second item -> Merged FileMetadata
     */
    Tuple<Map<Tuple<String, String>, String>, FileMetadata> merge(List<FileMetadata> fileMetadataList /*, MapperService or any other needed service*/);

    /**
     *
     * @param fileMetadataList List of FileMetadata to merge
     * @param rowIdMapping Mapping of old segment + old rowId to new rowId
     * @return FileMetadata Merged FileMetadata
     */
    FileMetadata merge(List<FileMetadata> fileMetadataList, Map<Tuple<String, String>, String> rowIdMapping /*, MapperService or any other needed service*/);
}
