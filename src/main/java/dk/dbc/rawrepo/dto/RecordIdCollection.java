/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt or at https://opensource.dbc.dk/licenses/gpl-3.0/
 */

package dk.dbc.rawrepo.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Record id collection value object
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordIdCollection {

    List<RecordId> recordIds;

    public List<RecordId> getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(List<RecordId> recordIds) {
        this.recordIds = recordIds;
    }

    @Override
    public String toString() {
        return "RecordIdCollectionDTO{" + "recordIds=" + recordIds + '}';
    }

    /**
     * Convert to sorted (by agency / bibliographicRecordId) array
     *
     * @return sorted array
     */
    public RecordId[] toArray() {
        return recordIds.stream()
                .sorted()
                .toArray(RecordId[]::new);
    }

}
