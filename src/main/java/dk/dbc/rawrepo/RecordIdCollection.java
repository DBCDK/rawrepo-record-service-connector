package dk.dbc.rawrepo;

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
    RecordId[] toArray() {
        return recordIds.stream()
                .sorted()
                .toArray(RecordId[]::new);
    }

}
