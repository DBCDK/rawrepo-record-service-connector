package dk.dbc.rawrepo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Arrays;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordData {

    public static class RecordId {
        private String bibliographicRecordId;
        private int agencyId;

        public RecordId () {
        }

        public RecordId (String bibliographicRecordId, int agencyId) {
            this.bibliographicRecordId = bibliographicRecordId;
            this.agencyId = agencyId;
        }

        public String getBibliographicRecordId () {
            return bibliographicRecordId;
        }

        public int getAgencyId () {
            return agencyId;
        }

        @Override
        public String toString () {
            return "RecordId{" +
                    "bibliographicRecordId='" + bibliographicRecordId + '\'' +
                    ", agencyId=" + agencyId +
                    '}';
        }
    }

    private RecordData.RecordId recordId;
    private boolean deleted;
    private String mimetype;
    private String created;
    private String modified;
    private String trackingId;
    private byte[] content;
    private String enrichmentTrail;

    public RecordId getRecordId() {
        return recordId;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public String getMimetype() {
        return mimetype;
    }

    public String getCreated() {
        return created;
    }

    public String getModified() {
        return modified;
    }

    public String getTrackingId() {
        return trackingId;
    }

    public byte[] getContent() {
        return content;
    }

    public String getEnrichmentTrail() { return enrichmentTrail; }

    @Override
    public String toString () {
        return "RecordData{" +
                "recordId=" + recordId +
                ", deleted=" + deleted +
                ", mimetype='" + mimetype + '\'' +
                ", created='" + created + '\'' +
                ", modified='" + modified + '\'' +
                ", trackingId='" + trackingId + '\'' +
                    ", enrichmentTrail='" + enrichmentTrail + '\'' +
                '}';
    }
}
