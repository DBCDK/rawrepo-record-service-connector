package dk.dbc.rawrepo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Arrays;
import java.util.Objects;

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

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RecordId that = (RecordId) o;

            return agencyId == that.agencyId && bibliographicRecordId.equals(that.bibliographicRecordId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bibliographicRecordId, agencyId);
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

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
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

    public void setTrackingId(String trackingId) {
        this.trackingId = trackingId;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content.clone();
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
