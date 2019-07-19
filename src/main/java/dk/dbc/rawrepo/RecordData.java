package dk.dbc.rawrepo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import dk.dbc.rawrepo.content.ContentJSON;

/**
 * Record data (meta + content) value object
 * <p>
 * Be advised that the byte[] exposed by the setContent() and getContent()
 * methods is a reference to the original data passed to the object
 * and therefore risks external mutation.
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RecordData {
    private RecordId recordId;
    private boolean deleted;
    private String mimetype;
    private String created;
    private String modified;
    private String trackingId;
    private ContentJSON contentJSON;
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

    public ContentJSON getContentJSON() {
        return contentJSON;
    }

    public void setContentJSON(ContentJSON contentJSON) {
        this.contentJSON = contentJSON;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public String getEnrichmentTrail() {
        return enrichmentTrail;
    }

    @Override
    public String toString() {
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
