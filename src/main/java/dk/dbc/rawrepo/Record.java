package dk.dbc.rawrepo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Record {
    private RecordId recordId;
    private boolean deleted;
    private String mimetype;
    private String created;
    private String modified;
    private String trackingId;
    private byte[] content;

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
}
