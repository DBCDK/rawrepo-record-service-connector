package dk.dbc.rawrepo;

import java.util.Objects;

public class RecordId implements Comparable<RecordId> {
    private String bibliographicRecordId;
    private int agencyId;

    public RecordId() {
    }

    public RecordId(String bibliographicRecordId, int agencyId) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.agencyId = agencyId;
    }

    public String getBibliographicRecordId() {
        return bibliographicRecordId;
    }

    public int getAgencyId() {
        return agencyId;
    }

    @Override
    public String toString() {
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

    @Override
    public int compareTo(RecordId other) {
        int ret = Integer.compare(agencyId, other.agencyId);
        if (ret == 0)
            ret = bibliographicRecordId.compareTo(other.bibliographicRecordId);
        return ret;
    }
}