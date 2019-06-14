package dk.dbc.rawrepo.content;

import java.util.List;

public class ContentJSON {

    private String leader;
    private List<Field> fields;

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "ContentJSON{" +
                "leader='" + leader + '\'' +
                ", fields=" + fields +
                '}';
    }
}
