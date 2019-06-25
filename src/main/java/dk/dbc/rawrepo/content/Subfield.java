package dk.dbc.rawrepo.content;

import java.util.Objects;

public class Subfield {

    private String name;
    private String value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Subfield{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subfield subfield = (Subfield) o;
        return Objects.equals(name, subfield.name) &&
                Objects.equals(value, subfield.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }
}
