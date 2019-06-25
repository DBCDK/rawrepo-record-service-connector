package dk.dbc.rawrepo.content;

import java.util.List;
import java.util.Objects;

public class Field {

    private String name;
    private String indicators;
    private List<Subfield> subfields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndicators() {
        return indicators;
    }

    public void setIndicators(String indicators) {
        this.indicators = indicators;
    }

    public List<Subfield> getSubfields() {
        return subfields;
    }

    public void setSubfields(List<Subfield> subfields) {
        this.subfields = subfields;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", indicators='" + indicators + '\'' +
                ", subfields=" + subfields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return Objects.equals(name, field.name) &&
                Objects.equals(indicators, field.indicators) &&
                Objects.equals(subfields, field.subfields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indicators, subfields);
    }
}
