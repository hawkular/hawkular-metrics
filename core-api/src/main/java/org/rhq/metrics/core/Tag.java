package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class Tag {

    public static final String NO_DESCRIPTION = "";

    private String value;

    private String description = NO_DESCRIPTION;

    public Tag(String tag) {
        value = tag;
    }

    public Tag(String tag, String description) {
        value = tag;
        this.description = description;
    }

    public String getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tag tag = (Tag) o;

        if (!value.equals(tag.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("value", value).add("description", description).toString();
    }
}
