package com.epam.dataflow.jdbc;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@DefaultCoder(AvroCoder.class)
public class RangeHolder {

    private long rangeStart, rangeEnd;

    public RangeHolder() {}

    public RangeHolder(long rangeStart, long rangeEnd) {
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public long getRangeStart() {
        return rangeStart;
    }

    public long getRangeEnd() {
        return rangeEnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof RangeHolder)) return false;

        RangeHolder that = (RangeHolder) o;

        return new EqualsBuilder()
                .append(getRangeStart(), that.getRangeStart())
                .append(getRangeEnd(), that.getRangeEnd())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getRangeStart())
                .append(getRangeEnd())
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("rangeStart", rangeStart)
                .append("rangeEnd", rangeEnd)
                .toString();
    }
}
