package com.facebook.presto.hbase.model;

import com.facebook.presto.spi.predicate.Range;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TabletSplitMetadata
{
    private final Optional<String> hostPort;
    private final List<Range> ranges;

    @JsonCreator
    public TabletSplitMetadata(
            @JsonProperty("hostPort") Optional<String> hostPort,
            @JsonProperty("ranges") List<Range> ranges)
    {
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));
    }

    @JsonProperty
    public Optional<String> getHostPort()
    {
        return hostPort;
    }

    @JsonProperty
    public List<Range> getRanges()
    {
        return ranges;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostPort, ranges);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        TabletSplitMetadata other = (TabletSplitMetadata) obj;
        return Objects.equals(this.hostPort, other.hostPort)
                && Objects.equals(this.ranges, other.ranges);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hostPort", hostPort)
                .add("numRanges", ranges.size())
                .toString();
    }
}
