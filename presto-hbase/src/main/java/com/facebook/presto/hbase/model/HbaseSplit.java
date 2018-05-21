package com.facebook.presto.hbase.model;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.Range;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HbaseSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String rowId;
    private final String schema;
    private final String table;
    //private final String serializerClassName;
    private final Optional<String> scanAuthorizations;
    private final Optional<String> hostPort;
    private final List<HostAddress> addresses;
    private final List<HbaseColumnConstraint> constraints;
    private final List<Range> ranges;

    @JsonCreator
    public HbaseSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
//            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("ranges") List<Range> ranges,
            @JsonProperty("constraints") List<HbaseColumnConstraint> constraints,
            @JsonProperty("scanAuthorizations") Optional<String> scanAuthorizations,
            @JsonProperty("hostPort") Optional<String> hostPort)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
//        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.constraints = ImmutableList.copyOf(requireNonNull(constraints, "constraints is null"));
        this.scanAuthorizations = requireNonNull(scanAuthorizations, "scanAuthorizations is null");
        this.hostPort = requireNonNull(hostPort, "hostPort is null");
        this.ranges = ImmutableList.copyOf(requireNonNull(ranges, "ranges is null"));

        // Parse the host address into a list of addresses, this would be an Accumulo Tablet server or some localhost thing
        if (hostPort.isPresent()) {
            addresses = ImmutableList.of(HostAddress.fromString(hostPort.get()));
        }
        else {
            addresses = ImmutableList.of();
        }
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Optional<String> getHostPort()
    {
        return hostPort;
    }

    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonIgnore
    public String getFullTableName()
    {
        return (this.getSchema().equals("default") ? "" : this.getSchema() + ".") + this.getTable();
    }

//    @JsonProperty
//    public String getSerializerClassName()
//    {
//        return this.serializerClassName;
//    }

    @JsonProperty("ranges")
    public List<Range> getRanges()
    {
        return ranges;
    }

//    @JsonIgnore
//    public List<Range> getRanges()
//    {
//        return ranges.stream().map(WrappedRange::getRange).collect(Collectors.toList());
//    }

    @JsonProperty
    public List<HbaseColumnConstraint> getConstraints()
    {
        return constraints;
    }

//    @SuppressWarnings("unchecked")
//    @JsonIgnore
//    public Class<? extends AccumuloRowSerializer> getSerializerClass()
//    {
//        try {
//            return (Class<? extends AccumuloRowSerializer>) Class.forName(serializerClassName);
//        }
//        catch (ClassNotFoundException e) {
//            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
//        }
//    }

    @JsonProperty
    public Optional<String> getScanAuthorizations()
    {
        return scanAuthorizations;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("rowId", rowId)
//                .add("serializerClassName", serializerClassName)
                .add("addresses", addresses)
                //.add("numRanges", ranges.size())
                .add("constraints", constraints)
                .add("scanAuthorizations", scanAuthorizations)
                .add("hostPort", hostPort)
                .toString();
    }
}
