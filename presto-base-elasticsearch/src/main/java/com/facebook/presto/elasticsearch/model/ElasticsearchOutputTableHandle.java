package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ElasticsearchOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final List<ElasticsearchColumnHandle> columns;

    @JsonCreator
    public ElasticsearchOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<ElasticsearchColumnHandle> columns)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public List<ElasticsearchColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ElasticsearchOutputTableHandle other = (ElasticsearchOutputTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaName + ":" + tableName + ":" + columns;
    }
}
