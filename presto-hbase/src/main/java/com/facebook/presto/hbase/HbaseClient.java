package com.facebook.presto.hbase;

import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public final class HbaseClient
{
    private final Connection connection;

    @Inject
    public HbaseClient(
            HbaseConnectorId connectorId,
            HbaseConfig hbaseConfig,
            NodeManager nodeManager,
            Connection connection)
    {
        this.connection = requireNonNull(connection, "hbaseClient is null");
    }

    public Set<String> getSchemaNames()
    {
        try {
            return Arrays.stream(connection.getAdmin().listNamespaceDescriptors())
                    .map(x -> x.getName()).collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }

    public Set<String> getTableNames()
    {
        try {
            return Arrays.stream(connection.getAdmin().listTableNames())
                    .map(x -> x.getNameAsString()).collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }

    public Set<String> getTableNames(String schema)
    {
        try {
            return Arrays.stream(connection.getAdmin().listTableNamesByNamespace(schema))
                    .map(x -> x.getNameAsString().replace(schema + ":", "")
                    ).collect(Collectors.toSet());
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }

    public HTableDescriptor getTable(SchemaTableName schemaTableName)
    {
        try {
            return connection.getAdmin()
                    .getTableDescriptor(TableName.valueOf(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }

    public List<ColumnMetadata> getColumns(SchemaTableName schemaTableName)
    {
        try {
            HTableDescriptor descriptor = connection.getAdmin()
                    .getTableDescriptor(TableName.valueOf(schemaTableName.getSchemaName(), schemaTableName.getTableName()));

            return Arrays.stream(descriptor.getColumnFamilies()).map(x ->
                    new ColumnMetadata(x.getNameAsString(), VARCHAR)).collect(Collectors.toList());
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }

    public boolean tableExists(SchemaTableName schemaTableName)
    {
        try {
            return connection.getAdmin()
                    .tableExists(TableName.valueOf(schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, e);
        }
    }
}
