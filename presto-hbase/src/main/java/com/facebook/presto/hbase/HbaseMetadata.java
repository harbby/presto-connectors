package com.facebook.presto.hbase;

import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.HTableDescriptor;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class HbaseMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final HbaseClient client;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    @Inject
    public HbaseMetadata(
            HbaseConnectorId connectorId,
            HbaseClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = client.getSchemaNames();
        }
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : client.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        boolean exists = client.tableExists(tableName);
        if (exists) {
            return new HbaseTableHandle(
                    connectorId
                    , tableName.getSchemaName()
                    , tableName.getTableName()
            );
        }
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        HbaseTableHandle tableHandle = (HbaseTableHandle) table;
        return new ConnectorTableMetadata(
                tableHandle.toSchemaTableName(),
                client.getColumns(tableHandle.toSchemaTableName()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
//        HbaseTableHandle hbaseTableHandle = (HbaseTableHandle) tableHandle;
//        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
//        int index = 0;
//
//        client.getTable(hbaseTableHandle.toSchemaTableName())
//        for (ColumnMetadata column : client.getTable(hbaseTableHandle.toSchemaTableName())) {
//            int ordinalPosition;
//            ordinalPosition = index;
//            index++;
//            HbaseColumnHandle columnHandle = new HbaseColumnHandle(
//                    column.getName(),
//                    column.g,
//                    ordinalPosition
//            )
//            columnHandles.put(column.getName(), columnHandle);
//        }
//        return columnHandles.build();
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return null;
    }
}
