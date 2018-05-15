/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kudu;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.kudu.client.KuduException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.kudu.KuduErrorCode.KUDU_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class KuduMetadata
        implements ConnectorMetadata
{
    public static final Logger logger = Logger.get(KuduMetadata.class);

    public static final String PRESTO_KUDU_SCHEMA = "default";
    private static final List<String> SCHEMA_NAMES = ImmutableList.of(PRESTO_KUDU_SCHEMA);

    private final KuduTableManager kuduTableManager;
    private final String connectorId;

    @Inject
    public KuduMetadata(KuduConnectorId connectorId, KuduTableManager kuduTableManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kuduTableManager = requireNonNull(kuduTableManager, "kuduTables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        return kuduTableManager.getTables().get(tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(
                tableHandle.getSchemaTableName(), kuduTableManager.getColumns(tableHandle));
        return connectorTableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        List<SchemaTableName> schemaTableNames = new ArrayList<>(kuduTableManager.getTables()
                .keySet());
        return schemaTableNames;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        ConnectorTableLayout layout = new ConnectorTableLayout(new KuduTableLayoutHandle(tableHandle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        KuduTableLayoutHandle layout = Types.checkType(handle, KuduTableLayoutHandle.class, "layout");
        return new ConnectorTableLayout(layout);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        KuduTableHandle tableHandle = Types.checkType(table, KuduTableHandle.class, "tableHandle");
        return getColumnHandles(tableHandle);
    }

    private Map<String, ColumnHandle> getColumnHandles(KuduTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;

        for (ColumnMetadata column : kuduTableManager.getColumns(tableHandle)) {
            int ordinalPosition;
            ordinalPosition = index;
            index++;
            columnHandles.put(column.getName(), new KuduColumnHandle(column.getName(), column.getType(), ordinalPosition));
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        Types.checkType(tableHandle, KuduTableHandle.class, "tableHandle");
        return Types.checkType(columnHandle, KuduColumnHandle.class, "columnHandle").toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            KuduTableHandle tableHandle = kuduTableManager.getTables().get(tableName);
            if (tableHandle != null) {
                columns.put(tableName, kuduTableManager.getColumns(tableHandle));
            }
        }
        return columns.build();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return emptyList();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return emptyMap();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty());
        finishCreateTable(session, outputTableHandle, ImmutableList.of());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            //column.getExtraInfo()
            //column.getComment();  //primary key and partition
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        //tableMetadata.getProperties() 获取表的配置 这个还需要进一步测试
        // get the root directory for the database
        String tableName = tableMetadata.getTable().getTableName();

        // We need to create the Cassandra table before commit because the record needs to be written to the table.

        try {
            kuduTableManager.createTable(tableMetadata, tableName);
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_ERROR, "creating TABLE ERROR WITH KUDU", e);
        }
        return new KuduOutputTableHandle(
                connectorId,
                "default",
                tableName,
                columnNames.build(),
                columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        KuduTableHandle table = (KuduTableHandle) tableHandle;

        List<ColumnMetadata> columns = kuduTableManager.getColumns(table);
        List<String> columnNames = columns.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        List<Type> columnTypes = columns.stream().map(ColumnMetadata::getType).collect(Collectors.toList());

        //----------返回----------
        return new KuduOutputTableHandle(
                connectorId,
                "default",
                table.getTableName(),
                columnNames,
                columnTypes);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        try {
            kuduTableManager.dropTable(((KuduTableHandle) tableHandle).getSchemaTableName().getTableName());
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_ERROR, "drop TABLE ERROR", e);
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KuduTableHandle table = (KuduTableHandle) tableHandle;
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates or deletes");
        //return new KuduColumnHandle("day", INTEGER, 0);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support deletes");
//        KuduTableHandle table = (KuduTableHandle) tableHandle;
//        //----------执行删除--------
//        return new KuduTableHandle(
//                connectorId,
//                table.getSchemaTableName());
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        //----------删除完成--------
    }
}
