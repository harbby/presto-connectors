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

import com.facebook.presto.kudu.util.KuduUtil;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.kudu.KuduErrorCode.KUDU_ERROR;
import static com.facebook.presto.kudu.KuduMetadata.PRESTO_KUDU_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class KuduTableManager
{
    private KuduClientManager kuduClientManager;
    private final String connectorId;

    @Inject
    public KuduTableManager(KuduConnectorId connectorId, KuduClientManager kuduClientManager)
    {
        this.connectorId = requireNonNull(connectorId, "clientId is null").toString();
        this.kuduClientManager = requireNonNull(kuduClientManager, "kuduClientManager is null");
    }

    public Map<SchemaTableName, KuduTableHandle> getTables()
    {
        Map<SchemaTableName, KuduTableHandle> tables = null;
        ImmutableMap.Builder<SchemaTableName, KuduTableHandle> tablesBuilder = ImmutableMap.builder();
//        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumnsBuilder = ImmutableMap.builder();
        List<String> listTable = null;
        KuduClient kuduClient = kuduClientManager.getClient();
        try {
            listTable = kuduClient.getTablesList().getTablesList();
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_ERROR, e);
        }
        finally {
            kuduClientManager.close(kuduClient);
        }

        for (String table : listTable) {
            SchemaTableName schemaTableName = new SchemaTableName(PRESTO_KUDU_SCHEMA, table);
            tablesBuilder.put(schemaTableName, new KuduTableHandle(connectorId, schemaTableName));
        }

        tables = tablesBuilder.build();

        return tables;
    }

    public List<ColumnMetadata> getColumns(KuduTableHandle tableHandle)
    {
        List<ColumnMetadata> columnMetadatas = new ArrayList<ColumnMetadata>();
        String tableName = tableHandle.getTableName();
        KuduClient kuduClient = kuduClientManager.getClient();
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();
            for (ColumnSchema columnSchema : columnSchemas) {
                ColumnMetadata columnMetadata = null;

                switch (columnSchema.getType()) {
                    case STRING:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), VARCHAR);
                        break;
                    case BOOL:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BOOLEAN);
                        break;
                    case INT8:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), TINYINT);
                        break;
                    case INT16:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), SMALLINT);
                        break;
                    case INT32:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), INTEGER);
                        break;
                    case INT64:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BIGINT);
                        break;
                    case FLOAT:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), DOUBLE);
                        break;
                    case DOUBLE:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), DOUBLE);
                        break;
                    case UNIXTIME_MICROS:
                        columnMetadata = new ColumnMetadata(columnSchema.getName(), BIGINT);
                        break;
                }
                if (null != columnMetadata) {
                    columnMetadatas.add(columnMetadata);
                }
                else {
                    throw new IllegalArgumentException("The provided data type doesn't map" +
                            " to know any known one.");
                }
            }
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_ERROR, tableName + " open failed", e);
        }
        finally {
            kuduClientManager.close(kuduClient);
        }
        return columnMetadatas;
    }

    public void dropTable(String tableName)
            throws KuduException
    {
        KuduClient kuduClient = kuduClientManager.getClient();
        try {
            kuduClient.deleteTable(tableName);
        }
        finally {
            kuduClientManager.close(kuduClient);
        }
    }

    public void createTable(ConnectorTableMetadata tableMetadata, String tableName)
            throws KuduException
    {
        KuduClient kuduClient = kuduClientManager.getClient();
        try {
            KuduUtil.createTable(tableMetadata, tableName, kuduClient);
        }
        finally {
            kuduClientManager.close(kuduClient);
        }
    }
}
