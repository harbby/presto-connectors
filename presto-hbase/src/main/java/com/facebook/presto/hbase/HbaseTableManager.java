package com.facebook.presto.hbase;

import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_DNE;
import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_EXISTS;
import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * This class is a light wrapper for Hbase's Connector object.
 * It will perform the given operation, or throw an exception if an Hbase- or ZooKeeper-based error occurs.
 */
public class HbaseTableManager
{
    private static final Logger LOG = Logger.get(HbaseTableManager.class);
    private static final String DEFAULT = "default";
    private final Connection connection;

    @Inject
    public HbaseTableManager(Connection connection)
    {
        this.connection = requireNonNull(connection, "connection is null");
    }

    /**
     * Ensures the given Hbase namespace exist, creating it if necessary
     *
     * @param schema Presto schema (Hbase namespace)
     */
    public void ensureNamespace(String schema)
    {
        try {
            // If the table schema is not "default" and the namespace does not exist, create it
            Set<String> namespaces = Arrays.stream(connection.getAdmin().listNamespaceDescriptors())
                    .map(x -> x.getName()).collect(Collectors.toSet());
            if (!schema.equals(DEFAULT) && !namespaces.contains(schema)) {
                connection.getAdmin().createNamespace(NamespaceDescriptor.create(schema).build());
            }
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to check for existence or create Hbase namespace", e);
        }
    }

    public boolean exists(String table)
    {
        try {
            return connection.getAdmin().tableExists(TableName.valueOf(table));
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to check for existence Hbase table", e);
        }
    }

    public void createHbaseTable(String table, Set<HColumnDescriptor> familys)
    {
        try (Admin admin = connection.getAdmin()) {
            HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(table));

            for (HColumnDescriptor family : familys) {
                hbaseTable.addFamily(family);
            }
            admin.createTable(hbaseTable);
        }
        catch (TableExistsException e) {
            throw new PrestoException(HBASE_TABLE_EXISTS, "Hbase table already exists", e);
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to create Hbase table", e);
        }
    }

//    public void setIterator(String table, IteratorSetting setting)
//    {
//        try {
//            // Remove any existing iterator settings of the same name, if applicable
//            Map<String, EnumSet<IteratorScope>> iterators = connector.tableOperations().listIterators(table);
//            if (iterators.containsKey(setting.getName())) {
//                connector.tableOperations().removeIterator(table, setting.getName(), iterators.get(setting.getName()));
//            }
//
//            connector.tableOperations().attachIterator(table, setting);
//        }
//        catch (TableNotFoundException e) {
//            throw new PrestoException(HBASE_TABLE_DNE, "Failed to set iterator, table does not exist", e);
//        }
//    }

    public void deleteHbaseTable(String tableName)
    {
        try (Admin admin = connection.getAdmin()) {
            TableName htableName = TableName.valueOf(tableName);
            admin.disableTable(htableName);
            if (admin.isTableDisabled(htableName)) {
                admin.deleteTable(htableName);
            }
            else {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to delete Hbase table, TableDisabled is false");
            }
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(HBASE_TABLE_DNE, "Failed to delete Hbase table, does not exist", e);
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to delete Hbase table", e);
        }
    }

    public void renameHbaseTable(String oldName, String newName)
    {
        throw new PrestoException(NOT_SUPPORTED, "hbase catalog NOT_SUPPORTED rename table name");
    }
}
