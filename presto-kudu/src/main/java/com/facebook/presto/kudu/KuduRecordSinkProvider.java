package com.facebook.presto.kudu;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.connector.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KuduRecordSinkProvider
        implements ConnectorRecordSinkProvider
{
    private final KuduClientManager kuduClientManager;

    @Inject
    public KuduRecordSinkProvider(KuduClientManager kuduClientManager)
    {
        this.kuduClientManager = requireNonNull(kuduClientManager, "kuduClientManager is null");
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KuduOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");
        KuduOutputTableHandle handle = (KuduOutputTableHandle) tableHandle;

        return new KuduRecordSink(
                kuduClientManager,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                true);
    }

    @Override
    public RecordSink getRecordSink(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof KuduOutputTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        KuduOutputTableHandle handle = (KuduOutputTableHandle) tableHandle;

        return new KuduRecordSink(
                kuduClientManager,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                false);
    }
}
