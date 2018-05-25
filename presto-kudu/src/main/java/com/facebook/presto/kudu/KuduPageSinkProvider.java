package com.facebook.presto.kudu;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KuduPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final KuduClientManager kuduClientManager;

    @Inject
    public KuduPageSinkProvider(KuduClientManager kuduClientManager)
    {
        this.kuduClientManager = requireNonNull(kuduClientManager, "kuduClientManager is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        KuduOutputTableHandle handle = (KuduOutputTableHandle) outputTableHandle;

        return new KuduPageSink(
                kuduClientManager,
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                true);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        requireNonNull(insertTableHandle, "tableHandle is null");
        checkArgument(insertTableHandle instanceof KuduOutputTableHandle, "tableHandle is not an instance of CassandraOutputTableHandle");

        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }
}
