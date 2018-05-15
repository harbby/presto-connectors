package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class HbasePageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final Connection hbaseClient;

    @Inject
    public HbasePageSinkProvider(Connection hbaseClient)
    {
        this.hbaseClient = requireNonNull(hbaseClient, "hbaseClient is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        HbaseTableHandle tableHandle = (HbaseTableHandle) outputTableHandle;
        //client.getTable(tableHandle.toSchemaTableName())
        return new HbasePageSink(hbaseClient, tableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }
}
