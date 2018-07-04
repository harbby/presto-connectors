package com.facebook.presto.elasticsearch.io;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final BaseClient client;

    @Inject
    public ElasticsearchPageSinkProvider(
            BaseClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) outputTableHandle;
        //throw new UnsupportedOperationException("this method have't support!");
        return new ElasticsearchPageSink(client, tableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        return createPageSink(transactionHandle, session, (ConnectorOutputTableHandle) insertTableHandle);
    }
}
