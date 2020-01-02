package com.facebook.presto.elasticsearch.io;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.ElasticsearchTable;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchOutputTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

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
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkProperties pageSinkProperties) {
        ElasticsearchOutputTableHandle tableHandle = (ElasticsearchOutputTableHandle) outputTableHandle;
        //throw new UnsupportedOperationException("this method have't support!");
        return new ElasticsearchPageSink(client, tableHandle.getSchemaTableName(), tableHandle.getColumns());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkProperties pageSinkProperties) {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) insertTableHandle;
        ElasticsearchTable table = client.getTable(tableHandle.getSchemaTableName());
        List<ElasticsearchColumnHandle> columns = table.getColumns().stream()
                .filter(x -> !x.isHidden()).collect(Collectors.toList());
        return new ElasticsearchPageSink(client, tableHandle.getSchemaTableName(), columns);
    }
}
