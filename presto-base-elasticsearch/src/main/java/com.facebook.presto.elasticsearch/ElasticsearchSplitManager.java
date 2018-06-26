package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final BaseClient client;

    @Inject
    public ElasticsearchSplitManager(
            ElasticsearchConnectorId connectorId,
            BaseClient client)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ElasticsearchTableLayoutHandle layoutHandle = (ElasticsearchTableLayoutHandle) layout;
        ElasticsearchTableHandle tableHandle = layoutHandle.getTable();

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();

        //TODO: 这里需要重新实现, 目前只做实验
        // Get non-dsl column constraints
//        List<ElasticsearchColumnConstraint> constraints = getColumnConstraints("_dsl", layoutHandle.getConstraint());
//
//        // Get the dsl column range
//        Optional<Domain> rDom = getRangeDomain("_dsl", layoutHandle.getConstraint());
//
//        // Call out to our client to retrieve all tablet split metadata using the row ID domain and the secondary index
        List<ElasticsearchSplit> tabletSplits = client.getTabletSplits(tableHandle, layoutHandle); //tableHandle.getSerializerInstance()

        // Pack the tablet split metadata into a connector split
        return new FixedSplitSource(tabletSplits);
    }
}
