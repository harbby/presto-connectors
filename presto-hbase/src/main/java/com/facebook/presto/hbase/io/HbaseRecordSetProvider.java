package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.HbaseConnectorId;
import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Connection;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HbaseRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final Connection connection;
    private final HbaseConfig config;

    @Inject
    public HbaseRecordSetProvider(
            Connection connection,
            HbaseConnectorId connectorId,
            HbaseConfig config)
    {
        this.connection = requireNonNull(connection, "connection is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        HbaseSplit hbaseSplit = (HbaseSplit) split;
        checkArgument(hbaseSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<HbaseColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((HbaseColumnHandle) handle);
        }

        return new HbaseRecordSet(connection, session, hbaseSplit, handles.build());
    }
}
