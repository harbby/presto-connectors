package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.model.HbaseTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class HbasePageSink
        implements ConnectorPageSink
{
    public HbasePageSink(
            Connection hbaseClient,
            HbaseTableHandle tableHandle)
    {
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return null;
    }

    @Override
    public void abort()
    {

    }
}
