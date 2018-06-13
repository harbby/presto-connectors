package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;

public class ElasticsearchTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle, ConnectorTableHandle
{
}
