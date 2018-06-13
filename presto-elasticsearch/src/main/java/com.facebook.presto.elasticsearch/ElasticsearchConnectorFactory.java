package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class ElasticsearchConnectorFactory
        implements ConnectorFactory
{
    private final String name;

    public ElasticsearchConnectorFactory(final String name)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
    }

    @Override
    public String getName()
    {
        return this.name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return null;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        return null;
    }
}
