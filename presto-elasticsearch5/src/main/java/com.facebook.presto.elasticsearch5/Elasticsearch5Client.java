package com.facebook.presto.elasticsearch5;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.ElasticsearchTable;
import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.inject.Inject;
import org.elasticsearch.client.Client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Elasticsearch5Client
        implements BaseClient
{
    private final Client client;

    @Inject
    public Elasticsearch5Client(
            Client client,
            ElasticsearchConfig elasticsearchConfig)
    {
        this.client = requireNonNull(client, "elasticsearch client is null");
    }

    @Override
    public Set<String> getSchemaNames()
    {
        return null;
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        return null;
    }

    @Override
    public List<ElasticsearchSplit> getTabletSplits(ElasticsearchTableHandle tableHandle, ElasticsearchTableLayoutHandle layoutHandle)
    {
        return null;
    }

    @Override
    public Iterator<Stream<Map<String, Object>>> execute(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns)
    {
        return null;
    }

    @Override
    public ElasticsearchTable getTable(SchemaTableName tableName)
    {
        return null;
    }
}
