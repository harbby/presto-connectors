package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface BaseClient
{
    public Set<String> getSchemaNames();

    public Set<String> getTableNames(String schema);

    public List<ElasticsearchSplit> getTabletSplits(ElasticsearchTableHandle tableHandle, ElasticsearchTableLayoutHandle layoutHandle);

    public Iterator<Stream<Map<String, Object>>> execute(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns);

    public ElasticsearchTable getTable(SchemaTableName tableName);
}
