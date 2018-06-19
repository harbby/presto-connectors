package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.ColumnHandle;

public class ElasticsearchColumnHandle implements ColumnHandle, Comparable<ElasticsearchColumnHandle>
{
    @Override
    public int compareTo(ElasticsearchColumnHandle o)
    {
        return 0;
    }
}
