package com.facebook.presto.elasticsearch5;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;

public class Elasticsearch5Plugin
        extends ElasticsearchPlugin
{
    public Elasticsearch5Plugin()
    {
        super("elasticsearch5", new Elasticsearch5Module());
    }
}
