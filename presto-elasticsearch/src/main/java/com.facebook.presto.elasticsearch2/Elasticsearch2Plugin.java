package com.facebook.presto.elasticsearch2;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;

public class Elasticsearch2Plugin
        extends ElasticsearchPlugin
{
    public Elasticsearch2Plugin()
    {
        super("elasticsearch", new Elasticsearch2Module());
    }
}
