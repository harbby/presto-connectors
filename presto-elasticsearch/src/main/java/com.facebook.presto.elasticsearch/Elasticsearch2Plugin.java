package com.facebook.presto.elasticsearch;

public class Elasticsearch2Plugin
        extends ElasticsearchPlugin
{
    public Elasticsearch2Plugin()
    {
        super("elasticsearch", new Elasticsearch2Module());
    }
}
