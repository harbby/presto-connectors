package com.facebook.presto.elasticsearch6;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;
import com.facebook.presto.elasticsearch6.functions.MatchQueryFunction;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Elasticsearch6Plugin
        extends ElasticsearchPlugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MatchQueryFunction.class)
                .build();
    }

    public Elasticsearch6Plugin()
    {
        super("elasticsearch6", new Elasticsearch6Module());
    }
}
