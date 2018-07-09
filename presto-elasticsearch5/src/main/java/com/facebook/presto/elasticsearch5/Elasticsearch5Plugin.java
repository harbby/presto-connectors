package com.facebook.presto.elasticsearch5;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Elasticsearch5Plugin
        extends ElasticsearchPlugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
//                .add(MatchQueryFunction.class)
                .build();
    }

    public Elasticsearch5Plugin()
    {
        super("elasticsearch5", new Elasticsearch5Module());
    }
}
