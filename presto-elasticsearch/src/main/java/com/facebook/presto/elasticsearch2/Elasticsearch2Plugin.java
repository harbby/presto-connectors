package com.facebook.presto.elasticsearch2;

import com.facebook.presto.elasticsearch.ElasticsearchPlugin;
import com.facebook.presto.elasticsearch2.functions.MatchQueryFunction;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class Elasticsearch2Plugin
        extends ElasticsearchPlugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(MatchQueryFunction.class)
                .build();
    }

    public Elasticsearch2Plugin()
    {
        super("elasticsearch", new Elasticsearch2Module());
    }
}
