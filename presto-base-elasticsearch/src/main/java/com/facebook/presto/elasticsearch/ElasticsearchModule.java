package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.conf.ElasticsearchSessionProperties;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSinkProvider;
import com.facebook.presto.elasticsearch.io.ElasticsearchPageSourceProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ElasticsearchModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ElasticsearchConfig.class);

        binder.bind(EsTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSinkProvider.class).in(Scopes.SINGLETON);

//        binder.bind(ElasticsearchTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSessionProperties.class).in(Scopes.SINGLETON);
    }
}
