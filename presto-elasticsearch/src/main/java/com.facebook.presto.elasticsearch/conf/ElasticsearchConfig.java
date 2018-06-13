package com.facebook.presto.elasticsearch.conf;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class ElasticsearchConfig
{
    private String elasticsearchHosts;
    private String clusterName;

    public String getElasticsearchHosts()
    {
        return elasticsearchHosts;
    }

    @Config("elasticsearch.transport.hosts")
    @ConfigDescription("IP:PORT where Elasticsearch Transport hosts connect")
    public ElasticsearchConfig setElasticsearchHosts(String elasticsearchHosts)
    {
        this.elasticsearchHosts = elasticsearchHosts;
        return this;
    }

    @NotNull
    public String getClusterName()
    {
        return this.clusterName;
    }

    @Config("elasticsearch.cluster.name")
    @ConfigDescription("Elasticsearch cluster name string")
    public ElasticsearchConfig setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }
}
