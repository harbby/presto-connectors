package com.facebook.presto.elasticsearch2;

import com.facebook.presto.elasticsearch.BaseClient;
import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.airlift.log.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import javax.inject.Provider;

import java.io.IOException;
import java.net.InetAddress;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.UNEXPECTED_ES_ERROR;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class Elasticsearch2Module
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ElasticsearchConfig.class);

        binder.bind(BaseClient.class).to(Elasticsearch2Client.class);

//        binder.bind(ElasticsearchTableProperties.class).in(Scopes.SINGLETON);
//        binder.bind(ElasticsearchSessionProperties.class).in(Scopes.SINGLETON);

        binder.bind(Client.class).toProvider(ConnectionProvider.class);
    }

    private static class ConnectionProvider
            implements Provider<Client>
    {
        private static final Logger LOG = Logger.get(ConnectionProvider.class);
        private final String clusterName;
        private final String hosts;

        @Inject
        public ConnectionProvider(ElasticsearchConfig config)
        {
            requireNonNull(config, "config is null");
            this.clusterName = config.getClusterName();
            this.hosts = config.getElasticsearchHosts();
        }

        @Override
        public Client get()
        {
            try {
                Settings settings = Settings.builder().put("cluster.name", clusterName)
                        .put("client.transport.sniff", true).build();
                TransportClient client = TransportClient.builder().settings(settings).build();
                for (String ip : hosts.split(",")) {
                    client.addTransportAddress(
                            new InetSocketTransportAddress(InetAddress.getByName(ip.split(":")[0]),
                                    Integer.parseInt(ip.split(":")[1])));
                }
                LOG.info("Connection to instance %s at %s established, user %s");
                return client;
            }
            catch (IOException e) {
                throw new PrestoException(UNEXPECTED_ES_ERROR, "Failed to get connection to Elasticsearch", e);
            }
        }
    }
}
