package com.facebook.presto.hbase;

import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.io.HbasePageSinkProvider;
import com.facebook.presto.hbase.io.HbaseRecordSetProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class HbaseModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HbaseConfig.class);

        binder.bind(HbaseClient.class).in(Scopes.SINGLETON);

        binder.bind(HbaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(HbaseMetadata.class).in(Scopes.SINGLETON);
        binder.bind(HbaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HbaseRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HbasePageSinkProvider.class).in(Scopes.SINGLETON);

        binder.bind(Connection.class).toProvider(ConnectorProvider.class);
    }

    private static class ConnectorProvider
            implements Provider<Connection>
    {
        private static final Logger LOG = Logger.get(ConnectorProvider.class);

//        private final String instance;
//        private final String zooKeepers;
//        private final String username;
//        private final String password;

        @Inject
        public ConnectorProvider(HbaseConfig config)
        {
            requireNonNull(config, "config is null");
//            this.instance = config.getInstance();
//            this.zooKeepers = config.getZooKeepers();
//            this.username = config.getUsername();
//            this.password = config.getPassword();
        }

        @Override
        public Connection get()
        {
            try {
                Configuration conf = HBaseConfiguration.create();
                Connection connection = ConnectionFactory.createConnection(conf);
//                Instance inst = new ZooKeeperInstance(instance, zooKeepers);
//                Connector connector = inst.getConnector(username, new PasswordToken(password.getBytes(UTF_8)));
                LOG.info("Connection to instance %s at %s established, user %s");
                //LOG.info("Connection to instance %s at %s established, user %s", instance, zooKeepers, username);
                return connection;
            }
            catch (IOException e) {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to get connection to HBASE", e);
            }
        }
    }
}
