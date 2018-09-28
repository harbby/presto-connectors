package com.facebook.presto.hbase;

import com.facebook.presto.hbase.conf.HbaseConfig;
import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.conf.HbaseTableProperties;
import com.facebook.presto.hbase.io.HbasePageSinkProvider;
import com.facebook.presto.hbase.io.HbaseRecordSetProvider;
import com.facebook.presto.hbase.metadata.ZooKeeperMetadataManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
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
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
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

        binder.bind(ZooKeeperMetadataManager.class).in(Scopes.SINGLETON);
        binder.bind(HbaseTableProperties.class).in(Scopes.SINGLETON);
        binder.bind(HbaseSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(HbaseTableManager.class).in(Scopes.SINGLETON);

        binder.bind(Connection.class).toProvider(ConnectionProvider.class).in(Scopes.SINGLETON);
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }

    private static class ConnectionProvider
            implements Provider<Connection>
    {
        private static final Logger LOG = Logger.get(ConnectionProvider.class);
        private final String zooKeepers;

        @Inject
        public ConnectionProvider(HbaseConfig config)
        {
            requireNonNull(config, "config is null");
            this.zooKeepers = config.getZooKeepers();
        }

        @Override
        public Connection get()
        {
            try {
                Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", zooKeepers);

                conf.set("hbase.client.pause", "50");
                conf.set("hbase.client.retries.number", "3");
                conf.set("hbase.rpc.timeout", "2000");
                conf.set("hbase.client.operation.timeout", "3000");
                conf.set("hbase.client.scanner.timeout.period", "10000");

                Connection connection = ConnectionFactory.createConnection(conf);
                LOG.info("Connection to instance %s at %s established, user %s");
                return connection;
            }
            catch (IOException e) {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to get connection to HBASE", e);
            }
        }
    }
}
