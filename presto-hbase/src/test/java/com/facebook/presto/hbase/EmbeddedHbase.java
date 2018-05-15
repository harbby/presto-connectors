package com.facebook.presto.hbase;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.management.ObjectName;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EmbeddedHbase
{
    private static Logger log = Logger.get(EmbeddedHbase.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7051;

    private static Connection session;
    private static boolean initialized;

    private EmbeddedHbase() {}

    public static synchronized void start()
            throws Exception
    {
        if (initialized) {
            return;
        }

        log.info("Starting Hbase...");

        Configuration conf = HBaseConfiguration.create();
        Connection client = ConnectionFactory.createConnection(conf);;

        EmbeddedHbase.session = client;
        initialized = true;
    }

    public static synchronized  EmbeddedHbase createEmbeddedHbase(){
        return new EmbeddedHbase();
    }




    public static synchronized Connection getSession()
    {
        checkIsInitialized();
        return requireNonNull(session, "cluster is null");
    }

    public static synchronized void close()
            throws IOException
    {
        checkIsInitialized();
        session.close();
    }

    public static synchronized String getHost()
    {
        checkIsInitialized();
        return HOST;
    }

    public static synchronized int getPort()
    {
        checkIsInitialized();
        return PORT;
    }

    private static void checkIsInitialized()
    {
        checkState(initialized, "EmbeddedCassandra must be started with #start() method before retrieving the cluster retrieval");
    }

    private static void refreshSizeEstimates()
            throws Exception
    {
        ManagementFactory
                .getPlatformMBeanServer()
                .invoke(
                        new ObjectName("org.apache.cassandra.db:type=StorageService"),
                        "refreshSizeEstimates",
                        new Object[] {},
                        new String[] {});
    }
}
