package com.facebook.presto.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import javax.management.ObjectName;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public class EmbeddedKudu
{
    private static Logger log = Logger.get(EmbeddedKudu.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7051;

    private static KuduClient session;
    private static boolean initialized;

    private EmbeddedKudu() {}

    public static synchronized void start()
            throws Exception
    {
        if (initialized) {
            return;
        }

        log.info("Starting kudu...");

        System.setProperty("kudu.master", "localhost:7051");

        KuduClient client = new KuduClient.KuduClientBuilder("localhost").build();

        EmbeddedKudu.session = client;
        initialized = true;
    }

    public static synchronized  EmbeddedKudu createEmbeddedKudu(){
        return new EmbeddedKudu();
    }




    public static synchronized KuduClient getSession()
    {
        checkIsInitialized();
        return requireNonNull(session, "cluster is null");
    }

    public static synchronized void close()
            throws KuduException
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
