package com.facebook.presto.kudu;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

public class TestKuduPlugin
{

    @Test
    public void testStartup()
    {
        KuduPlugin plugin = new KuduPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, KuduConnectorFactory.class);

        Connector c = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("kudu.master", "slave05:7051")
                        .build(),
                new TestingConnectorContext());
        assertNotNull(c);
    }
}
