package com.facebook.presto.kudu.util;

import com.facebook.presto.kudu.EmbeddedKudu;
import com.facebook.presto.kudu.KuduPlugin;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class KuduTestUtils
{
    public static void installRedisPlugin(EmbeddedKudu embeddedKudu, QueryRunner queryRunner)
    {
        KuduPlugin kuduPlugin = new KuduPlugin();
        //kuduPlugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(kuduPlugin);

        Map<String, String> redisConfig = ImmutableMap.of(
                "kudu.master", embeddedKudu.getHost() + ":" + embeddedKudu.getPort()
                );
        queryRunner.createCatalog("kudu", "kudu", redisConfig);
    }
}
