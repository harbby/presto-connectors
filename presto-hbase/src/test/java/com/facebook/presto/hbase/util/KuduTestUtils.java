package com.facebook.presto.hbase.util;

import com.facebook.presto.hbase.EmbeddedHbase;
import com.facebook.presto.hbase.HbasePlugin;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class KuduTestUtils
{
    public static void installRedisPlugin(EmbeddedHbase embeddedHbase, QueryRunner queryRunner)
    {
        HbasePlugin kuduPlugin = new HbasePlugin();
        //kuduPlugin.setTableDescriptionSupplier(() -> tableDescriptions);
        queryRunner.installPlugin(kuduPlugin);

        Map<String, String> config = ImmutableMap.of(
              //  "hbase.master", embeddedHbase.getHost() + ":" + embeddedHbase.getPort()
                );
        queryRunner.createCatalog("hbase", "hbase", config);
    }
}
