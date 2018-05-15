package com.facebook.presto.hbase.conf;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class HbaseConfig
{
    private String kuduMaster;

    public String getKuduMaster()
    {
        return kuduMaster;
    }

    @Config("hbase.hosts")
    @ConfigDescription("IP:PORT where hbase master connect")
    public HbaseConfig setKuduMaster(String kuduMaster)
    {
        this.kuduMaster = kuduMaster;
        return this;
    }
}
