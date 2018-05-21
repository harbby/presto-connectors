package com.facebook.presto.hbase.conf;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class HbaseConfig
{
    private String hbaseMaster;
    private String zooKeepers;
    private String zkMetadataRoot = "/presto-hbase";

    public String getHbaseMaster()
    {
        return hbaseMaster;
    }

    @Config("hbase.hosts")
    @ConfigDescription("IP:PORT where hbase master connect")
    public HbaseConfig setHbaseMaster(String hbaseMaster)
    {
        this.hbaseMaster = hbaseMaster;
        return this;
    }

    @NotNull
    public String getZooKeepers()
    {
        return this.zooKeepers;
    }

    @Config("hbase.zookeepers")
    @ConfigDescription("ZooKeeper quorum connect string for Hbase")
    public HbaseConfig setZooKeepers(String zooKeepers)
    {
        this.zooKeepers = zooKeepers;
        return this;
    }

    @NotNull
    public String getZkMetadataRoot()
    {
        return zkMetadataRoot;
    }

    @Config("hbase.zookeeper.metadata.root")
    @ConfigDescription("Sets the root znode for metadata storage")
    public void setZkMetadataRoot(String zkMetadataRoot)
    {
        this.zkMetadataRoot = zkMetadataRoot;
    }
}
