package cn.ideal.kudu;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class HbaseTest
{
    private static final Logger LOG = Logger.get(HbaseTest.class);
    static String tableName = "n603_c_1";

    static Configuration conf = HBaseConfiguration.create();
    /* ** 下面是1.1.2 版本推荐的连接池 ***/
    static Connection hbaseClient;

    static {
        try {
            hbaseClient = ConnectionFactory.createConnection(conf);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] agrs)
            throws IOException
    {
        Table htable = hbaseClient.getTable(TableName.valueOf("students"));

        Scan scan = new Scan();
        scan.setMaxVersions();
        //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
        scan.setBatch(10);

        //scan.setFilter(new PageFilter(2));   //limit 2 只获取一个1 rowkey

        //scan.addFamily(Bytes.toBytes("f1"))   //选择列族    注意选择之间是 或的关系 需要几列 就写几列
        scan.addColumn(Bytes.toBytes("account"), Bytes.toBytes("name"));  //选择 列
        scan.addFamily(Bytes.toBytes("address"));   //选择列族

        // set attrs...
        ResultScanner rs = htable.getScanner(scan);

        for (Result r : rs) {
            // process result...
            System.out.println(new String(r.getRow()));
        }

        rs.close(); // always close the ResultScanner!
        htable.close();
        hbaseClient.close();
    }

    @Test
    public void showSchemas()
            throws IOException
    {
        Object a1 = Arrays.stream(hbaseClient.getAdmin().listNamespaceDescriptors())
                .map(x -> x.getName()).collect(Collectors.toList());
        System.out.println(a1);
    }

    @Test
    public void showTables()
            throws IOException
    {
        Object a1 = Arrays.stream(hbaseClient.getAdmin()
                .listTableNamesByNamespace("default")
        ).map(x -> x.getNameAsString()).collect(Collectors.toList());
        System.out.println(a1);

        HTableDescriptor descriptor = hbaseClient.getAdmin().getTableDescriptor(TableName.valueOf("students"));
        Object a2 = descriptor.getColumnFamilies();
        Object a3 = descriptor.getFamilies();
        System.out.println(a3);
    }

    @Test
    public void createTable()
    {
    }

    @Test
    public void drop()
    {
    }

    @Test
    public void getClonums()
    {
    }

    @Test
    public void select()
    {
    }

    @Test
    public void scan()
            throws Exception
    {
    }

    @Test
    public void tableExists()
    {
    }

    public static InetAddress getInetAddress(String host)
    {
        long start = System.nanoTime();

        try {
            InetAddress ip = InetAddress.getByName(host);
            long latency = System.nanoTime() - start;
            if (latency > 500000L && LOG.isDebugEnabled()) {
                LOG.debug("Resolved IP of `{}' to {} in {}ns", new Object[] {host, ip, latency});
            }
            else if (latency >= 3000000L) {
                LOG.warn("Slow DNS lookup! Resolved IP of `{}' to {} in {}ns", new Object[] {host, ip, latency});
            }

            return ip;
        }
        catch (UnknownHostException var6) {
            var6.printStackTrace();
            LOG.error("Failed to resolve the IP of `{}' in {}ns", host, System.nanoTime() - start);
            return null;
        }
    }

    @After
    public void close()
            throws IOException
    {
        hbaseClient.close();
    }
}
