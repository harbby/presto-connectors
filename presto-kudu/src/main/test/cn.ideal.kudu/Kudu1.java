package cn.ideal.kudu;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;

public class Kudu1 {

    private static final Logger LOG = LoggerFactory.getLogger(Kudu1.class);
    static String tableName = "n603_c_1";
    static KuduClient client = new KuduClient.KuduClientBuilder(Arrays.asList("localhost")).build();

    @Test
    public void createTable() throws KuduException
    {
        //------------createTable-----
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
            .key(true)  //主键字段必须要放到前面
            .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
            .build());
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("key");

        Schema schema = new Schema(columns);
        client.createTable("test1", schema,
            new CreateTableOptions()
//                    .addRangePartition(new PartialRow())
//                    .addHashPartitions()
                .setRangePartitionColumns(rangeKeys)
        );
    }

    @Test
    public void drop() throws KuduException
    {
        //checkState(false, "record not finished");
        client.deleteTable("iris");
    }

    @Test
    public void getClonums() throws KuduException
    {
        Schema schema = client.openTable("n603_c_1").getSchema();
        client.openTable("n603_c_1").getPartitionSchema().getRangeSchema();
        schema.getColumns().stream().map(x->x.isKey());
    }

    @Test
    public void select() throws KuduException
    {
        KuduTable table = client.openTable("iris");
        List<String> projectColumns = Arrays.asList("name","day");


        KuduScanner scanner = client.newScannerBuilder(table)
            .setProjectedColumnNames(projectColumns)
            .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.print(result.getString(0));
                System.out.print("  ");
                System.out.print(result.getLong(1));
                System.out.println();
            }
        }
    }

    @Test
    public void scan() throws Exception {
        int kuduTokenId = 0;
        KuduTable table = client.openTable("iris");
        List<String> projectColumns = Arrays.asList("name","day");

        KuduScanToken.KuduScanTokenBuilder kuduScanTokenBuilder =
            client.newScanTokenBuilder(table);

        List<KuduScanToken> tokends = kuduScanTokenBuilder
            .setProjectedColumnNames(projectColumns)
            .build();  //--这里存在bug-

        KuduScanner kuduScanner = tokends.get(kuduTokenId).intoScanner(client);
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator results = kuduScanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                System.out.print(result.getString(0));
                System.out.print("  ");
                System.out.print(result.getLong(1));
                System.out.println();
            }
        }
    }


    @Test
    public void test1() throws KuduException
    {
        boolean tableExists= client.tableExists("test1");
        Assert.assertEquals(true,tableExists);
    }


    @Test
    public void main()
    {

        try {
            //--------------insert into----------------------
            KuduTable table = client.openTable("test1");
            KuduSession session = client.newSession();
            for (int i = 0; i < 3; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "value " + i);
                session.apply(insert);
            }

            List<String> projectColumns = new ArrayList<>(1);
            projectColumns.add("value");
            KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(result.getString(0));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                //client.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static InetAddress getInetAddress(String host)
    {
        long start = System.nanoTime();

        try {
            InetAddress ip = InetAddress.getByName(host);
            long latency = System.nanoTime() - start;
            if (latency > 500000L && LOG.isDebugEnabled()) {
                LOG.debug("Resolved IP of `{}' to {} in {}ns", new Object[]{host, ip, latency});
            } else if (latency >= 3000000L) {
                LOG.warn("Slow DNS lookup! Resolved IP of `{}' to {} in {}ns", new Object[]{host, ip, latency});
            }

            return ip;
        } catch (UnknownHostException var6) {
            var6.printStackTrace();
            LOG.error("Failed to resolve the IP of `{}' in {}ns", host, System.nanoTime() - start);
            return null;
        }
    }

    @After
    public void close() throws KuduException
    {
        client.close();
    }
}
