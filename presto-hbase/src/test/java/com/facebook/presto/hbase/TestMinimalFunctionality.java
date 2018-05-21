package com.facebook.presto.hbase;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hbase.util.KuduTestUtils.installRedisPlugin;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("hbase")
            .setSchema("default")
            .build();

    private EmbeddedHbase embeddedRedis;
    private String tableName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startRedis()
            throws Exception
    {
        embeddedRedis = EmbeddedHbase.createEmbeddedHbase();
        embeddedRedis.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopRedis()
            throws Exception
    {
        embeddedRedis.close();
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.tableName = "test1";

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        installRedisPlugin(embeddedRedis, queryRunner);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
    }

    @Test
    public void testTableExists()
            throws Exception
    {
        QualifiedObjectName name = new QualifiedObjectName("hbase", "default", tableName);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTableHasData()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();

        assertEquals(result, expected);

        int count = 1;
        //populateData(count);

        result = queryRunner.execute("SELECT count(1) from " + tableName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void showTables()
    {
        MaterializedResult result = queryRunner.execute("show tables");

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void testSelect()
    {
        MaterializedResult result = queryRunner.execute("SELECT * from a3");

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void testDrop()
    {
        MaterializedResult result = queryRunner.execute("drop table " + tableName);

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void testInsert()
    {
        String sql = "INSERT INTO a1 VALUES\n" +
                "('row3', 'Grace Hopper', 109, DATE '2017-12-09' ),\n" +
                "('row4', 'Alan Turing', 103, DATE '2017-06-23' )";
        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void testDelete()
    {
        String sql = "delete from iris where name='hp'";
        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void testCreateTable()
    {
        String sql = "CREATE TABLE a1 (\n" +
                "  rowkey VARCHAR,\n" +
                "  name VARCHAR,\n" +
                "  age BIGINT,\n" +
                "  birthday DATE\n" +
                ")\n" +
                "WITH (\n" +
                "  external = true,\n" +
                "  column_mapping = 'name:info:name,age:info:age,birthday:info:date'\n" +
                ")";

        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    public void testCreateTable2()
    {
        String sql = "create table a3 as select 1 as age";

        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }

    @Test
    public void descTable()
    {
        MaterializedResult result = queryRunner.execute("drop table a1");

        result.getMaterializedRows().stream().forEach(x ->
                x.getFields().forEach(y -> System.out.println(y)));
    }
}
