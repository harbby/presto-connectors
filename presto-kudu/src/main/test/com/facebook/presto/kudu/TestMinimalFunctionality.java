package com.facebook.presto.kudu;

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
import java.util.UUID;

import static com.facebook.presto.kudu.util.KuduTestUtils.installRedisPlugin;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("kudu")
            .setSchema("default")
            .build();

    private EmbeddedKudu embeddedRedis;
    private String tableName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startRedis()
            throws Exception
    {
        embeddedRedis = EmbeddedKudu.createEmbeddedKudu();
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
        QualifiedObjectName name = new QualifiedObjectName("kudu", "default", tableName);
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
    public void testSelect()
    {
        MaterializedResult result = queryRunner.execute("SELECT * from iris");

        result.getMaterializedRows().stream().forEach(
                x -> x.getFields().forEach(y -> {
                    System.out.println(y);
                })
        );
    }

    @Test
    public void testDrop()
    {
        MaterializedResult result = queryRunner.execute("drop table " + tableName);

        result.getMaterializedRows().stream().forEach(
                x -> x.getFields().forEach(y -> {
                    System.out.println(y);
                })
        );
    }

    @Test
    public void testInsert()
    {
        String sql ="insert into iris values(20171118,'hp','pwd',18)";
        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(
                x -> x.getFields().forEach(y -> {
                    System.out.println(y);
                })
        );
    }

    @Test
    public void testDelete()
    {
        String sql ="delete from iris where name='hp'";
        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(
                x -> x.getFields().forEach(y -> {
                    System.out.println(y);
                })
        );
    }

    @Test
    public void testCreateTable()
    {
        String sql ="create table iris2 (\n" +
                "day integer COMMENT 'primary key and partition',\n" +
                "name  varchar COMMENT 'primary key',\n" +
                "pwd\t varchar,\n" +
                "age  integer\n" +
                ")\n" +
                "COMMENT '\n" +
                "PARTITION BY \n" +
                "HASH PARTITIONS 16,\n" +
                "HASH(name) PARTITIONS 4,\n" +
                "RANGE (day,hour) (\n" +
                "  PARTITION VALUE = (20171120,'1500'),\n" +
                "  PARTITION VALUE = 20171121,\n" +
                "  PARTITION VALUE = 20171122,\n" +
                "  PARTITION VALUE = 20171123,\n" +
                "  PARTITION VALUE = 20171124\n" +
                ")\n" +
                "'";
        MaterializedResult result = queryRunner.execute(sql);

        result.getMaterializedRows().stream().forEach(
                x -> x.getFields().forEach(y -> {
                    System.out.println(y);
                })
        );
    }
}