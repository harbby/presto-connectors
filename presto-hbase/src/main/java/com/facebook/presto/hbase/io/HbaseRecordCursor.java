package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_CLOSE_ERR;
import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_DNE;
import static com.facebook.presto.hbase.HbaseErrorCode.IO_ERROR;
import static java.util.Objects.requireNonNull;

public class HbaseRecordCursor
        implements RecordCursor
{
    private final Connection hbaseClient;
    private final String tableName;
    private final HbaseSplit hbaseSplit;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    private final Table htable;
    private final ResultScanner resultScanner;
    private Result result;

    public HbaseRecordCursor(
            Connection hbaseClient,
            String tableName,
            HbaseSplit hbaseSplit,
            List<String> columnNames,
            List<Type> columnTypes
    )
    {
        this.hbaseClient = requireNonNull(hbaseClient, "hbaseClient is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.hbaseSplit = requireNonNull(hbaseSplit, "hbaseSplit is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");

        try {
            this.htable = hbaseClient.getTable(TableName.valueOf("students"));

            Scan scan = new Scan();
            scan.setMaxVersions();
            //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
            scan.setBatch(10);

            //scan.setFilter(new PageFilter(2));   //limit 2 只获取一个1 rowkey

            //scan.addFamily(Bytes.toBytes("f1"))   //选择列族    注意选择之间是 或的关系 需要几列 就写几列
            //scan.addColumn(Bytes.toBytes("account"), Bytes.toBytes("name"));  //选择 列
            //scan.addFamily(Bytes.toBytes("address"));   //选择列族

            // set attrs...
            this.resultScanner = htable.getScanner(scan);
        }
        catch (IOException e) {
            throw new PrestoException(HBASE_TABLE_DNE, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (resultScanner != null) {

                this.result = resultScanner.next();

                if (result != null) { return true; }
            }
        }
        catch (IOException e) {
            throw new PrestoException(IO_ERROR, "Caught IO error from resultScanner on read", e);
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        return null;
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        if (resultScanner != null) {resultScanner.close();}

        try {
            if (htable != null) { htable.close(); }
        }
        catch (IOException e) {
            throw new PrestoException(HBASE_TABLE_CLOSE_ERR, e);
        }
    }
}