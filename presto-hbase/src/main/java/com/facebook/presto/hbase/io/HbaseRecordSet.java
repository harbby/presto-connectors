package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HbaseRecordSet
        implements RecordSet
{
    private final List<HbaseColumnHandle> columnHandles;
    private final List<HbaseColumnConstraint> constraints;
    private final List<Type> columnTypes;
    private final ResultScanner resultScanner;
    private final String rowIdName;

    public HbaseRecordSet(
            Connection connection,
            ConnectorSession session,
            HbaseSplit split,
            List<HbaseColumnHandle> columnHandles)
    {
        requireNonNull(session, "session is null");
        requireNonNull(split, "split is null");
        constraints = requireNonNull(split.getConstraints(), "constraints is null");

        rowIdName = split.getRowId();

        // Save off the column handles and createa list of the Accumulo types
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HbaseColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        // Create the BatchScanner and set the ranges from the split
        try (Table htable = connection.getTable(TableName.valueOf(split.getSchema(), split.getTable()))) {
            Scan scan = new Scan();
            scan.setMaxVersions();
            //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
            scan.setBatch(10);

            columnHandles.forEach(column -> {
                column.getFamily().ifPresent(x->scan.addColumn(Bytes.toBytes(x), Bytes.toBytes(column.getQualifier().get())));
            });

            //scan.setFilter(new PageFilter(2));   //limit 2 只获取一个1 rowkey
            //scan.addFamily(Bytes.toBytes("f1"))   //选择列族    注意选择之间是 或的关系 需要几列 就写几列
            //scan.addColumn(Bytes.toBytes("account"), Bytes.toBytes("name"));  //选择 列

            //--set range --
//            scan.setStartRow();
//            scan.setStopRow();

            // set attrs...
            this.resultScanner = htable.getScanner(scan);
        }
        catch (Exception e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, format("Failed to create batch scan for table %s", split.getFullTableName()), e);
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HbaseRecordCursor(resultScanner, rowIdName, columnHandles, constraints);
    }
}
