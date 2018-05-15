package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.client.Connection;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class HbaseRecordSet
        implements RecordSet
{
    public static final Logger logger = Logger.get(HbaseRecordSet.class);

    private final List<HbaseColumnHandle> columns;
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final String tableName;
    private final HbaseSplit hbaseSplit;
    private final Connection hbaseClient;

    public HbaseRecordSet(
            Connection hbaseClient,
            ConnectorSession session,
            HbaseSplit split,
            List<HbaseColumnHandle> columns)
    {
        //将要查询的Hbase列
        this.columns = requireNonNull(columns, "column handles is null");
        this.columnNames = columns.stream().map(hbaseColumn -> hbaseColumn.getColumnName()).collect(Collectors.toList());
        this.hbaseClient = requireNonNull(hbaseClient, "hbaseClient is null");

        requireNonNull(split, "split is null");

        //将要查询的Hbase列的数据类型
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HbaseColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

//        this.address = Iterables.getOnlyElement(split.getAddresses());
        //this.effectivePredicate = split.getEffectivePredicate();
        this.tableName = split.getTableName();

        this.hbaseSplit = requireNonNull(split, "HbaseTables is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HbaseRecordCursor(hbaseClient, tableName, hbaseSplit, columnNames, columnTypes);
    }
}
