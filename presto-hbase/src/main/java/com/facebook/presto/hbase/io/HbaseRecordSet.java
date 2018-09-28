package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.conf.HbaseSessionProperties;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.hbase.serializers.HbaseRowSerializerUtil.toHbaseBytes;
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

        // Save off the column handles and createa list of the Hbase types
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HbaseColumnHandle column : columnHandles) {
            types.add(column.getType());
        }
        this.columnTypes = types.build();

        // Create the BatchScanner and set the ranges from the split
        try (Table htable = connection.getTable(TableName.valueOf(split.getSchema(), split.getTable()))) {
            Optional<Range> range = split.getRanges().stream().findAny();
            Scan scan = range.isPresent() ?
                    getScanFromPrestoRange(range.get())
                    : new Scan();

            scan.setMaxVersions(HbaseSessionProperties.getScanMaxVersions(session)); //默认值为1 只返回最新的
            //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
            scan.setBatch(HbaseSessionProperties.getScanBatchSize(session)); //一次最多返回得列数, 如果列数超过该值会被 拆分成多列
            scan.setCaching(HbaseSessionProperties.getScanBatchCaching(session));
            scan.setMaxResultSize(HbaseSessionProperties.getScanMaxResultSize(session)); //最多返回1w条

            columnHandles.forEach(column -> {
                column.getFamily().ifPresent(x -> scan.addColumn(Bytes.toBytes(x), Bytes.toBytes(column.getQualifier().get())));
            });

            //--------- set Filters -----
            //scan.setFilter(new PageFilter(2));   //limit 2 只获取一个1 rowkey
//            FilterList filterList = new FilterList();
//            filterList.
//            scan.setFilter(filterList);

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

    private static Scan getScanFromPrestoRange(Range prestoRange)
            throws TableNotFoundException
    {
        Scan hbaseScan = new Scan();
        if (prestoRange.isAll()) { //全表扫描  all rowkey
        }
        else if (prestoRange.isSingleValue()) {
            //直接get即可
            Type type = prestoRange.getType();
            Object value = prestoRange.getSingleValue();
            hbaseScan.setStartRow(toHbaseBytes(type, value));
            hbaseScan.setStopRow(toHbaseBytes(type, value));
        }
        else {
            if (prestoRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking inclusivity
                Type type = prestoRange.getType();
                Object value = prestoRange.getHigh().getValue();
                hbaseScan.setStopRow(toHbaseBytes(type, value));
            }
            else if (prestoRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking inclusivity
                Type type = prestoRange.getType();
                Object value = prestoRange.getLow().getValue();
                hbaseScan.setStartRow(toHbaseBytes(type, value));
            }
            else {
                // If high is unbounded, then create a range from low to high, checking inclusivity
                Type type = prestoRange.getType();
                Object startSplit = prestoRange.getLow().getValue();
                Object endSplit = prestoRange.getHigh().getValue();
                //------- set start and stop -----
                hbaseScan.setStartRow(toHbaseBytes(type, startSplit));
                hbaseScan.setStopRow(toHbaseBytes(type, endSplit));
            }
        }

        return hbaseScan;
    }
}
