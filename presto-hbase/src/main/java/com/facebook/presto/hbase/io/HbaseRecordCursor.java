package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.Types;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hbase.HbaseErrorCode.IO_ERROR;
import static com.facebook.presto.hbase.serializers.HbaseRowSerializerUtil.getBlockFromArray;
import static com.facebook.presto.hbase.serializers.HbaseRowSerializerUtil.getBlockFromMap;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HbaseRecordCursor
        implements RecordCursor
{
    private final String rowIdName;
    private final List<HbaseColumnHandle> columnHandles;
    private final List<HbaseColumnConstraint> constraints;
    private final ResultScanner resultScanner;
    private Result result;

    private long nanoStart;
    private long nanoEnd;

    public HbaseRecordCursor(
            ResultScanner resultScanner,
            String rowIdName,
            List<HbaseColumnHandle> columnHandles,
            List<HbaseColumnConstraint> constraints)
    {
        this.resultScanner = requireNonNull(resultScanner, "resultScanner is null");
        this.rowIdName = requireNonNull(rowIdName, "rowIdName is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.constraints = requireNonNull(constraints, "constraints is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }

        try {
            this.result = resultScanner.next();
            if (result != null) {
                return true;
            }
            else {
                return false;
            }
        }
        catch (IOException e) {
            throw new PrestoException(IO_ERROR, "Caught IO error from resultScanner on read", e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        byte[] bytes = getValue(field);
        return Bytes.toBoolean(bytes);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT, DATE, INTEGER, REAL, SMALLINT, TIME, TIMESTAMP, TINYINT);
        Type type = getType(field);
        byte[] bytes = getValue(field);
        if (type.equals(BIGINT)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(DATE)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(INTEGER)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(REAL)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(SMALLINT)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(TIME)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(TIMESTAMP)) {
            return Bytes.toLong(bytes);
        }
        else if (type.equals(TINYINT)) {
            return Bytes.toLong(bytes);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + getType(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        byte[] bytes = getValue(field);
        return Bytes.toDouble(bytes);
    }

    @Override
    public Slice getSlice(int field)
    {
        byte[] bytes = getValue(field);
        Type type = getType(field);
        if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer(bytes);
        }
        else if (type instanceof VarcharType) {
            return Slices.utf8Slice(new String(bytes, UTF_8));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported type " + type);
        }
    }

    private byte[] getValue(int field)
    {
        HbaseColumnHandle handle = columnHandles.get(field);
        byte[] bytes = handle.getFamily().isPresent() ?
                result.getValue(Bytes.toBytes(handle.getFamily().get()), Bytes.toBytes(handle.getQualifier().get()))
                : result.getRow();
        return bytes;
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type), "Expected field %s to be a type of array or map but is %s", field, type);
        byte[] bytes = getValue(field);

        if (Types.isArrayType(type)) {
            try {
                return getBlockFromArray(type, bytes);
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Unsupported type " + type, e);
            }
        }
        else {
            try {
                return getBlockFromMap(type, bytes);
            }
            catch (IOException e) {
                throw new UnsupportedOperationException("Unsupported type " + type, e);
            }
        }
    }

    @Override
    public boolean isNull(int field)
    {
        HbaseColumnHandle handle = columnHandles.get(field);
        return handle.getFamily().isPresent()
                && result.getValue(
                Bytes.toBytes(handle.getFamily().get()),
                Bytes.toBytes(handle.getQualifier().get())) == null;
    }

    @Override
    public void close()
    {
        if (resultScanner != null) {
            resultScanner.close();
        }
        nanoEnd = System.nanoTime();
    }

    /**
     * Checks that the given field is one of the provided types.
     *
     * @param field Ordinal of the field
     * @param expected An array of expected types
     * @throws IllegalArgumentException If the given field does not match one of the types
     */
    private void checkFieldType(int field, Type... expected)
    {
        Type actual = getType(field);
        for (Type type : expected) {
            if (actual.equals(type)) {
                return;
            }
        }

        throw new IllegalArgumentException(format("Expected field %s to be a type of %s but is %s", field, StringUtils.join(expected, ","), actual));
    }
}
