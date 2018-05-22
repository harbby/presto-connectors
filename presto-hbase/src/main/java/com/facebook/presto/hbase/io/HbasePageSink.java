package com.facebook.presto.hbase.io;

import com.facebook.presto.hbase.Types;
import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeUtils;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_DNE;
import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HbasePageSink
        implements ConnectorPageSink
{
    public static final Text ROW_ID_COLUMN = new Text("___ROW___");
    public static final int MAX_PUT_NUM = 10_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<Put> puts;
    private Table htable = null;
    private long numRows;
    private final List<HbaseColumnHandle> columns;
    private final int rowIdOrdinal;

    public HbasePageSink(
            Connection connection,
            HbaseTable table)
    {
        requireNonNull(table, "table is null");
        this.columns = table.getColumns();

        // Fetch the row ID ordinal, throwing an exception if not found for safety
        Optional<Integer> ordinal = columns.stream()
                .filter(columnHandle -> columnHandle.getName().equals(table.getRowId()))
                .map(HbaseColumnHandle::getOrdinal)
                .findAny();

        if (!ordinal.isPresent()) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Row ID ordinal not found");
        }
        this.rowIdOrdinal = ordinal.get();
        this.puts = new ArrayList<>(MAX_PUT_NUM);
        try {
            this.htable = connection.getTable(TableName.valueOf(table.getSchema(), table.getTable()));
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(HBASE_TABLE_DNE, "Hbase error when getting htable and/or Indexer, table does not exist", e);
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Hbase error when getting htable and/or Indexer", e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // For each position within the page, i.e. row
        for (int position = 0; position < page.getPositionCount(); ++position) {
            Type rowkeyType = columns.get(rowIdOrdinal).getType();
            Object rowKey = TypeUtils.readNativeValue(rowkeyType, page.getBlock(rowIdOrdinal), position);
            if (rowKey == null) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Column mapped as the Hbase row ID cannot be null");
            }
            Put put = new Put(toHbaseBytes(rowkeyType, rowKey));

            // For each channel within the page, i.e. column
            for (HbaseColumnHandle column : columns) {
                // Skip the row ID ordinal
                if (column.getOrdinal() == rowIdOrdinal) {
                    continue;
                }
                // Get the type for this channel
                int channel = column.getOrdinal();

                // Read the value from the page and append the field to the row
                Object value = TypeUtils.readNativeValue(column.getType(), page.getBlock(channel), position);
                put.addColumn(
                        Bytes.toBytes(column.getFamily().get()),
                        Bytes.toBytes(column.getQualifier().get()),
                        toHbaseBytes(column.getType(), value));
            }

            // Convert row to a Mutation, writing and indexing it
            puts.add(put);
            ++numRows;

            // TODO Fix arbitrary flush every 10k rows
            if (numRows % MAX_PUT_NUM == 0) {
                flush();
            }
        }

        return NOT_BLOCKED;
    }

    private static byte[] toHbaseBytes(Type type, @Nonnull Object value)
    {
        Object toEncode;
        if (Types.isArrayType(type)) {
            toEncode = getArrayFromBlock(Types.getElementType(type), (Block) value);
            try {
                return MAPPER.writeValueAsBytes(toEncode);
            }
            catch (JsonProcessingException e) {
                throw new UnsupportedOperationException("Unsupported type " + type, e);
            }
        }
        else if (Types.isMapType(type)) {
            toEncode = getMapFromBlock(type, (Block) value);
            try {
                return MAPPER.writeValueAsBytes(toEncode);
            }
            catch (JsonProcessingException e) {
                throw new UnsupportedOperationException("Unsupported type " + type, e);
            }
        }
        else if (type.equals(BIGINT)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(DATE)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(INTEGER)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(REAL)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(SMALLINT)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(TIME)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(TIMESTAMP)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(TINYINT)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(BOOLEAN)) {
            return Bytes.toBytes((Boolean) value);
        }
        else if (type.equals(DOUBLE)) {
            return Bytes.toBytes((Double) value);
        }
        else if (type.equals(VARBINARY)) {
            return ((Slice) value).getBytes();
        }
        else if (type instanceof VarcharType) {
            return ((Slice) value).getBytes();
        }
        else {
            throw new UnsupportedOperationException("Unsupported type " + type + " valaueClass " + value.getClass());
        }
    }

    /**
     * Given the array element type and Presto Block, decodes the Block into a list of values.
     *
     * @param elementType Array element type
     * @param block Array block
     * @return List of values
     */
    static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            arrayBuilder.add(readObject(elementType, block, i));
        }
        return arrayBuilder.build();
    }

    /**
     * Given the map type and Presto Block, decodes the Block into a map of values.
     *
     * @param type Map type
     * @param block Map block
     * @return List of values
     */
    static Map<Object, Object> getMapFromBlock(Type type, Block block)
    {
        Map<Object, Object> map = new HashMap<>(block.getPositionCount() / 2);
        Type keyType = Types.getKeyType(type);
        Type valueType = Types.getValueType(type);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put(readObject(keyType, block, i), readObject(valueType, block, i + 1));
        }
        return map;
    }

    /**
     * @param type Presto type
     * @param block Block to decode
     * @param position Position in the block to get
     * @return Java object from the Block
     */
    static Object readObject(Type type, Block block, int position)
    {
        if (Types.isArrayType(type)) {
            Type elementType = Types.getElementType(type);
            return getArrayFromBlock(elementType, block.getObject(position, Block.class));
        }
        else if (Types.isMapType(type)) {
            return getMapFromBlock(type, block.getObject(position, Block.class));
        }
        else {
            if (type.getJavaType() == Slice.class) {
                Slice slice = (Slice) TypeUtils.readNativeValue(type, block, position);
                return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
            }

            return TypeUtils.readNativeValue(type, block, position);
        }
    }

    private void flush()
    {
        try {
            htable.put(puts);
        }
        catch (IOException e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "puts rejected by server on flush", e);
        }
        finally {
            puts.clear();
            numRows = 0;
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (htable != null) {
            try (Table table = htable) {
                table.put(puts);
            }
            catch (IOException e) {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Error when htable closes", e);
            }
        }
        // TODO Look into any use of the metadata for writing out the rows
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        getFutureValue(finish());
    }
}
