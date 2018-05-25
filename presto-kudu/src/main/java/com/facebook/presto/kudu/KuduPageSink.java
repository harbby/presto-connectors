package com.facebook.presto.kudu;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.kudu.KuduErrorCode.KUDU_ERROR;
import static com.facebook.presto.kudu.KuduErrorCode.KUDU_INSERT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.readBigDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KuduPageSink
        implements ConnectorPageSink
{
    private final KuduClientManager kuduClientManager;
    private final List<Type> columnTypes;
    private KuduSession kuduSession;
    private KuduTable kuduTable;
    private Insert insert;
    private final KuduClient client;

    private int batchSize;
    private static final int MAX_PUT_NUM = 10_000;

    KuduPageSink(
            KuduClientManager kuduClientManager,
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            boolean generateUUID)
    {
        this.kuduClientManager = requireNonNull(kuduClientManager, "kudu");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));

        this.client = kuduClientManager.getClient();
        this.kuduSession = kuduClientManager.getClient().newSession();
        try {
            this.kuduTable = client.openTable(tableName);
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_ERROR, e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // For each position within the page, i.e. row
        try {
            for (int position = 0; position < page.getPositionCount(); ++position) {
                Insert insert = kuduTable.newInsert();
                // For each channel within the page, i.e. column
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                // Convert row to a Mutation, writing and indexing it
                kuduSession.apply(insert);
                ++batchSize;

                // TODO Fix arbitrary flush every 10k rows
                if (batchSize >= MAX_PUT_NUM) {
                    flush();
                }
            }
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_INSERT_ERROR, "puts rejected by server on kuduSession.apply(insert)", e);
        }

        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        int parameter = channel + 1;

        if (block.isNull(position)) {
            insert.getRow().setNull(parameter);
            return;
        }

        Type type = columnTypes.get(channel);
        if (BOOLEAN.equals(type)) {
            insert.getRow().addBoolean(parameter, type.getBoolean(block, position));
        }
        else if (BIGINT.equals(type)) {
            insert.getRow().addLong(parameter, type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            insert.getRow().addInt(parameter, toIntExact(type.getLong(block, position)));
        }
        else if (SMALLINT.equals(type)) {
            insert.getRow().addShort(parameter, Shorts.checkedCast(type.getLong(block, position)));
        }
        else if (TINYINT.equals(type)) {
            insert.getRow().addByte(parameter, SignedBytes.checkedCast(type.getLong(block, position)));
        }
        else if (DOUBLE.equals(type)) {
            insert.getRow().addDouble(parameter, type.getDouble(block, position));
        }
        else if (REAL.equals(type)) {
            insert.getRow().addFloat(parameter, intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (type instanceof DecimalType) {
            insert.getRow().addDouble(parameter, readBigDecimal((DecimalType) type, block, position).doubleValue());
        }
        else if (isVarcharType(type) || isCharType(type)) {
            insert.getRow().addString(parameter, type.getSlice(block, position).toStringUtf8());
        }
        else if (VARBINARY.equals(type)) {
            insert.getRow().addBinary(parameter, type.getSlice(block, position).getBytes());
        }
        else if (DATE.equals(type)) {
            insert.getRow().addLong(parameter, type.getLong(block, position));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    private void flush()
    {
        try {
            //kuduSession.setFlushInterval();
            kuduSession.setMutationBufferSpace(1024 * 1024_1024 * 2); //2g
            //kuduSession.setFlushMode();
            kuduSession.flush(); //真正落地
            this.insert = kuduTable.newInsert();
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_INSERT_ERROR, "puts rejected by server on flush", e);
        }
        finally {
            batchSize = 0;
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        try {
            kuduSession.flush(); //真正落地
            kuduSession.close();
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_INSERT_ERROR, e);
        }
        finally {
            kuduClientManager.close(client);
        }

        // TODO Look into any use of the metadata for writing out the rows
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
        finish();
    }
}
