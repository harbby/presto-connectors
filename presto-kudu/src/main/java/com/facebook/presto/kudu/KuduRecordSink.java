package com.facebook.presto.kudu;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;

import static com.facebook.presto.kudu.KuduErrorCode.KUDU_ERROR;
import static com.facebook.presto.kudu.KuduErrorCode.KUDU_INSERT_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * sink
 */
public class KuduRecordSink
        implements RecordSink
{
    private final KuduClientManager kuduClientManager;
    private final List<Type> columnTypes;
    private KuduSession kuduSession;
    private KuduTable kuduTable;
    private Insert insert;
    private final KuduClient client;

    private int field = -1;
    private int batchSize = 0;

    public KuduRecordSink(
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
    public void beginRecord()
    {
        this.insert = kuduTable.newInsert();
    }

    @Override
    public void finishRecord()
    {
        try {
            kuduSession.apply(insert);
            batchSize++;

            if (batchSize >= 1000) {
                //kuduSession.setFlushInterval();
                kuduSession.setMutationBufferSpace(1024 * 1024_1024 * 2); //2g
                //kuduSession.setFlushMode();
                kuduSession.flush(); //真正落地
                batchSize = 0;
            }
        }
        catch (KuduException e) {
            throw new PrestoException(KUDU_INSERT_ERROR, e);
        }
    }

    @Override
    public void appendNull()
    {
        ++field;
    }

    @Override
    public void appendBoolean(boolean value)
    {
        insert.getRow().addBoolean(++field, value);
    }

    @Override
    public void appendLong(long value)
    {
        insert.getRow().addLong(++field, value);
    }

    @Override
    public void appendDouble(double value)
    {
        insert.getRow().addDouble(++field, value);
    }

    @Override
    public void appendBigDecimal(BigDecimal value)
    {
        insert.getRow().addDouble(++field, value.doubleValue());
    }

    @Override
    public void appendString(byte[] value)
    {
        insert.getRow().addString(++field, new String(value, UTF_8));
    }

    @Override
    public void appendObject(Object value)
    {
        insert.getRow().addString(++field, value.toString());
    }

    @Override
    public Collection<Slice> commit()
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

        checkState(!(field == -1), "record not finished");
        // the committer does not need any additional info
        return ImmutableList.of();
    }

    @Override
    public void rollback()
    {
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < columnTypes.size(), "all fields already set");
        ++field;
    }
}
