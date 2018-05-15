/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.kudu;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.kudu.KuduErrorCode.KUDU_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KuduRecordCursor
        implements RecordCursor
{
    public static final Logger logger = Logger.get(KuduRecordCursor.class);

    // TODO This should be a config option as it may be different for different log files
    private final int[] fieldToColumnIndex;
    private final List<KuduColumnHandle> columns;

    private KuduScanner kuduScanner;
    private RowResult result;
    private RowResultIterator results;
    private final KuduClientManager kuduClientManager;
    private final KuduClient kuduClient;

    public KuduRecordCursor(KuduClientManager kuduClientManager, int kuduTokenId, List<KuduColumnHandle> columns, SchemaTableName tableName, TupleDomain<KuduColumnHandle> predicate)

    {
        this.kuduClientManager = requireNonNull(kuduClientManager, "kuduClientManager is null");
        this.columns = requireNonNull(columns, "columns is null");

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            KuduColumnHandle columnHandle = columns.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        this.kuduClient = requireNonNull(kuduClientManager.getClient(), "kuduClient is null");

        try {
            List<KuduScanToken> tokends = kuduClientManager
                    .newScanTokenBuilder(this.kuduClient, tableName.getTableName())
                    .setProjectedColumnNames(columns.stream().map(column->column.getColumnName()).collect(Collectors.toList()))
                    .setTimeout(1000)
                    .build();  //--这里存在bug-

            this.kuduScanner = tokends.get(kuduTokenId).intoScanner(this.kuduClient);
        }
        catch (Exception e) {
            throw new PrestoException(KUDU_ERROR, e);
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
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            if (results != null && results.hasNext()) {
                result = results.next();
                return true;
            }
            else {
                if (kuduScanner.hasMoreRows()) {
                    results = kuduScanner.nextRows();
                    if (results.hasNext()) {
                        result = results.next();
                        return true;
                    }
                }
            }
        }
        catch (KuduException e) {
            logger.error(e, e.getMessage());
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return result.getBoolean(field);
    }

    @Override
    public long getLong(int field)
    {
        switch (result.getColumnType(field)) {
            case INT8:
                return result.getInt(field);
            case INT16:
                return result.getInt(field);
            case INT32:
                return result.getInt(field);
            case INT64:
                return result.getLong(field);
            default:
                throw new IllegalStateException("Cannot retrieve long for " + result.getColumnType(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        switch (result.getColumnType(field)) {
            case DOUBLE:
                return result.getDouble(field);
            case FLOAT:
                return result.getFloat(field);
            default:
                throw new IllegalStateException("Cannot retrieve double for " + result.getColumnType(field));
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        return Slices.utf8Slice(result.getString(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return result.isNull(field);
    }

    @Override
    public void close()
    {
        // TODO: 18/04/2017 如果有资源申请,在这个地方需要关闭
        try {
            if (kuduScanner != null) {
                kuduScanner.close();
            }
            kuduClientManager.close(this.kuduClient);
        }
        catch (KuduException e) {
            logger.error(e, e.getMessage());
        }
    }
}
