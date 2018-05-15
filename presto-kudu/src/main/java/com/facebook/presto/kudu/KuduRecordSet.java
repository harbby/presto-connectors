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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class KuduRecordSet
        implements RecordSet
{
    public static final Logger logger = Logger.get(KuduRecordSet.class);

    private final List<KuduColumnHandle> columns;
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    //    private final HostAddress address;
    private final TupleDomain<KuduColumnHandle> effectivePredicate;
    private final SchemaTableName tableName;
    private final KuduTableManager kuduTableManager;
    private final KuduSplit kuduSplit;
    private final KuduClientManager kuduClientManager;

    public KuduRecordSet(KuduTableManager kuduTableManager, KuduClientManager kuduClientManager, KuduSplit split, List<KuduColumnHandle> columns)
    {
        //将要查询的kudu列
        this.columns = requireNonNull(columns, "column handles is null");
        this.columnNames = columns.stream().map(kuduColumn -> kuduColumn.getColumnName()).collect(Collectors.toList());

        requireNonNull(split, "split is null");

        //将要查询的kudu列的数据类型
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KuduColumnHandle column : columns) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

//        this.address = Iterables.getOnlyElement(split.getAddresses());
        this.effectivePredicate = split.getEffectivePredicate();
        this.tableName = split.getTableName();

        this.kuduTableManager = requireNonNull(kuduTableManager, "kuduTables is null");
        this.kuduSplit = requireNonNull(split, "kuduTables is null");

        this.kuduClientManager = requireNonNull(kuduClientManager, "kuduClientManager is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KuduRecordCursor(kuduClientManager, kuduSplit.getKuduTokenId(), columns, tableName, effectivePredicate);
    }
}
