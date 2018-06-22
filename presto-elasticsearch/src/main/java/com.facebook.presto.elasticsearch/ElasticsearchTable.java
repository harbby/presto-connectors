package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.EsIndex;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ElasticsearchTable
{
    private final String schema;
    private final String table;
    private final List<ElasticsearchColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;

    ElasticsearchTable(String schema, String table, final EsIndex esIndex)
    {
        requireNonNull(esIndex, "esIndex is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = table;

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
        columnMetadataBuilder.add(new ColumnMetadata("_dsl", VarcharType.VARCHAR));
        columnMetadataBuilder.add(new ColumnMetadata("_type", VarcharType.VARCHAR));
        columnMetadataBuilder.add(new ColumnMetadata("_id", VarcharType.VARCHAR));
        for (EsField esField : esIndex.mapping().values()) {
            Type type = PrestoTypes.toPrestoType(esField);
            String comment = null;  //字段注释
            columnMetadataBuilder.add(new ColumnMetadata(esField.getName(), type, comment, false));
        }
        this.columnsMetadata = columnMetadataBuilder.build();

        //---------------------------------
        ImmutableList.Builder<ElasticsearchColumnHandle> columnHandleBuilder = ImmutableList.builder();
        columnHandleBuilder.add(new ElasticsearchColumnHandle("_dsl", VarcharType.VARCHAR, "", false));
        columnHandleBuilder.add(new ElasticsearchColumnHandle("_type", VarcharType.VARCHAR, "", false));
        columnHandleBuilder.add(new ElasticsearchColumnHandle("_id", VarcharType.VARCHAR, "", false));
        for (EsField esField : esIndex.mapping().values()) {
            Type type = PrestoTypes.toPrestoType(esField);
            String comment = "";  //字段注释
            ElasticsearchColumnHandle columnHandle = new ElasticsearchColumnHandle(
                    esField.getName(),
                    type,
                    comment,
                    true);
            columnHandleBuilder.add(columnHandle);
        }
        this.columns = columnHandleBuilder.build();
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return this.columnsMetadata;
    }

    public List<ElasticsearchColumnHandle> getColumns()
    {
        return columns;
    }
}
