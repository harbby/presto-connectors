package com.facebook.presto.kudu.util;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class KuduUtil
{
    private KuduUtil()
    {
    }

    public static void createTable(final ConnectorTableMetadata tableMetadata, final String tableName,
            final KuduClient kuduClient)
            throws KuduException
    {
        //建表的时候 主键字段必须放到前面

        List<ColumnSchema> columns = new ArrayList<>();
        List<ColumnSchema> rangeKeys = new ArrayList<>();

        for (final ColumnMetadata field : tableMetadata.getColumns()) {
            final Type type = field.getType();
            String comment = field.getComment();
            if (comment == null) {
                comment = "";
            }
            else {
                comment = field.getComment().toLowerCase();
            }
            ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(
                    field.getName().toLowerCase(),
                    getKuduColumn(type.getJavaType()));

            if (comment.contains("partition")) {  //分区字段一定要是主键
                builder.key(true).nullable(false);
                ColumnSchema columnSchema = builder.build();
                columns.add(columnSchema);
                rangeKeys.add(columnSchema);
            }
            else if (comment.contains("primary key")) { //主键字段
                builder.key(true).nullable(false);
                columns.add(builder.build());
            }
            else {
                builder.key(false).nullable(true);
                columns.add(builder.build());
            }
        }
        //tableMetadata.getComment() 获取表信息
        CreateTableOptions options = partitionParser(tableMetadata.getComment().orElse(""), rangeKeys);
        System.exit(0);
        kuduClient.createTable(tableName, new Schema(columns), options);
    }

    private static CreateTableOptions partitionParser(String partitionComment, List<ColumnSchema> rangeKeys)
    {
        short deep = 0;  //表示进入() 的深度   '('深度加1 ')'深度减1

        ImmutableSet.Builder<String> partitions = ImmutableSet.builder();
        StringBuilder tmp = new StringBuilder();
        for (char i : partitionComment.toCharArray()) {
            if (i == '(') {
                deep += 1;
            }
            else if (i == ')') {
                deep -= 1;
            }
            else if (i == ',' && deep == 0) {
                partitions.add(tmp.toString());
                tmp = new StringBuilder();
                continue;
            }
            tmp.append(i);
        }
        partitions.add(tmp.toString());
//        partitions.build().forEach(x -> {
//            System.out.println(x);
//        });

        PartialRow partialRow = new PartialRow(new Schema(rangeKeys));
        partialRow.addInt(0, 20171011);
        partialRow.addInt(20171012, 1);

        return new CreateTableOptions().addRangePartition(partialRow, partialRow)
                //.addRangePartition(partialRow, partialRow)
                .setRangePartitionColumns(rangeKeys.stream().map(x -> x.getName())
                        .collect(Collectors.toList()));
    }

    private static org.apache.kudu.Type getKuduColumn(Class<?> dataType)
    {
        if (dataType == String.class) {  //string
            return org.apache.kudu.Type.STRING;
        }
        else if (dataType == byte.class) {
            return org.apache.kudu.Type.BINARY;
        }
        else if (dataType == short.class) {
            return org.apache.kudu.Type.INT16;
        }
        else if (dataType == int.class) {
            return org.apache.kudu.Type.INT32;
        }
        else if (dataType == long.class) {
            return org.apache.kudu.Type.INT64;
        }
        else if (dataType == double.class) {
            return org.apache.kudu.Type.DOUBLE;
        }
        else if (dataType == float.class) {
            return org.apache.kudu.Type.FLOAT;
        }
        else if (dataType == Map.class) {
            return org.apache.kudu.Type.STRING;
        }
        else {
            return org.apache.kudu.Type.STRING;
        }
    }
}
