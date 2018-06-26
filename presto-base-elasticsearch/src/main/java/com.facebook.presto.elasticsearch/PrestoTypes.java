package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.UnsupportedEsField;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public final class PrestoTypes
{
    private PrestoTypes() {}

    public static Type toPrestoType(TypeManager typeManager, EsField esField)
    {
        switch (esField.getDataType()) {
            case NULL:
            case OBJECT:
                if (esField.hasDocValues()) {
                    RowType rowType = RowType.from(esField.getProperties().values()
                            .stream().map(x -> RowType.field(x.getName(), toPrestoType(typeManager, x))).collect(Collectors.toList()));
                    return rowType;
                }
                return null;
            case UNSUPPORTED: {
                String stringType = ((UnsupportedEsField) esField).getOriginalType();
                if ("string".equals(stringType)) {
                    return VARCHAR;
                }
                if ("ip".equals(stringType)) {
                    return VARCHAR;
                }
                if ("geo_point".equals(stringType)) {  //地理位置类型 经纬度值
                    return mapType(typeManager, VARCHAR, DOUBLE);
                }
                return VARCHAR;
            }
            case BOOLEAN:
                return BOOLEAN;
            case SHORT:
                return SMALLINT;
            case LONG:
                return BIGINT;
            case INTEGER:
                return INTEGER;
            case FLOAT:
                return REAL;
            case SCALED_FLOAT:
            case HALF_FLOAT:
            case DOUBLE:
                return DOUBLE;
            case TEXT:
            case KEYWORD:
                return VARCHAR;
            case BINARY:
                return VARBINARY;
            case DATE:
                return DATE;
        }
        return null;
    }

    public static MapType mapType(TypeManager typeManager, Type keyType, Type valueType)
    {
        return (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }
}
