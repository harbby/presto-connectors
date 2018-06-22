package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.UnsupportedEsField;
import com.facebook.presto.spi.type.Type;

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

    public static Type toPrestoType(EsField esField)
    {
        switch (esField.getDataType()) {
            case NULL:
            case UNSUPPORTED:
                if (esField instanceof UnsupportedEsField && "string".equals(((UnsupportedEsField) esField).getOriginalType())) {
                    return VARCHAR;
                }
                return null;
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
}
