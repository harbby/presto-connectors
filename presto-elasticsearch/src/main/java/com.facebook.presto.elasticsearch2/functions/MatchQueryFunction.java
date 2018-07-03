package com.facebook.presto.elasticsearch2.functions;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * 输入bitmap 输出该bitmap的值
 */
public final class MatchQueryFunction
{
    public static final String MATCH_COLUMN_SEP = "__%s@#$%^&*()_+~";

    @ScalarFunction("match_query")
    @Description("es match_query")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public Slice matchQuery(
            @SqlType(StandardTypes.VARCHAR) Slice filter)
    {
        if (filter == null) {
            return null;
        }
        String filterStr = filter.toStringUtf8();

        QueryBuilder builder = QueryBuilders.matchQuery(MATCH_COLUMN_SEP, filterStr);
        return Slices.utf8Slice(builder.toString());
    }

    @ScalarFunction("match_phrase")
    @Description("es match_phrase")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public Slice matchPhrase(
            @SqlType(StandardTypes.VARCHAR) Slice filter)
    {
        if (filter == null) {
            return null;
        }
        String filterStr = filter.toStringUtf8();

        QueryBuilder builder = QueryBuilders.matchPhraseQuery(MATCH_COLUMN_SEP, filterStr);
        return Slices.utf8Slice(builder.toString());
    }
}
