package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum ElasticsearchErrorCode
        implements ErrorCodeSupplier
{
    // Thrown when an Elasticsearch error is caught that we were not expecting,
    // such as when a create table operation fails (even though we know it will succeed due to our validation steps)
    UNEXPECTED_ES_ERROR(1, EXTERNAL),

    // Thrown when a serialization error occurs when reading/writing data from/to Elasticsearch
    IO_ERROR(2, EXTERNAL),

    // Thrown when a table that is expected to exist does not exist
    ES_TABLE_DNE(3, EXTERNAL),

    ES_TABLE_CLOSE_ERR(4, EXTERNAL),

    ES_TABLE_EXISTS(5, EXTERNAL),

    ES_DSL_ERROR(6, USER_ERROR),

    ES_MAPPING_ERROR(7, EXTERNAL);

    private final ErrorCode errorCode;

    ElasticsearchErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0103_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
