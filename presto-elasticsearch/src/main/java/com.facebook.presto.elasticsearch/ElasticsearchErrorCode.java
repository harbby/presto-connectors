package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum ElasticsearchErrorCode
        implements ErrorCodeSupplier
{
    // Thrown when an Hbase error is caught that we were not expecting,
    // such as when a create table operation fails (even though we know it will succeed due to our validation steps)
    UNEXPECTED_ES_ERROR(1, EXTERNAL),

    // Thrown when a serialization error occurs when reading/writing data from/to Hbase
    IO_ERROR(3, EXTERNAL),

    // Thrown when a table that is expected to exist does not exist
    ES_TABLE_DNE(4, EXTERNAL),

    ES_TABLE_CLOSE_ERR(5, EXTERNAL),

    ES_TABLE_EXISTS(6, EXTERNAL);

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
