package com.facebook.presto.elasticsearch.metadata;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ES_MAPPING_EXISTS;

public class MappingException
        extends PrestoException
{
    public MappingException(String message)
    {
        super(ES_MAPPING_EXISTS, message);
    }

    public MappingException(String message, Exception e)
    {
        super(ES_MAPPING_EXISTS, message, e);
    }
}
