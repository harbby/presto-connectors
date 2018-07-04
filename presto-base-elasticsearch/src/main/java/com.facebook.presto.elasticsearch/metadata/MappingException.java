package com.facebook.presto.elasticsearch.metadata;

import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ES_MAPPING_ERROR;

public class MappingException
        extends PrestoException
{
    public MappingException(String message)
    {
        super(ES_MAPPING_ERROR, message);
    }

    public MappingException(String message, Exception e)
    {
        super(ES_MAPPING_ERROR, message, e);
    }
}
