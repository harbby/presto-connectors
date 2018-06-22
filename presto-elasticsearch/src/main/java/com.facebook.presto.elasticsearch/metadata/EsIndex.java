package com.facebook.presto.elasticsearch.metadata;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class EsIndex
{
    private final String name;
    private final Map<String, EsField> mapping;

    public EsIndex(String name, Map<String, EsField> mapping)
    {
        this.name = requireNonNull(name, "name is null");
        this.mapping = requireNonNull(mapping, "mapping is null");
    }

    public String name()
    {
        return name;
    }

    public Map<String, EsField> mapping()
    {
        return mapping;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
