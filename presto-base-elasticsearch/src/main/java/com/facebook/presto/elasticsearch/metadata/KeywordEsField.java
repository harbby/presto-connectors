/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package com.facebook.presto.elasticsearch.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * SQL-related information about an index field with keyword type
 */
public class KeywordEsField
        extends EsField
{
    private final int precision;
    private final boolean normalized;

    public KeywordEsField(String name)
    {
        this(name, Collections.emptyMap(), true, DataType.KEYWORD.defaultPrecision, false);
    }

    public KeywordEsField(String name, Map<String, EsField> properties, boolean hasDocValues, int precision, boolean normalized)
    {
        super(name, DataType.KEYWORD, properties, hasDocValues);
        this.precision = precision;
        this.normalized = normalized;
    }

    @Override
    public int getPrecision()
    {
        return precision;
    }

    @Override
    public boolean isExact()
    {
        return normalized == false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        KeywordEsField that = (KeywordEsField) o;
        return precision == that.precision &&
                normalized == that.normalized;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), precision, normalized);
    }
}
