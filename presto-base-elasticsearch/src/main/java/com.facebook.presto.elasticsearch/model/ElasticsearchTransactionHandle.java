package com.facebook.presto.elasticsearch.model;

import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTransactionHandle
        implements ConnectorTransactionHandle
{
    private final UUID uuid;

    public ElasticsearchTransactionHandle()
    {
        this(UUID.randomUUID());
    }

    @JsonCreator
    public ElasticsearchTransactionHandle(@JsonProperty("uuid") UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        return Objects.equals(uuid, ((ElasticsearchTransactionHandle) obj).uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("uuid", uuid).toString();
    }
}
