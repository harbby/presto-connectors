package com.facebook.presto.kudu;

import java.util.Objects;
import static java.util.Objects.requireNonNull;

public class KuduConnectorId
{
    private final String connectorId;

    public KuduConnectorId(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        KuduConnectorId other = (KuduConnectorId) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
