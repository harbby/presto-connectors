/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kudu;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class KuduTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final String tableName;
    private final String schemaName;

    @JsonCreator
    public KuduTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "tableName is null");
        this.schemaName = schemaTableName.getSchemaName();
        this.tableName = schemaTableName.getTableName();
    }

    @JsonProperty
    public String getConnectorId()
    {
        return this.connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return schemaTableName.getTableName();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaTableName.getSchemaName();
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return this.schemaTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
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
        KuduTableHandle other = (KuduTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
            Objects.equals(this.schemaName, other.schemaName) &&
            Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + tableName;
    }
}
