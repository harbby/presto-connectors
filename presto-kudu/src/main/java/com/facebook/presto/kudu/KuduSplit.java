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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KuduSplit
        implements ConnectorSplit
{
    private final List<HostAddress> addresses;
    private final SchemaTableName tableName;
    //用来传输where条件
    private final TupleDomain<KuduColumnHandle> effectivePredicate;
    private final int kuduTokenId;

    @JsonCreator
    public KuduSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("kuduTokenId") int kuduTokenId,
            @JsonProperty("effectivePredicate") TupleDomain<KuduColumnHandle> effectivePredicate)

    {
        this.addresses = addresses;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.kuduTokenId = requireNonNull(kuduTokenId, "kuduScanToken is null");
        this.effectivePredicate = effectivePredicate;
    }

    @JsonProperty
    public int getKuduTokenId()
    {
        return kuduTokenId;
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<KuduColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
//                .add("address", addresses)
                .add("tableName", tableName)
                .add("kuduTokenId", kuduTokenId)
                .toString();
    }
}
