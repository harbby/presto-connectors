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

import com.facebook.presto.kudu.pool.KuduClientPool;
import com.facebook.presto.spi.NodeManager;
import io.airlift.log.Logger;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

/**
 * Created by sylar on 19/04/2017.
 */
public class KuduClientManager
{
    private static final Logger log = Logger.get(KuduClientManager.class);

    private final KuduConfig kuduConfig;
    private KuduClientPool pool;

    @Inject
    KuduClientManager(KuduConfig kuduConfig, NodeManager nodeManager)
    {
        this.kuduConfig = kuduConfig;
    }

    public KuduClient getClient()
    {
        if (null == pool) {
            pool = new KuduClientPool(kuduConfig.getKuduMaster());
        }
//        KuduClient client = new KuduClient.KuduClientBuilder(kuduConfig.getKuduMaster()).build();
        return pool.getResource();
    }

    public KuduTable openTable(KuduClient client, final String name)

    {
        KuduTable kuduTable = null;
        try {
            kuduTable = client.openTable(name);
        }
        catch (KuduException e) {
            log.error(e, e.getMessage());
        }
        return kuduTable;
    }

    public KuduScanToken.KuduScanTokenBuilder newScanTokenBuilder(KuduClient client, final String name)
    {
        KuduScanToken.KuduScanTokenBuilder kuduScanTokenBuilder = client.newScanTokenBuilder(this.openTable(client, name));
        return kuduScanTokenBuilder;
    }

    public void close(KuduClient kuduClient)
    {
        try {
//            kuduClient.close();
            pool.returnResource(kuduClient);
        }
        catch (Exception e) {
            log.error(e, e.getMessage());
        }
    }

    @PreDestroy
    public void tearDown()
    {
        // TODO: 19/04/2017 需要搞清楚tearDown的触发机制
        log.debug("KuduClientManager teadDown");
    }
}
