package com.facebook.presto.kudu.pool;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * PoolableObjectFactory custom impl.
 */
class KuduClientFactory
        implements PooledObjectFactory<KuduClient>
{
    private final String masterAddresses;

    public KuduClientFactory(final String masterAddresses)
    {
        this.masterAddresses = masterAddresses;
    }

    @Override
    public void activateObject(PooledObject<KuduClient> pooledKuduClient)
            throws Exception
    {
        // TODO active kudu client.
    }

    @Override
    public void destroyObject(PooledObject<KuduClient> pooledKuduClient)
            throws Exception
    {
        final KuduClient kuduClient = pooledKuduClient.getObject();
        kuduClient.close();
    }

    @Override
    public PooledObject<KuduClient> makeObject()
            throws Exception
    {
        KuduClient kuduClient = null;

        try {
            kuduClient = new KuduClient.KuduClientBuilder(masterAddresses).build();
        }
        catch (Exception e) {
            throw new KuduClientPoolException(e);
        }

        return new DefaultPooledObject<KuduClient>(kuduClient);
    }

    @Override
    public void passivateObject(PooledObject<KuduClient> pooledKuduClient)
            throws Exception
    {
        // TODO Not sure right now.
    }

    @Override
    public boolean validateObject(PooledObject<KuduClient> pooledKuduClient)
    {
        final KuduClient kuduClient = pooledKuduClient.getObject();
        try {
            kuduClient.listTabletServers();
            return true;
        }
        catch (KuduException e) {
            return false;
        }
    }
}
