package com.facebook.presto.kudu.pool;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kudu.client.KuduClient;

import java.io.Closeable;
import java.util.NoSuchElementException;

public class KuduClientPool
        implements Closeable
{
    protected GenericObjectPool<KuduClient> internalPool;

    public KuduClientPool()
    {
        this(new GenericObjectPoolConfig(), new KuduClientFactory("localhost:7051"));
    }

    public KuduClientPool(String masterAddresses)
    {
        this(new GenericObjectPoolConfig(), new KuduClientFactory(masterAddresses));
    }

    public KuduClientPool(final GenericObjectPoolConfig poolConfig, String masterAddresses)
    {
        this(poolConfig, new KuduClientFactory(masterAddresses));
    }

    private KuduClientPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<KuduClient> factory)
    {
        initPool(poolConfig, factory);
    }

    @Override
    public void close()
    {
        destroy();
    }

    public boolean isClosed()
    {
        return this.internalPool.isClosed();
    }

    public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<KuduClient> factory)
    {
        if (this.internalPool != null) {
            try {
                closeInternalPool();
            }
            catch (Exception e) {
            }
        }

        this.internalPool = new GenericObjectPool<KuduClient>(factory, poolConfig);
    }

    public KuduClient getResource()
    {
        try {
            return internalPool.borrowObject();
        }
        catch (NoSuchElementException nse) {
            throw new KuduClientPoolException("Could not get a resource from the pool", nse);
        }
        catch (Exception e) {
            throw new KuduClientPoolException("Could not get a resource from the pool", e);
        }
    }

    public void returnResourceObject(final KuduClient resource)
    {
        if (resource == null) {
            return;
        }
        try {
            internalPool.returnObject(resource);
        }
        catch (Exception e) {
            throw new KuduClientPoolException("Could not return the resource to the pool", e);
        }
    }

    public void returnBrokenResource(final KuduClient resource)
    {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    public void returnResource(final KuduClient resource)
    {
        if (resource != null) {
            try {
                returnResourceObject(resource);
            }
            catch (Exception e) {
                returnBrokenResource(resource);
                throw new KuduClientPoolException("Could not return the resource to the pool", e);
            }
        }
    }

    public void destroy()
    {
        closeInternalPool();
    }

    protected void returnBrokenResourceObject(final KuduClient resource)
    {
        try {
            internalPool.invalidateObject(resource);
        }
        catch (Exception e) {
            throw new KuduClientPoolException("Could not return the resource to the pool", e);
        }
    }

    protected void closeInternalPool()
    {
        try {
            internalPool.close();
        }
        catch (Exception e) {
            throw new KuduClientPoolException("Could not destroy the pool", e);
        }
    }

    public int getNumActive()
    {
        if (poolInactive()) {
            return -1;
        }

        return this.internalPool.getNumActive();
    }

    public int getNumIdle()
    {
        if (poolInactive()) {
            return -1;
        }

        return this.internalPool.getNumIdle();
    }

    public int getNumWaiters()
    {
        if (poolInactive()) {
            return -1;
        }

        return this.internalPool.getNumWaiters();
    }

    public long getMeanBorrowWaitTimeMillis()
    {
        if (poolInactive()) {
            return -1;
        }

        return this.internalPool.getMeanBorrowWaitTimeMillis();
    }

    public long getMaxBorrowWaitTimeMillis()
    {
        if (poolInactive()) {
            return -1;
        }

        return this.internalPool.getMaxBorrowWaitTimeMillis();
    }

    private boolean poolInactive()
    {
        return this.internalPool == null || this.internalPool.isClosed();
    }

    public void addObjects(int count)
    {
        try {
            for (int i = 0; i < count; i++) {
                this.internalPool.addObject();
            }
        }
        catch (Exception e) {
            throw new KuduClientPoolException("Error trying to add idle objects", e);
        }
    }
}
