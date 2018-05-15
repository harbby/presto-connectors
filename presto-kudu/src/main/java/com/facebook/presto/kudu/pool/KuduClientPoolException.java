package com.facebook.presto.kudu.pool;

public class KuduClientPoolException
        extends RuntimeException
{
    public KuduClientPoolException()
    {
        super();
    }

    public KuduClientPoolException(String message)
    {
        super(message);
    }

    public KuduClientPoolException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public KuduClientPoolException(Throwable cause)
    {
        super(cause);
    }
}
