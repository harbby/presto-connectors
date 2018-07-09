package com.facebook.presto.elasticsearch.io;

import java.io.Closeable;
import java.util.Iterator;

public interface SearchResult<E>
        extends Iterator<E>, Closeable
{
}
