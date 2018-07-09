package com.facebook.presto.elasticsearch.io;

import java.util.Map;

public class Document
{
    private String index;
    private String type = "presto";
    private String id;

    private Map<String, Object> source;

    public Map<String, Object> getSource()
    {
        return source;
    }

    public String getId()
    {
        return id;
    }

    public String getIndex()
    {
        return index;
    }

    public String getType()
    {
        return type;
    }

    public static DocumentBuilder newDocument()
    {
        return new DocumentBuilder();
    }

    public static class DocumentBuilder
    {
        private final Document document = new Document();

        public DocumentBuilder setIndex(String index)
        {
            document.index = index;
            return this;
        }

        public DocumentBuilder setType(String type)
        {
            document.type = type;
            return this;
        }

        public DocumentBuilder setId(String id)
        {
            document.id = id;
            return this;
        }

        public DocumentBuilder setSource(Map<String, Object> source)
        {
            document.source = source;
            return this;
        }

        public Document get()
        {
            return document;
        }
    }
}
