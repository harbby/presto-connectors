package com.facebook.presto.elasticsearch.metadata;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class IndexResolution
{
    public static IndexResolution valid(EsIndex index)
    {
        requireNonNull(index, "index must not be null if it was found");
        return new IndexResolution(index, null);
    }

    public static IndexResolution invalid(String invalid)
    {
        requireNonNull(invalid, "invalid must not be null to signal that the index is invalid");
        return new IndexResolution(null, invalid);
    }

    public static IndexResolution notFound(String name)
    {
        requireNonNull(name, "name must not be null");
        return invalid("Unknown index [" + name + "]");
    }

    private final EsIndex index;
    @Nullable
    private final String invalid;

    private IndexResolution(EsIndex index, @Nullable String invalid)
    {
        this.index = index;
        this.invalid = invalid;
    }

    public boolean matches(String index)
    {
        return isValid() && this.index.name().equals(index);
    }

    /**
     * Get the {@linkplain EsIndex}
     *
     * @throws MappingException if the index is invalid for use with sql
     */
    public EsIndex get()
    {
        if (invalid != null) {
            throw new MappingException(invalid);
        }
        return index;
    }

    /**
     * Is the index valid for use with sql? Returns {@code false} if the
     * index wasn't found.
     */
    public boolean isValid()
    {
        return invalid == null;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        IndexResolution other = (IndexResolution) obj;
        return Objects.equals(index, other.index)
                && Objects.equals(invalid, other.invalid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index, invalid);
    }

    @Override
    public String toString()
    {
        return invalid != null ? invalid : index.name();
    }
}
