package com.facebook.presto.elasticsearch;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.facebook.presto.elasticsearch.conf.ElasticsearchConfig;
import com.facebook.presto.elasticsearch.metadata.EsField;
import com.facebook.presto.elasticsearch.metadata.EsIndex;
import com.facebook.presto.elasticsearch.metadata.IndexResolution;
import com.facebook.presto.elasticsearch.metadata.MappingException;
import com.facebook.presto.elasticsearch.metadata.Types;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.IO_ERROR;
import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.UNEXPECTED_ES_ERROR;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class Elasticsearch2Client
        implements BaseClient
{
    private final Client client;
    private TypeManager typeManager;

    @Inject
    public Elasticsearch2Client(
            TypeManager typeManager,
            Client client,
            ElasticsearchConfig elasticsearchConfig)
    {
        this.typeManager = typeManager;
        this.client = requireNonNull(client, "elasticsearch client is null");
    }

    @Override
    public Set<String> getSchemaNames()
    {
        return ImmutableSet.of("default");
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        String[] indexs = client.admin().indices().prepareGetIndex().get().indices();
        return ImmutableSet.copyOf(indexs);
    }

    @Override
    public List<ElasticsearchSplit> getTabletSplits(ElasticsearchTableHandle tableHandle, ElasticsearchTableLayoutHandle layoutHandle)
    {
        final long timeValue = 60_000;  //1m
        QueryBuilder qb = QueryBuilders.boolQuery();

        String index = tableHandle.getTableName();
        ImmutableList.Builder<ElasticsearchSplit> splitBuilder = ImmutableList.builder();
        for (ClusterSearchShardsGroup shardsGroup : client.admin().cluster().prepareSearchShards(index).get().getGroups()) {
            int shardId = shardsGroup.getShardId();
            SearchResponse scrollResp = client.prepareSearch(index)
                    //.addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                    .setSearchType(SearchType.SCAN)   //不加这个会导致 此处直接就返回数据(数据会在driver主节点上)
                    .setScroll(new TimeValue(timeValue))  //1m
                    .setQuery(qb)
                    .setPreference("_shards:" + shardId)
                    .setSize(100)  //max of 100 hits will be returned for each scroll
                    .get();

            ElasticsearchSplit split = new ElasticsearchSplit(
                    tableHandle.getConnectorId(),
                    tableHandle.getSchemaName(),
                    tableHandle.getTableName(),
                    scrollResp.getScrollId(),
                    timeValue,
                    Optional.empty());
            splitBuilder.add(split);
        }
        //client.admin().cluster().prepareSearchShards(index).get().getGroups()[0].getShards()[0]
        return splitBuilder.build();
    }

    @Override
    public Iterator<Stream<Map<String, Object>>> execute(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns)
    {
        final Iterator<Stream<Map<String, Object>>> iterator = new Iterator<Stream<Map<String, Object>>>()
        {
            private SearchHit[] searchHits;

            @Override
            public boolean hasNext()
            {
                final SearchResponse scrollResp = client.prepareSearchScroll(split.getScrollId())
                        .setScroll(new TimeValue(split.getTimeValue()))
                        .execute().actionGet();
                searchHits = scrollResp.getHits().getHits();
                return searchHits != null && searchHits.length != 0;
            }

            @Override
            public Stream<Map<String, Object>> next()
            {
                return Arrays.stream(searchHits).map(SearchHit::getSource);
            }
        };
        return iterator;
    }

    @Override
    public ElasticsearchTable getTable(SchemaTableName tableName)
    {
        String indexWildcard = tableName.getTableName();
        GetIndexRequest getIndexRequest = createGetIndexRequest(indexWildcard);

        //TODO: es中运行index名访问时可以使用*进行匹配,所以可能会返回多个index的mapping, 因此下面需要进行mapping merge  test table = test1"*"
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings;
        try {
            mappings = client.admin().indices()
                    .getIndex(getIndexRequest).actionGet().getMappings();
        }
        catch (NoNodeAvailableException e) {
            throw new PrestoException(IO_ERROR, e);
        }
        catch (Exception e) {
            throw new PrestoException(UNEXPECTED_ES_ERROR, e);
        }

        List<IndexResolution> resolutions;
        if (mappings.size() > 0) {
            resolutions = new ArrayList<>(mappings.size());
            for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexMappings : mappings) {
                String concreteIndex = indexMappings.key;
                resolutions.add(buildGetIndexResult(concreteIndex, concreteIndex, indexMappings.value));
            }
        }
        else {
            resolutions = emptyList();
        }

        IndexResolution indexWithMerged = merge(resolutions, indexWildcard);
        return new ElasticsearchTable(typeManager, tableName.getSchemaName(), tableName.getTableName(), indexWithMerged.get());
    }

    private static IndexResolution buildGetIndexResult(String concreteIndex, String indexOrAlias,
            ImmutableOpenMap<String, MappingMetaData> mappings)
    {
        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            //Default mappings are ignored as they are applied to each type. Each type alone holds all of its fields.
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }

        if (singleType == null) {
            return IndexResolution.invalid("[" + indexOrAlias + "] doesn't have any types so it is incompatible with sql");
        }
        else if (typeNames != null) {
            Collections.sort(typeNames);
            return IndexResolution.invalid(
                    "[" + indexOrAlias + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        }
        else {
            try {
                Map<String, EsField> mapping = Types.fromEs(singleType.sourceAsMap());
                return IndexResolution.valid(new EsIndex(indexOrAlias, mapping));
            }
            catch (MappingException ex) {
                return IndexResolution.invalid(ex.getMessage());
            }
            catch (IOException e) {
                throw new MappingException("sourceAsMap error", e);
            }
        }
    }

    private static IndexResolution merge(List<IndexResolution> resolutions, String indexWildcard)
    {
        IndexResolution merged = null;
        for (IndexResolution resolution : resolutions) {
            // everything that follows gets compared
            if (!resolution.isValid()) {
                return resolution;
            }
            // initialize resolution on first run
            if (merged == null) {
                merged = resolution;
            }
            // need the same mapping across all resolutions
            if (!merged.get().mapping().equals(resolution.get().mapping())) {
                return IndexResolution.invalid(
                        "[" + indexWildcard + "] points to indices [" + resolution.get().name() + "] "
                                + "and [" + resolution.get().name() + "] which have different mappings. "
                                + "When using multiple indices, the mappings must be identical.");
            }
        }
        if (merged != null) {
            // at this point, we are sure there's the same mapping across all (if that's the case) indices
            // to keep things simple, use the given pattern as index name
            merged = IndexResolution.valid(new EsIndex(indexWildcard, merged.get().mapping()));
        }
        else {
            merged = IndexResolution.notFound(indexWildcard);
        }
        return merged;
    }

    private static GetIndexRequest createGetIndexRequest(String index)
    {
        return new GetIndexRequest()
                .local(true)
                .indices(index)
                .features(GetIndexRequest.Feature.MAPPINGS)
                //lenient because we throw our own errors looking at the response e.g. if something was not resolved
                //also because this way security doesn't throw authorization exceptions but rather honours ignore_unavailable
                .indicesOptions(IndicesOptions.lenientExpandOpen());
    }
}
