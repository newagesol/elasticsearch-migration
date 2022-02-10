/**
 * Copyright (C) 2018 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.lib.elasticsearchmigration;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.jayway.restassured.http.ContentType;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.jayway.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public abstract class AbstractESTest {

    protected Client client;

    private static final ObjectMapper esObjectMapper = new ObjectMapper();

    @BeforeClass
    public static final void initESClass() {
        esObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        esObjectMapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        esObjectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        esObjectMapper.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true);
        esObjectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        esObjectMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        esObjectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        esObjectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        esObjectMapper.registerModule(new Jdk8Module());
        esObjectMapper.registerModule(new JavaTimeModule());
    }

    @Before
    public final void initES() throws IOException {
        final Settings settings =
                Settings.builder()
                        .put("cluster.name", "elasticsearch")
                        .build();


        client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        flushIndex();

        try {
            final DeleteIndexRequestBuilder deleteIndexRequestBuilder = new DeleteIndexRequestBuilder(client, DeleteIndexAction.INSTANCE, "_all");
            deleteIndexRequestBuilder.setIndicesOptions(IndicesOptions.strictExpand());
            deleteIndexRequestBuilder.get();
        } catch (IndexNotFoundException indexNotFoundException) {
            // Do nothing
        }

        try {
            final DeleteIndexTemplateRequestBuilder deleteIndexTemplateRequestBuilder = new DeleteIndexTemplateRequestBuilder(client, DeleteIndexTemplateAction.INSTANCE, "*");
            deleteIndexTemplateRequestBuilder.get();
        } catch (IndexNotFoundException indexNotFoundException) {
            // Do nothing
        }

        final DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);
        deleteByQueryRequestBuilder.source().setIndices("_all").setQuery(QueryBuilders.matchAllQuery());

        assertThat(deleteByQueryRequestBuilder.get().isTimedOut(), is(false));
        flushIndex();

        final SearchResponse searchResponse = client.prepareSearch("_all")
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
        assertThat(searchResponse.getHits().getTotalHits(), is(0L));
    }

    @SneakyThrows
    protected void indexDocument(final String index, final String type, final String id, final String definition) {
        client.prepareIndex(index, type)
                .setId(id)
                .setSource(esObjectMapper.readValue(definition, Map.class))
                .execute().get();

        client.admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).execute().get();
    }

    @SneakyThrows
    protected void deleteDocument(final String index, final String type, final String id) {
        client.prepareDelete(index, type, id).execute().get();
        client.admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).execute().get();
    }

    @SneakyThrows
    protected void createIndex(final String index, final String definition) {
        final CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(client, CreateIndexAction.INSTANCE)
                .setIndex(index)
                .setSource(definition, XContentType.JSON);

        assertThat(createIndexRequestBuilder.get().isAcknowledged(), is(true));
        client.admin().indices().prepareFlush(index).setWaitIfOngoing(true).setForce(true).execute().get();
    }

    @SneakyThrows
    protected void createTemplate(final String template, final String definition) {
        final PutIndexTemplateRequestBuilder putIndexTemplateRequestBuilder = new PutIndexTemplateRequestBuilder(client, PutIndexTemplateAction.INSTANCE, template)
                .setSource(esObjectMapper.readValue(definition, Map.class));

        assertThat(putIndexTemplateRequestBuilder.get().isAcknowledged(), is(true));
    }

    @SneakyThrows
    protected void flushIndex() {
        given().port(9200).body("{\"wait_if_ongoing\":true}").contentType(ContentType.JSON).expect().statusCode(200).post("/_flush");
        Thread.sleep(2000);
    }

    @SneakyThrows
    protected String loadResource(String fileName) {
        try (final InputStream resourceStream = this.getClass().getResourceAsStream(fileName)) {
            if (resourceStream == null) {
                throw new IllegalStateException("No such file: " + fileName);
            }
            return IOUtils.toString(resourceStream);
        }
    }

    @SneakyThrows
    protected boolean checkDocumentExists(String indexName, String type, String id) {
        final ActionFuture<GetResponse> response = client.get(new GetRequest(indexName, type, id));
        client.admin().indices().prepareFlush(indexName).setWaitIfOngoing(true).setForce(true).execute().get();
        final GetResponse getFields = response.get();

        assertThat("Response should be done", response.isDone(), is(true));
        return getFields.isExists();
    }

    @SneakyThrows
    protected boolean checkIndexExists(String indexName) {
        final ActionFuture<GetIndexResponse> response = client.admin().indices().getIndex(new GetIndexRequest().indices(indexName));

        try {
            return response.get() != null;
        } catch (ExecutionException e) {
            return false;
        }
    }

    @SneakyThrows
    protected boolean checkTemplateExists(String name) {
        final ActionFuture<GetIndexTemplatesResponse> response = client.admin().indices().getTemplates(new GetIndexTemplatesRequest(name));
        return !response.get().getIndexTemplates().isEmpty();
    }

    @SneakyThrows
    protected String getFromIndex(String indexName, String type, String id) {
        final ActionFuture<GetResponse> response = client.get(new GetRequest(indexName, type, id).fetchSourceContext(FetchSourceContext.FETCH_SOURCE));
        client.admin().indices().prepareFlush(indexName).setWaitIfOngoing(true).setForce(true).execute().get();
        final GetResponse getFields = response.get();

        assertThat("Response should be done", response.isDone(), is(true));
        assertThat("Get " + id + " should exist (" + indexName + ", " + type + ")", getFields.isExists(), is(true));
        assertThat("Source field should not be empty", getFields.isSourceEmpty(), is(false));

        final String sourceAsString = getFields.getSourceAsString();

        assertThat("response source should not be null", sourceAsString, notNullValue());
        return sourceAsString;
    }

    @SneakyThrows
    protected <T> T getFromIndex(String indexName, String type, String id, Class<T> clazz) {
        final ActionFuture<GetResponse> response = client.get(new GetRequest(indexName, type, id));
        client.admin().indices().prepareFlush(indexName).setWaitIfOngoing(true).setForce(true).execute().get();
        final GetResponse getFields = response.get();

        final String sourceAsString = getFields.getSourceAsString();

        return esObjectMapper.readValue(sourceAsString, clazz);
    }
}
