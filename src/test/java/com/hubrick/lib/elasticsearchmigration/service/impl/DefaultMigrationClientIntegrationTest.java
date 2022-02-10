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
package com.hubrick.lib.elasticsearchmigration.service.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hubrick.lib.elasticsearchmigration.AbstractESTest;
import com.hubrick.lib.elasticsearchmigration.exception.MigrationFailedException;
import com.hubrick.lib.elasticsearchmigration.exception.PreviousMigrationFailedException;
import com.hubrick.lib.elasticsearchmigration.model.es.MigrationEntry;
import com.hubrick.lib.elasticsearchmigration.model.es.MigrationEntryMeta;
import com.hubrick.lib.elasticsearchmigration.model.es.State;
import com.hubrick.lib.elasticsearchmigration.model.migration.CreateIndexMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.CreateOrUpdateIndexTemplateMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.DeleteDocumentMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.DeleteIndexMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.DeleteIndexTemplateMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.IndexDocumentMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationMeta;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationSet;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationSetEntry;
import com.hubrick.lib.elasticsearchmigration.model.migration.UpdateMappingMigration;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class DefaultMigrationClientIntegrationTest extends AbstractESTest {

    private static final String IDENTIFIER = "test";

    @Test
    public void testInitIndexCreation() {
        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        assertThat(checkIndexExists(MigrationEntryMeta.INDEX), is(true));
    }

    @Test
    public void testInitIndexCreationNotHappaned() {
        assertThat(checkIndexExists(MigrationEntryMeta.INDEX), is(false));
    }

    @Test
    public void testApplyCreateIndexMigration() {

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkIndexExists("test_index"), is(true));
        assertMigrationEntry();
    }

    @Test
    public void testApplyDeleteIndexMigration() {

        createIndex("test_index", loadResource("create_index.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new DeleteIndexMigration("test_index")),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkIndexExists("test_index"), is(false));
        assertMigrationEntry();
    }

    @Test
    public void testApplyCreateOrUpdateIndexTemplateMigration() {

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateOrUpdateIndexTemplateMigration("test_template", loadResource("create_template.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkTemplateExists("test_template"), is(true));
        assertMigrationEntry();
    }

    @Test
    public void testApplyDeleteIndexTemplateMigration() {

        createTemplate("test_template", loadResource("create_template.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new DeleteIndexTemplateMigration("test_template")),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkTemplateExists("test_template"), is(false));
        assertMigrationEntry();
    }

    @Test
    public void testApplyUpdateMappingMigration() {

        createIndex("test_index", loadResource("create_index.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new UpdateMappingMigration(ImmutableSet.of("test_index"), "test", loadResource("update_mapping.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkIndexExists("test_index"), is(true));
        assertMigrationEntry();
    }


    @Test
    public void testApplyIndexDocumentMigration() {

        createIndex("test_index", loadResource("create_index.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new IndexDocumentMigration("test_index", "test", Optional.of("1"), Optional.empty(), loadResource("index_document.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkDocumentExists("test_index", "test", "1"), is(true));
        assertMigrationEntry();
    }

    @Test
    public void testApplyDeleteDocumentMigration() {

        createIndex("test_index", loadResource("create_index.json"));
        indexDocument("test_index", "test", "1", loadResource("index_document.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new DeleteDocumentMigration("test_index", "test", "1")),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkDocumentExists("test_index", "test", "1"), is(false));
        assertMigrationEntry();
    }

    @Test
    public void testReapplyMigration() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkIndexExists("test_index"), is(false));
        assertMigrationEntry();
    }

    @Test
    public void testPreviousMigrationFailedIgnore() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("failed_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test(expected = PreviousMigrationFailedException.class)
    public void testPreviousMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient(false, 15000, 5);
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("failed_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test(expected = MigrationFailedException.class)
    public void testVersionMismatchMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        ),
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test
    public void testMigrationRetried() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));
        
        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);

        assertThat(checkIndexExists("test_index"), is(true));
        assertMigrationEntry();
    }

    @Test(expected = MigrationFailedException.class)
    public void testNameMismatchMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "wrong_name"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test(expected = MigrationFailedException.class)
    public void testChecksumMismatchMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "20d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test(expected = MigrationFailedException.class)
    public void testLocalChangeSetSmallerMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));
        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-2", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    @Test(expected = MigrationFailedException.class)
    public void testSmallerChangeSetThenAlreadyAppliedMigrationFailedException() {

        final DefaultMigrationClient defaultMigrationClient = createClient();
        defaultMigrationClient.applyMigrationSet(new MigrationSet(Collections.emptySet()));

        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", loadResource("successful_elasticsearchmigration_version_entry.json"));
        indexDocument(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-2", loadResource("successful_elasticsearchmigration_version_entry.json"));

        final MigrationSet migrationSet = new MigrationSet(
                ImmutableSet.of(
                        new MigrationSetEntry(
                                ImmutableList.of(new CreateIndexMigration("test_index", loadResource("create_index.json"))),
                                new MigrationMeta(
                                        ImmutableSet.of(
                                                "10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"
                                        ),
                                        1,
                                        "singularity"
                                )

                        )
                )
        );

        defaultMigrationClient.applyMigrationSet(migrationSet);
    }

    private void assertMigrationEntry() {
        final MigrationEntry migrationEntry = getFromIndex(MigrationEntryMeta.INDEX, MigrationEntryMeta.TYPE, "test-1", MigrationEntry.class);
        assertThat(migrationEntry.getName(), is("singularity"));
        assertThat(migrationEntry.getVersion(), is(1));
        assertThat(migrationEntry.getIdentifier(), is(IDENTIFIER));
        assertThat(migrationEntry.getSha256Checksum(), containsInAnyOrder("10d798ee9a8265432b6b9c621adeec1eb5ae9a79a6d5c3a684e06e6021163007"));
        assertThat(migrationEntry.getState(), is(State.SUCCESS));
        assertThat(migrationEntry.getCreated(), notNullValue());
        assertThat(migrationEntry.getFailureMessage(), isEmptyString());
    }

    private DefaultMigrationClient createClient() {
        return createClient(true, 15000, 5);
    }

    private DefaultMigrationClient createClient(boolean ignorePreviousFailures, int backoffPeriodMillis, int retryCount) {
        final RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        return new DefaultMigrationClient("test", new RestHighLevelClient(builder), ignorePreviousFailures, backoffPeriodMillis, retryCount);
    }
}
