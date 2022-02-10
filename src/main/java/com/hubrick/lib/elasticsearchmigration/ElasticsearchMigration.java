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

import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationSet;
import com.hubrick.lib.elasticsearchmigration.service.MigrationClient;
import com.hubrick.lib.elasticsearchmigration.service.MigrationSetProvider;
import com.hubrick.lib.elasticsearchmigration.service.impl.DefaultMigrationClient;
import com.hubrick.lib.elasticsearchmigration.service.impl.YamlDirectoryMigrationSetProvider;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.stream.Collectors;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Slf4j
public class ElasticsearchMigration {

    private final ElasticsearchMigrationConfig elasticsearchMigrationConfig;
    private final MigrationSetProvider migrationSetProvider;

    public ElasticsearchMigration(@NonNull final ElasticsearchMigrationConfig elasticsearchMigrationConfig) {
        this.elasticsearchMigrationConfig = elasticsearchMigrationConfig;
        this.migrationSetProvider = new YamlDirectoryMigrationSetProvider();
    }

    private RestHighLevelClient createElasticsearchClient(ElasticsearchConfig elasticsearchConfig) {
        final RestClientBuilder builder = RestClient.builder(
                elasticsearchConfig.getUrls().stream().map(e -> new HttpHost(e.getHost(), e.getPort(), e.getProtocol())).collect(Collectors.toSet()).toArray(new HttpHost[0])
        );
        builder.setDefaultHeaders(elasticsearchConfig.getHeaders().entries().stream().map(e -> new BasicHeader(e.getKey(), e.getValue())).collect(Collectors.toList()).toArray(new Header[0]));

        if (elasticsearchConfig.getMaxRetryTimeoutMillis() != null) {
            builder.setMaxRetryTimeoutMillis(elasticsearchConfig.getMaxRetryTimeoutMillis());
        }
        if (elasticsearchConfig.getPathPrefix() != null) {
            builder.setPathPrefix(elasticsearchConfig.getPathPrefix());
        }

        return new RestHighLevelClient(builder);
    }

    public void migrate() {
        log.info("Starting ES schema migration...");
        try (MigrationClient migrationClient = buildMigrationClient()) {
            final MigrationSet migrationSet = migrationSetProvider.getMigrationSet(elasticsearchMigrationConfig.getBasePackage());
            migrationClient.applyMigrationSet(migrationSet);
            log.info("Finished ES schema migration");
        }
    }

    private MigrationClient buildMigrationClient() {
        return new DefaultMigrationClient(
                elasticsearchMigrationConfig.getIdentifier(),
                createElasticsearchClient(elasticsearchMigrationConfig.getElasticsearchConfig()),
                elasticsearchMigrationConfig.getIgnorePreviousFailures(),
                elasticsearchMigrationConfig.getBackoffPeriodInMillis(),
                elasticsearchMigrationConfig.getRetryCount()
        );
    }
}
