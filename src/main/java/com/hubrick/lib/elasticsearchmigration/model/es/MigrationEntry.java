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
package com.hubrick.lib.elasticsearchmigration.model.es;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Instant;
import java.util.Set;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class MigrationEntry {

    @NonNull
    @JsonProperty(MigrationEntryMeta.IDENTIFIER_FIELD)
    private String identifier;
    @NonNull
    @JsonProperty(MigrationEntryMeta.VERSION_FIELD)
    private int version;
    @NonNull
    @JsonProperty(MigrationEntryMeta.NAME_FIELD)
    private String name;
    @NonNull
    @JsonProperty(MigrationEntryMeta.SHA_256_CHECKSUM_FIELD)
    private Set<String> sha256Checksum;
    @NonNull
    @JsonProperty(MigrationEntryMeta.STATE_FIELD)
    private State state;

    @JsonProperty(MigrationEntryMeta.FAUILURE_MESSAGE_FIELD)
    private String failureMessage;
    @NonNull
    @JsonProperty(MigrationEntryMeta.CREATED_FIELD)
    private Instant created;
}
