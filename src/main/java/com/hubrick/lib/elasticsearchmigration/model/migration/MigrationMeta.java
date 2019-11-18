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
package com.hubrick.lib.elasticsearchmigration.model.migration;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Getter
public class MigrationMeta {

    private final Set<String> sha256Checksums;
    private final int version;
    private final String name;

    public MigrationMeta(final Set<String> sha256Checksums, final int version, final String name) {
        checkNotNull(sha256Checksums, "sha256Checksums must not be null");
        checkArgument(sha256Checksums.stream().map(e -> StringUtils.trimToNull(e) != null).reduce(true, (a, b) -> a && b), "sha256Checksum must not be null");
        checkNotNull(StringUtils.trimToNull(name), "name must not be null");

        this.sha256Checksums = sha256Checksums;
        this.version = version;
        this.name = name;
    }
}
