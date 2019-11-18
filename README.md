Elasticsearch Migration
========

A simple and lightweight migration tool for Elasticsearch database that's based on [Axel Fontaine's Flyway project](https://github.com/flyway/flyway).
Elasticsearch Migration works just like Flyway but using yaml files for describing changessets.

## Requirements
* Java (Tested with JDK 8+)
* Elasticsearch 6.x.x (Tested with 6.2.4+. Could work with lower versions since it's using the REST Api)
```
<dependency>
    <groupId>com.hubrick.lib</groupId>
    <artifactId>elasticsearch-migration</artifactId>
    <version>1.0.5</version>
</dependency>
```

## Indexes
These indexes are created on the first run and are there too keep track of the migrations.

### Migration version index (elasticsearch_migration_version)
Keeping track of the executed changesets. If a migration fails it will be transitioned to state 'FAILED' and the failureMessage field will contain the reason.
The entry won't be removed and the changes applied to this point will stay in the cluster. There is no automatic rollback which means that the cleaneup has to be done manually.

```javascript
  {
    "settings": {
        "number_of_shards": 3
    },
    "mappings": {
        "migration": {
            "dynamic": "strict",
            "_source": {
                "enabled": true
            },
            "properties": {
                "identifier": {
                    "type": "keyword",
                    "index": true
                },
                "version": {
                    "type": "keyword",
                    "index": true
                },
                "name": {
                    "type": "keyword",
                    "index": true
                },
                "sha256Checksum": {
                    "type": "keyword",
                    "index": true
                },
                "state": {
                    "type": "keyword",
                    "index": true
                },
                "failureMessage": {
                    "type": "text",
                    "index": true
                },
                "created": {
                    "type": "date",
                    "format": "date_time",
                    "index": true
                }
            }
        }
    }
}
```

## YAML changesets
The changesets are defined with versioned yaml files (V{version}__{name}.yaml)(example: V1_0_0__singularity.yaml). 
The yaml files have to conform to this schema [YAML Schema](src/main/resources/schema/yaml/schema.json). 

Currently the following migration types are supported: 
 * CREATE_INDEX
 * DELETE_INDEX
 * CREATE_OR_UPDATE_INDEX_TEMPLATE
 * DELETE_INDEX_TEMPLATE
 * UPDATE_MAPPING
 * INDEX_DOCUMENT
 * UPDATE_DOCUMENT
 * DELETE_DOCUMENT

### Example changeset
```yaml
migrations:
  - type: CREATE_INDEX
    index: 'test_index'
    definition: >
      {
          "settings": {
              "number_of_shards": 3
          },
          "mappings": {
              "dynamic": false,
              "_source": {
                  "enabled": true
              },
              "test": {
                  "properties": {
                      "user": {
                          "type": "keyword",
                          "index": true
                      },
                      "post_date": {
                          "type": "keyword",
                          "index": true
                      },
                      "message": {
                          "type": "keyword",
                          "index": true
                      }
                  }
              }
          }
      }
  - type: CREATE_OR_UPDATE_INDEX_TEMPLATE
    template: 'test_template'
    definition: >
      {
        "index_patterns": ["foo*", "bar*"],
        "settings": {
          "number_of_shards": 1
        },
        "mappings": {
          "test": {
            "properties": {
              "host_name": {
                "type": "keyword"
              },
              "created_at": {
                "type": "date",
                "format": "EEE MMM dd HH:mm:ss Z YYYY"
              }
            }
          }
        }
      }
  - type: UPDATE_MAPPING
    mapping: 'test'
    indices:
      - 'test_index'
    definition: >
      {
        "properties": {
          "email": {
            "type": "keyword"
          }
        }
      }
  - type: INDEX_DOCUMENT
    index: 'test_index'
    id: '1'
    mapping: 'test'
    definition: >
      {
          "user" : "kimchy",
          "post_date" : "2009-11-15T14:12:12",
          "message" : "trying out Elasticsearch"
      }
  - type: UPDATE_DOCUMENT
    index: 'test_index'
    mapping: 'test'
    id: '1'
    definition: >
      {
          "doc" : {
              "user" : "new_user"
          }
      }
  - type: DELETE_DOCUMENT
    index: 'test_index'
    mapping: 'test'
    id: '1'
  - type: DELETE_INDEX_TEMPLATE
    template: 'test_template'
  - type: DELETE_INDEX
    index: 'test_index'



```

## Usage
Each service has to define an identitifier which will identify the owner of the indexes, templates, documents etc. in the ES cluster. 
The easiest way is to give the identifier the service name which ownes it.

Example:
```
final ElasticsearchMigration elasticsearchMigration = new ElasticsearchMigration(
  ElasticsearchMigrationConfig.builder( 
    "test-service", 
    ElasticsearchConfig.builder(new URL("http://localhost:9200")).build()
  ).basePackage("migration.es").build()
);

elasticsearchMigration.migrate();
```

## Migration from previous un-managed schema
1. Collect all your schema in one yaml changeset.
2. Create 'Migration version index' using the schemas from above or from the source tree
3. Startup your application manually. After it's started there will be one entry in the 'elasticsearch_migration_version' index. Copy this entry over to your staging/production ES cluster.

## Limitations
* The tool does not roll back the database upon migration failure. You're expected to manually restore backup.

## License
Apache License, Version 2.0
