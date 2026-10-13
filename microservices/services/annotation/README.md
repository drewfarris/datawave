## Annotation Service

The annotation service is a DATAWAVE microservice that provides annotation storage, retrieval,
and update capabilities for the DATAWAVE ecosystem. The service supports federated reads across
both `annotation` and `truthmark` table pairs, source-metadata masking, and an asynchronous
ack/retry write pipeline with optional file-backup fallback.

### Annotation Context

*https://host:port/annotation/v1/*

---

### User API

| Method   | Operation                                                | Description                                                                  | Path Params                                        | Request Body     |
|:---------|:---------------------------------------------------------|:-----------------------------------------------------------------------------|:---------------------------------------------------|:-----------------|
| `GET`    | /source/{analyticHash}                                    | Retrieves the annotation source for the given analytic hash                 | [AnalyticHash]                                     | N/A              |
| `GET`    | /{idType}/{id}/types                                      | Lists the distinct annotation types for a document                          | [IdType], [Id]                                      | N/A              |
| `GET`    | /{idType}/{id}                                            | Retrieves all annotations for a document                                    | [IdType], [Id]                                      | N/A              |
| `GET`    | /{idType}/{id}/type/{annotationType}                      | Retrieves all annotations of a specific type for a document                 | [IdType], [Id], [AnnotationType]                   | N/A              |
| `GET`    | /{idType}/{id}/annotation/{annotationId}                  | Retrieves a single annotation by annotation ID                              | [IdType], [Id], [AnnotationId]                     | N/A              |
| `GET`    | /{idType}/{id}/annotation/{annotationId}/segment/{segmentHash} | Retrieves a single segment of an annotation                          | [IdType], [Id], [AnnotationId], [SegmentHash]      | N/A              |
| `POST`   | /{idType}/{id}/annotation                                 | Adds a new annotation for a document                                        | [IdType], [Id]                                      | [Annotation]     |
| `PUT`    | /{idType}/{id}/annotation/{annotationId}                  | Updates an existing annotation by linking a new version to the target       | [IdType], [Id], [AnnotationId]                     | [Annotation]     |

Users must possess the **AnnotationWriter** role to access the `POST` and `PUT` endpoints.

* See [AnnotationControllerV1] class for details

### Architecture

#### Read Path (Federated + Truthmark)

Reads are performed via `FederatedAnnotationReader`, which fans out queries across
both the `annotation`/`annotationSource` table pair and the
`truthmark`/`truthmarkSource` table pair. This mirrors the legacy behavior in
`AnnotationManagerBean.RequestContext`.

The table names are configurable via `AnnotationProperties`:

| Property (YAML)                        | Default          | Description                              |
|:---------------------------------------|:-----------------|:-----------------------------------------|
| `annotation.annotation-table-name`     | `annotation`     | Primary annotation table                 |
| `annotation.annotation-source-table-name` | `annotationSource` | Primary annotation source table    |
| `annotation.truthmark-table-name`      | `truthmark`      | Truthmark annotation table (writes here) |
| `annotation.truthmark-source-table-name` | `truthmarkSource` | Truthmark annotation source table (writes here) |
| `annotation.mask-source-metadata`      | `["visibility"]` | Metadata keys to mask from sources     |
| `annotation.shard-table-name`          | `shard`          | Shard table for internal ID lookups     |

#### Source-Metadata Masking

When returning annotation sources injected into annotations, certain metadata
fields (e.g., `visibility`) are masked based on the `maskSourceMetadata`
configuration. This is applied **only** in `lookupAndInjectAnnotationSource`,
never in the `getAnnotationSource` endpoint.

#### Write Path (Microservice-Only)

The annotation service is the sole writer for truthmark tables. The monolith
ingest framework owns the `annotation`/`annotationSource` tables. Writes are
asynchronous via a message sink (`AnnotationConsumer` → `AccumuloAnnotationWriter`),
with configurable retry (`maxAttempts`, `failTimeoutMillis`,
`backoffIntervalMillis`) and optional file-backup fallback
(`fileAnnotationWriter`, injected as `@Autowired(required = false)`).

#### Ack / Retry Protocol

The `AnnotationAckTracker` singleton bean (replacing the static
`correlationLatchMap`) tracks in-flight writes by correlation ID. The
tracker's `processConfirmAck` handler counts down latches when acks arrive from the
message broker. It runs outside the secured REST controller because broker callbacks
have no requesting user's authentication context. Packaged defaults enable correlated
Rabbit publisher confirms and returns. This protocol is microservice-only; the legacy
`AnnotationManagerBean` has no write path.

The service includes the annotation messaging module and uses
`application/x-protobuf` for annotation bindings, matching the Helm configuration.
The publisher serializes concurrent emissions into its reactive sink; confirmation
waits remain independent for each request.
The service and Docker configuration examples now explicitly select this content
type. Align producers and consumers before cutting over any existing deployment
that uses a different annotation message content type.

### Test Data

The test suite includes sample annotation data for baseline testing:

* `service/src/test/resources/data/annotation_baseline.ndjson` — generated
  compatibility reference produced through the serialization code in
  `core/annotation`. It is retained with the service test resources for
  compatibility validation and is not currently loaded directly by the
  annotation-service unit tests.

### Test Certificates

The test suite includes SSL certificates suitable for local testing:

* `service/src/test/resources/ssl/host.p12` — Server certificate
* `service/src/test/resources/ssl/rootCA.p12` — Root certificate authority

### Roles

Users must possess the **AnnotationWriter** role to add or update annotations.
Query operations (`GET`) are available to any authenticated user.

### Configuration

Configuration is managed via `AnnotationProperties` (Spring `@ConfigurationProperties`
with prefix `annotation`). See [AnnotationProperties] for the full set of options,
including retry settings and ack timeout configuration.

### Independent API and writer deployments

Use the same executable jar and image for both pools. `annotation.role` selects the
deployment's responsibilities at startup:

| Role | Behavior |
|:-----|:---------|
| `combined` (default) | REST API and enabled queue consumers, preserving existing deployments. |
| `api` | REST reads/writes, identifier lookup, the annotation publisher, producer confirms, and optional direct file fallback. No annotation queue consumers or Accumulo writer health checker. |
| `writer` | Enabled Accumulo, log, and dump queue consumers. No annotation REST controller, publisher, acknowledgement tracker, lookup client, read executor, or direct API file fallback. Management endpoints remain available. |

Select the role and its Config Server settings with a service-specific Spring profile:

```bash
java -jar service/target/annotation-service*-exec.jar \
  --spring.profiles.active=annotation-api,dev,remoteauth

java -jar service/target/annotation-service*-exec.jar \
  --spring.profiles.active=annotation-writer,dev,remoteauth
```

Keep `spring.application.name=annotation` in both pools. Spring Cloud Config
combines that application name with each profile to select the configuration:

| Configuration file | Settings |
|:-------------------|:---------|
| `annotation.yml` | Shared destinations, table names, TLS, and authorization. |
| `annotation-annotation-api.yml` | `annotation.role: api`, acknowledgement/retry defaults, and direct fallback disabled. |
| `annotation-annotation-writer.yml` | `annotation.role: writer`, enabled backends, consumer concurrency, and lookup disabled. |

These overlays are supplied in `service/src/main/config`, `docker/config`, and the
Helm configuration chart. Publish them to Config Server before starting split
deployments. No Config Server code changes are required. Activate the role profile
first, then environment/backend profiles so those can override its defaults.
Shared configuration leaves `annotation.role` unset; the combined default remains
packaged in `service/src/main/resources/config/bootstrap.yml` when neither role
profile is active. Both role profiles together, a missing role overlay, or an
effective role that conflicts with the profile fail startup. Remove legacy
`ANNOTATION_ROLE` overrides when migrating to profiles. Direct property selection
remains supported for deployments without either role profile.
Distinguish deployment names and discovery instance IDs. Changing roles requires
restarting the process.

The service filters the configured Spring Cloud function definition before
stream bindings initialize. It retains functions belonging to the selected role
and enabled backends, preserves unrelated functions and compositions, and keeps
`busConsumer` when the configuration bus is enabled. Explicit
`spring.cloud.stream.function.definition` takes precedence over
`spring.cloud.function.definition`. Without either definition it selects the
role's enabled annotation functions and the enabled bus. A writer deployment
fails startup if no enabled annotation queue consumer is selected. Invalid role
values also fail startup.

Backend flags still apply:

- `annotation.writers.accumulo.enabled`: persistence to truthmark tables.
- `annotation.writers.log.enabled`: queue-driven log output.
- `annotation.writers.dump.enabled`: queue-driven file output.
- `annotation.writers.file.enabled`: direct file fallback in API/combined mode
  after publishing retries fail. This is separate from the dump consumer.

The API pool still needs Accumulo for reads and the configured identifier lookup
service. Writer-only deployments need Accumulo connections only when the
Accumulo backend is enabled. Writer management endpoints retain the service's
normal security, TLS, discovery, and messaging health configuration.

For independent Compose pools, run from `datawave/docker`:

```bash
docker compose -f docker-compose.yml -f docker-compose-annotation-split.yml \
  --profile datawave-stack --profile annotation-split up -d \
  --scale annotation-api=3 --scale annotation-writer=1
```

Use the split profile in place of the combined `annotation` profile. API
replicas receive dynamically assigned HTTPS host ports; place a load balancer
in front of the pool. The writer has no published host port. In the separate
`datawave-helm-charts` repository, `annotation/examples/api-values.yaml` and
`annotation/examples/writer-values.yaml` support two releases with distinct
resource names and replica counts. The chart's `role` value activates the matching
`annotation-api` or `annotation-writer` Spring profile,
omits annotation-cache initialization in writer mode, and suppresses writer
ingress while retaining internal management access.

Provision durable queues before switching traffic. Preserve the exchange and
stable backend groups: replicas within `accumulo`, `log`, or `dump` compete for
that group's messages; different groups each receive a copy. The existing dump
binding uses `bindQueue: false`, so its queue must be provisioned separately.
Align custom Rabbit topology checks with the queues actually deployed.

Bring up writers first, bring up and verify the API pool, switch REST traffic,
then retire combined replicas. During overlap, old and new writer replicas
consume from the same groups. Rollback restores combined mode against the same
message format and groups. Tune API replica counts by request load and read
connection budgets; tune writer replicas and binding concurrency by backlog,
persistence throughput, and backend capacity.

The `annotation-api` overlay defaults to broker-confirmed ingress; persistence
happens asynchronously. Combined deployments retain their existing configured
file fallback acceptance. Producer confirms
return to the originating API replica, so API requests do not require sticky
routing. Disable `annotation.writers.file.enabled` for broker-only acceptance,
or provide persistent per-replica fallback storage and a replay procedure.
Queue-driven dump files also need persistent storage and unique replica
subpaths. The split deployment examples do not provision file volumes or a
fallback replay service.

### Docker

For local containerized integration testing, refer to the
[DATAWAVE Docker Compose README][docker-compose] for instructions on building
and running DATAWAVE microservices. The annotation service is opt-in through
the Compose `annotation` profile; these repository assets do not enable a
production deployment.

[docker-compose]:../../../docker/README.md

---

### Getting Started

1. Prepare the supporting services required by the selected configuration.
   A typical development environment includes Config Server, authorization,
   RabbitMQ, Accumulo, Hazelcast, and the DATAWAVE web service used for internal
   identifier lookups. The [Docker Compose guide][docker-compose] is the
   authoritative in-repository setup for the complete local environment.

2. Make the annotation configuration available to Config Server. The checked-in
   examples are `service/src/main/config/annotation.yml` and
   `docker/config/annotation.yml`; select and adapt the one appropriate for the
   development environment.

3. Launch this service with the `remoteauth` profile to enable client-certificate
   authentication:

   ```
   java -jar service/target/annotation-service*-exec.jar --spring.profiles.active=dev,remoteauth
   ```

4. Ensure that the repository's [testUser.p12][testUser] (password: *ChangeIt*) cert is
   imported into your browser, and then visit any of the following:

   * https://localhost:8643/annotation/v1/source/testAnalyticHash
   * https://localhost:8643/annotation/v1/DOCUMENT/20250704_249:testDataType:abcde.fghij.klmno
   * https://localhost:8643/annotation/v1/DOCUMENT/20250704_249:testDataType:abcde.fghij.klmno/annotation/testAnnotationId

[AnnotationControllerV1]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[AnnotationProperties]:service/src/main/java/datawave/microservice/annotation/service/config/AnnotationProperties.java
[testUser]:../../starters/datawave/src/main/resources/testUser.p12
[analyticHash]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[idType]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[id]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotationId]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[segmentHash]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotationType]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
[annotation]:service/src/main/java/datawave/microservice/annotation/service/AnnotationControllerV1.java
