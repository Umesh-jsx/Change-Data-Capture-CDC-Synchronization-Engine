# CDC Synchronization Engine

An enterprise-grade Change Data Capture pipeline: captures changes on Orders,
Customers, Products, Inventory and Payments, and synchronizes them downstream
(Elasticsearch) with idempotency, retries, a dead-letter queue, auditing, JWT
security, and Prometheus metrics.

## Important — read this first

This ships as a genuinely runnable Spring Boot project, but a full
production Debezium+Kafka+Elasticsearch+Redis+Grafana stack cannot be
"just imported into Eclipse" — it needs those external services actually
running. So the project has **two modes**, controlled by a Spring profile,
exactly like the pattern used in your other projects:

| Profile | How to run | What it uses |
|---|---|---|
| `local` (default) | Just click Run in Eclipse — zero setup | H2 in-memory DB, a scheduled poller that **simulates** the Debezium/WAL change stream, an Elasticsearch **stub** that logs what it would index, in-memory cache |
| `docker` | `docker compose up -d` first, then run with `-Dspring.profiles.active=docker` | Real PostgreSQL, real Kafka + Debezium (Kafka Connect), real Elasticsearch + Kibana, real Redis, Prometheus + Grafana |

Both profiles run through the **same** idempotency, retry/DLQ, audit, and
metrics code (`EventProcessingService`, `RetryService`, `AuditService`) — so
everything you see working in `local` mode is the real pipeline logic, not a
mock. Only the *transport* (Kafka vs. an in-process poller) and the
*sink* (real ES vs. a logging stub) differ between profiles.

**Import into Eclipse and run `local` mode first** to confirm everything
works with zero external dependencies, then bring up Docker if you want to
exercise the full Kafka/Debezium/Elasticsearch path.

## Assumptions made

- The brief asks for Java 21; this project targets **Java 17** to match your
  Eclipse setup (Spring Boot 3.2 fully supports 17). Bump `java.version` in
  `pom.xml` if you do want to build against 21.
- Elasticsearch sync uses Spring's `RestClient` against the ES REST API
  rather than the full `co.elastic.clients` SDK, to keep the dependency
  footprint (and Eclipse import time) small.
- "Exactly-once business processing" is implemented as **at-least-once
  delivery + idempotent processing** (a `ProcessingStatus` row per event id
  guards against re-applying a redelivered message) — this is the standard,
  achievable interpretation of exactly-once in a Kafka-based pipeline.

## Getting started in Eclipse

1. Unzip the project.
2. Eclipse → File → Import → Maven → Existing Maven Projects → select the
   unzipped folder.
3. **Install the Lombok agent** if you haven't already (Lombok generates
   getters/setters/builders used throughout the entity and DTO classes):
   download `lombok.jar` from projectlombok.org, run
   `java -jar lombok.jar`, point it at your Eclipse install, restart Eclipse.
4. Right-click `CdcSyncEngineApplication.java` → Run As → Java Application.
   No `-Dspring.profiles.active` flag needed — `local` is the default.
5. Once it starts:
   - Swagger UI: http://localhost:8080/swagger-ui.html
   - H2 console: http://localhost:8080/h2-console (JDBC URL:
     `jdbc:h2:mem:cdcdb`, user `sa`, no password)
   - Health: http://localhost:8080/actuator/health
   - Prometheus metrics: http://localhost:8080/actuator/prometheus

## Default users (seeded at startup, no hardcoded hashes)

| Username | Password | Role |
|---|---|---|
| admin | Admin@123 | ADMIN |
| operator | Operator@123 | OPERATOR |

`POST /api/auth/login` with either to get a JWT, then send
`Authorization: Bearer <token>` on everything else. Change these before any
real deployment.

## Watching the pipeline work (local profile)

With nothing else running:
1. Every `cdc.poll-interval-ms` (default 5s), `CdcCaptureService` manufactures
   a few simulated change events into `change_log_entry`.
2. `ChangeDispatchService` picks them up, runs them through
   `EventProcessingService` (idempotency check → ES stub "index" → audit
   record → metrics), and marks them processed.
3. `GET /api/sync/status` shows pending/processed/failed counts.
4. To see a failure + retry play out, temporarily throw an exception from
   `LocalElasticsearchStub.index()` — the event will land in
   `GET /api/failed-events`, and `RetryService` will retry it with
   exponential backoff until it succeeds or is marked `EXHAUSTED`.

## Running the full stack (docker profile)

```bash
docker compose up -d
# wait for containers to be healthy, then register the Debezium connector:
curl -X POST -H "Content-Type: application/json" \
  --data @docker/debezium-postgres-connector.json \
  http://localhost:8083/connectors
```

Then run the app with:
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=docker
```

Now real inserts/updates/deletes on the `orders`, `customers`, `products`,
`inventory`, `payments` tables in the `cdc-postgres` container will flow
through Debezium → Kafka → `KafkaChangeConsumer` (manual ack, 3 concurrent
consumers for horizontal scaling) → the same `EventProcessingService` → real
Elasticsearch, with metadata cached in real Redis.

- Kibana: http://localhost:5601
- Elasticsearch: http://localhost:9200
- Grafana: http://localhost:3000 (admin / admin) — point it at the
  Prometheus datasource (http://prometheus:9090) and build a dashboard on
  the `cdc_events_processed_total`, `cdc_events_failed_total`, and
  `cdc_events_processing_time_seconds` metrics exposed at
  `/actuator/prometheus`.

## Running tests

```bash
mvn test
```

Unit tests (JUnit 5 + Mockito) cover the idempotency guard, successful
processing path, DLQ routing on failure, and the retry/backoff/exhaustion
state machine, plus a `@WebMvcTest` for the sync-status endpoint's
authorization behavior. A Testcontainers-based integration test class isn't
included in this drop (it needs Docker available at test time, which isn't
guaranteed in every Eclipse setup) — see `docs/testcontainers-notes.md` for
how to add one against the existing `docker-compose.yml` services.

## What's implemented vs. documented-only

Implemented and runnable end-to-end in both profiles: idempotent consumer
pattern, DLQ with exponential-backoff retry, sync status API, JWT auth with
ADMIN/OPERATOR roles, audit trail, correlation IDs, Micrometer/Prometheus
metrics, caching (Redis in docker / in-memory in local), Swagger/OpenAPI,
JUnit5+Mockito tests, DataInitializer-based seeding.

Provided as configuration/infrastructure (not application code, since these
are inherently external-service concerns): the Debezium connector config,
docker-compose topology, and a Prometheus scrape config. Grafana dashboard
JSON isn't pre-built — wire the three metric names above into panels once
Grafana is running, since a meaningful dashboard depends on your actual
throughput numbers.

## Project layout

```
src/main/java/com/cdc/sync/
  config/       security, Swagger, correlation-id filter, Kafka, caching
  security/     JWT util, filter, UserDetailsService
  entity/       AppUser, ChangeLogEntry, ProcessingStatus, FailedEvent, AuditLog
  repository/   Spring Data JPA repositories
  service/      capture, dispatch, processing, retry, ES sync, audit, status
  controller/   auth, sync-status, failed-events, admin
  exception/    global exception handler
  DataInitializer.java
docker-compose.yml
docker/                  Prometheus config, Debezium connector JSON
postman/                 Postman collection with auto-token-capture login
docs/                    architecture & ER diagrams, performance report template
```
