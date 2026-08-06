# Metadata-Driven Framework — GPT-5.6-Sol Implementation Prompt

## Recommended model settings

- Model: GPT-5.6-Sol
- Reasoning effort: High
- Service tier: Priority/fast when available

## Prompt

You are implementing a production-grade, enterprise metadata-driven data-lake framework in:

`C:\Users\shravan\Desktop\AstraDataV3`

Work as a senior production engineer. Use high reasoning internally, but keep the implementation simple, direct and easy to maintain. Do not stop after producing only an analysis. Complete the mandatory inspection, then implement phase by phase unless a defined phase-boundary condition requires a handoff. Provide concise progress updates while working.

## 1. Authoritative inputs and decision hierarchy

Read these completely before changing code:

1. Repository `AGENTS.md` instructions.
2. `C:\Users\shravan\Desktop\AstraDataV3\metadata-README.md`
3. `C:\Users\shravan\Desktop\AstraDataV3\docs\Metadata-Driven-Ingestion-Framework-Technical-Design-v1.1.docx`
4. Existing application workflow, nodes, models, platform clients, database utilities, tests and deployment configuration.

When instructions conflict, apply this priority:

1. Repository `AGENTS.md`.
2. Current authoritative `metadata-README.md` schema.
3. Approved technical design document.
4. This implementation prompt.
5. Existing implementation conventions.

Do not invent a schema field, file, repository component, workflow stage or platform capability to reconcile a conflict. Report the conflict and continue only with unaffected work.

Do not claim that a file, client, stage, test, table column or platform capability exists until it has been inspected in the repository.

The current `metadata-README.md` DDL is authoritative. Do not modify its logical schema without explicit user approval.

## 2. Primary objective

Upgrade the existing BRD-to-code-generation and execution flow into the agreed metadata-driven framework.

Implement database/JDBC sources first. Preserve clean extension boundaries for future SFTP, REST API, ADLS, S3, CSV, JSON, XML, Parquet and compressed payloads. Do not implement speculative connector or parser functionality that is not needed for the database-source milestone.

Do not copy or depend on the previous notebook-based Databricks metadata framework. The new implementation must be driven by this repository and the current eight-table metadata model.

Generated execution may use Python applications, packaged jobs, dbt projects, worker services or target-native jobs. Do not assume that all execution occurs through Databricks notebooks.

## 3. Non-negotiable engineering rules

1. Preserve all existing working functionality, APIs, UI behavior and pipeline outputs.
2. Upgrade the existing flow directly. Do not create duplicate permanent legacy and metadata implementations.
3. Use a temporary rollout flag only if essential, and only if the repository already has an appropriate feature/configuration mechanism.
4. Inspect and trace the real flow before editing.
5. Reuse existing nodes, utilities, platform clients, repositories and patterns.
6. Make the smallest correct changes in the fewest appropriate files.
7. Do not introduce unnecessary abstractions, wrappers, services, dependencies or boilerplate.
8. Keep flow direct. If the requirement is A→B, do not introduce A→C→B unless C solves a demonstrated requirement.
9. Add concise comments only around non-obvious logical blocks.
10. Fix shared behavior at the correct shared boundary instead of patching every caller separately.
11. Preserve all unrelated and uncommitted user changes.
12. Add focused tests for every non-trivial behavior.
13. Do not commit, reset, merge or push Git branches unless separately requested.
14. Do not introduce schema changes to the eight metadata tables or existing application tables.
15. Do not introduce a ninth metadata table.
16. Use existing JSON/string fields for structured configuration and evidence.
17. Never use `ai_store` as runtime configuration.
18. The final implementation must be production-grade, secure, idempotent, observable and maintainable.
19. No unresolved critical security, data-loss, concurrency, idempotency or correctness issue is acceptable.

## 4. Terminology

Use these terms consistently:

- Source connector type: initially JDBC/database.
- Source system: logical application, vendor, business system or data producer.
- Source connection: physical database endpoint and authentication/secret references.
- Source object: a source table or controlled query for the database-first scope.
- Target platform: Databricks, Snowflake or Fabric.
- Target environment: development, QA or production workspace/account.
- Design run: BRD-to-code-generation workflow tracked through existing application design stores.
- Runtime run: one execution attempt recorded in `metadata.ctl_run`.

Do not use the ambiguous phrase source platform when source system, connection or connector type is intended.

## 5. Fixed metadata boundary

Only these eight target-resident metadata/control tables are permitted:

Configuration:

1. `metadata.cfg_source_system`
2. `metadata.cfg_connection`
3. `metadata.cfg_ingestion_object`
4. `metadata.cfg_mapping`

Runtime/control:

5. `metadata.ctl_ingestion_queue`
6. `metadata.ctl_run`
7. `metadata.ctl_error_log`
8. `metadata.ctl_watermark`

Existing application/design stores remain separate:

- `brd_run_registry`
- `kpi_checkpoints`
- `ai_store`
- `hitl_review_queue`

Do not add another metadata table. Do not copy detailed design evidence into runtime metadata when it can remain in `ai_store`.

Responsibility boundary:

- `ai_store`: prompts, LLM outputs, nomination evidence, schema discovery evidence, column profiles, semantic enrichment, review evidence and generated-artifact evidence.
- Configuration tables: approved and versioned operational configuration required for generation and runtime.
- Control tables: runtime queueing, attempts, failures and committed checkpoints.

Generated runtime must never query `ai_store`, `hitl_review_queue` or the Knowledge Base.

## 6. Mandatory read-only inspection

Before making code changes:

1. Inspect repository and Git status.
2. Identify application entry points and package structure.
3. Trace the current workflow end to end: BRD ingestion, requirement extraction, KPI extraction, HITL Gate 1, table nomination, HITL Gate 2, metadata discovery, column profiling, semantic enrichment, enrichment approval, Bronze generation/review, Silver merge-key resolution, Silver generation/review, Gold generation/review and runtime execution.
4. Locate and evaluate existing Databricks, Snowflake and Fabric clients; database utilities; secret management; SQL/repository patterns; workflow state; `ai_store`; HITL; queue/retry/locking; artifact storage; runtime submission; observability; tests; deployment configuration.
5. Grep all callers before modifying shared functions.
6. Run a proportionate baseline test set.
7. Prepare a concise internal architecture and gap map.

Only then begin implementation. Do not begin with a broad rewrite.

## 7. Specialist reviews

Use specialist sub-agents when supported:

- Databricks SME: Delta DDL, SQL, MERGE semantics, concurrency, queue claims, watermarks and artifact execution.
- Snowflake SME: Snowflake DDL, transactions, MERGE, quoting, parameter binding, concurrency and dbt/artifact execution.
- Fabric SME: Warehouse/Lakehouse SQL, DDL, connectivity, metadata operations and execution prerequisites.
- AI/LLM SME: context construction, structured outputs, grounding, hallucination controls and sensitive-data handling.
- Technical architecture SME: end-to-end consistency, simplicity, idempotency, security and recovery.

If sub-agents are unavailable, perform and document separate bounded reviews for all five areas. Avoid concurrent edits to shared files. The primary agent owns final integration.

## 8. Platform delivery priority

Do not create three shallow implementations merely to claim coverage. Phase 0 must confirm the platform support that actually exists.

For Databricks and Snowflake provide platform-compatible DDL, production metadata repositories, database-source design-flow integration, target execution through existing runtimes, unit/contract tests and live integration tests when environments are available.

For Fabric provide platform-compatible DDL, the same logical metadata repository contract and SQL/behavior contract tests. Reuse an existing Fabric client if inspection confirms one exists. If no reusable Fabric connectivity/runtime exists, do not invent or claim it. Provide an explicit fail-fast unsupported boundary, document the missing prerequisite and do not claim Fabric deployment validation.

A platform is implementation-complete only when its code and locally executable tests pass. It is deployment-validated only when its live integration and concurrency tests pass.

## 9. Target selection and metadata location

The UI flow begins with:

1. Upload BRD.
2. Select target platform and environment.
3. Select an already onboarded source system and database connection.
4. Optionally select a Knowledge Base.

The selected target determines where the eight metadata tables are located. Do not add `target_platform` to metadata. The selected metadata repository identifies the platform.

Carry target identity through existing workflow state/checkpoint JSON from initial selection through all approvals, discovery, profiling, enrichment, mappings, generation and execution. Never require a second target selection. Approval callbacks must write to the originally selected target.

## 10. Metadata repository contract

Create only the smallest common contract needed to hide platform SQL differences. Reuse existing platform clients.

The contract must cover these capabilities using existing naming and abstractions where possible:

- Read and validate source systems/connections.
- Idempotently write ingestion objects and versioned mapping bundles.
- Select exact mapping/configuration versions.
- Activate validated configuration bundles.
- Create and atomically claim queue work.
- Create run attempts and record errors.
- Read and safely commit watermarks.

Do not create wrapper methods merely to match suggested names.

Each platform implementation owns SQL dialect, parameter binding, identifier quoting, catalog/schema naming, datatypes, timestamps, JSON, MERGE/upsert behavior, transactions, concurrency, ID generation and error translation. Do not scatter target-specific SQL conditionals through workflow nodes.

## 11. Prerequisite and onboarding setup

Reuse an existing bootstrap/migration directory if available. Otherwise create a focused `prereq/` area containing:

- Databricks metadata DDL
- Snowflake metadata DDL
- Fabric metadata DDL
- Idempotent bootstrap entry point
- Schema preflight validation
- Source-system/connection onboarding
- Setup documentation

Platform DDL may translate syntax and datatypes but must preserve the logical schema from `metadata-README.md`.

The setup is administrator-initiated but code-automated. Do not require ad hoc SQL maintenance.

Onboarding must upsert `cfg_source_system`, obtain its ID, upsert `cfg_connection`, validate relationships/settings, store only secret references and be safe to repeat. Reuse an existing admin UI/API/CLI; otherwise implement the smallest controlled entry point.

Do not use unsafe `MAX(id) + 1`. Reuse an existing safe ID strategy or use a documented concurrency-safe platform capability without adding a table or changing the schema.

## 12. Secret handling

`cfg_connection` stores only secret-store references. Support multi-secret authentication through the approved JSON contract.

Never store or log passwords, tokens, OAuth secrets, private keys, authorization headers or credential-bearing connection strings. Resolve secrets only when needed. Sanitize connection failures before persistence.

## 13. BRD, KPI and optional KB flow

Continue existing BRD ingestion, requirement extraction, KPI extraction and HITL Gate 1. These stages remain BRD-driven and independent of source connectivity and target SQL. Continue using existing application stores.

If a KB is selected, it may assist requirements, KPIs, nomination terminology, semantic enrichment, mappings and code-generation standards. Record KB identity and references in workflow state/`ai_store`. Validate KB claims against discovered metadata. The KB must not invent fields, override approved metadata or become a runtime dependency.

## 14. Table nomination

At nomination:

1. Use the selected target metadata repository.
2. Read and validate `cfg_source_system` and `cfg_connection`.
3. Validate source-system/connection consistency.
4. Resolve secret references.
5. Connect to the source database.
6. Reuse existing keyword expansion, lexical search, semantic search, rank fusion, FK expansion and lookup sweep.
7. Store candidates, scores and evidence in `ai_store`.
8. Use existing HITL Gate 2.

Do not create ingestion objects for all candidates.

After approval, idempotently create one inactive/draft `cfg_ingestion_object` per approved source table. Return and carry `ingestion_object_id` and `config_version`. Rejected tables remain only as `ai_store` evidence.

## 15. Metadata discovery and profiling

For each approved ingestion object, read the approved source connection and discover catalog, schema, table, columns, datatypes, nullability, primary keys, foreign keys, constraints and relationships. Reuse current discovery logic and store evidence in `ai_store`.

Do not add `ingestion_object_id` to the `ai_store` schema. Put it in the existing JSON payload:

    {
      "ingestion_object_id": 123,
      "database_name": "ClaimsDB",
      "schema_name": "dbo",
      "table_name": "Claims"
    }

Reuse column profiling. Store null percentages, cardinality, min/max, patterns, key evidence and safe samples in `ai_store` JSON with `ingestion_object_id`. Do not store profile statistics in `cfg_mapping`.

## 16. Semantic enrichment and guarded LLM use

Reuse semantic enrichment. Supply curated approved requirements/KPIs, tables, schema, keys, relationships, profiles, relevant KB references and target constraints.

Store semantic meanings, classifications, proposed transformations, key candidates, confidence and evidence in `ai_store`. Send results through human approval. Only approved executable details are promoted to `cfg_mapping`.

The LLM may propose naming/normalization, Silver transformations and keys, Gold facts/dimensions/joins/grain/aggregations/KPIs and target-specific code.

Every call requires strict structured output, schema validation, controlled transformations, real table/column verification, type/key/relationship validation, hallucination controls, prompt versioning, evidence, safe retries and approval where required.

Never send secrets or unnecessary sensitive rows. Never execute unrestricted metadata Python or arbitrary unvalidated SQL. The LLM proposes; application validation and humans decide.

## 17. Source-to-Bronze mapping

After enrichment approval:

1. Promote only the executable mapping contract.
2. Create one `cfg_mapping` row per source field/path.
3. Set `processing_stage = SOURCE_TO_BRONZE`.
4. Store source/target objects, fields, types, nullability, ordinal position, key participation, controlled transformation and validation behavior.
5. Keep profiles, reasoning and scores in `ai_store`.
6. Save an inactive draft `mapping_version`.

Bronze generation selects the exact draft using `ingestion_object_id`, `processing_stage`, explicit `mapping_version` and `active_flag = false`. Never select an arbitrary inactive mapping. Carry the approved version in workflow state/`ai_store`; do not add design `run_id` to `cfg_mapping`.

The pre-generation approval approves mapping design. The post-generation review verifies that code implements it. After validation, register the artifact and activate the matching mapping/configuration consistently. Runtime selects active configuration only.

## 18. Execution artifact contract

The authoritative DDL contains `cfg_ingestion_object.execution_spec_json`.

Generated code remains in existing durable artifact storage. Do not store full source code in this JSON. Use a versioned contract containing fields such as schema version, engine, artifact ID/URI, entry point, deployment ID, SHA-256 hash, generator version and mapping version.

Requirements:

- Artifact URI must be allow-listed.
- Verify the artifact hash before execution.
- Mapping/configuration version must match the artifact.
- Draft objects may omit the specification.
- Active executable objects must have a valid specification.
- Runtime must not locate artifacts through `ai_store`.

Preflight must compare deployed schema with `metadata-README.md`. If a required column is absent, report schema drift and stop deployment for that environment. Do not silently fall back to `ai_store` or alter schema without approval.

## 19. Bronze-to-Silver mapping

During Silver Merge Key Resolution, create/finalize the inactive `BRONZE_TO_SILVER` bundle with Bronze inputs, Silver outputs, types, controlled transformations, validation rules and business-key participation. Store merge keys in `cfg_ingestion_object.merge_keys_json`; keep reasoning in `ai_store`.

Silver generation reads the exact approved draft version. After validation/review, register the artifact, verify hashes and activate the corresponding mapping/artifact consistently.

## 20. Silver-to-Gold mapping

After Silver approval, position Gold design/mapping immediately before Gold generation.

For each Gold fact/dimension, create a transformation-type `cfg_ingestion_object` and draft `SILVER_TO_GOLD` mappings. Represent Silver inputs, multiple input objects, target grain, joins, aggregations, KPI formulas, dependencies and build order using existing columns/JSON. Keep reasoning in `ai_store`.

Gold generation reads the exact draft version. After validation/review, register and activate the Gold object, mapping and artifact consistently.

Do not reject computable Gold objects merely because they require multiple Silver inputs. Reject only for missing inputs, unresolved grain, invalid/ambiguous joins, unsupported KPI computation, failed validation or human rejection. Persist exact rejection reasons.

## 21. Code generation and execution boundary

The design flow remains common through mapping and generation orchestration. Generation is mapping-driven, guarded, target-aware, versioned and deterministically validated.

Runtime flow:

    Selected target
    → load active ingestion object and exact mapping/configuration version
    → load execution_spec_json
    → resolve artifact and verify hash
    → submit to target compute
    → execute
    → verify target commit
    → update runtime controls

Bronze target compute reads the external source through the approved connection. Silver and Gold run within the selected target.

Runtime must not regenerate mappings, call an LLM, query `ai_store`, query HITL/KB, select inactive mappings or execute an unverified artifact.

## 22. Control tables and runtime flow

Control tables become active during actual execution:

- `ctl_ingestion_queue`: bounded work, claims, leases, attempts and retries.
- `ctl_run`: one row per attempt.
- `ctl_error_log`: sanitized failures and retryability.
- `ctl_watermark`: last successfully committed incremental position.

Keep design and runtime run IDs separate.

Success sequence:

1. Create PENDING queue request with stable logical-work and idempotency identity.
2. Atomically claim it and record worker lease.
3. Create RUNNING `ctl_run` attempt.
4. Load one consistent active metadata snapshot.
5. Verify metadata/configuration/artifact hashes.
6. Read committed watermark where stateful.
7. Execute artifact and verify target commit.
8. Run blocking validation.
9. Commit watermark safely.
10. Mark run SUCCESS.
11. Mark queue SUCCESS.

Failure sequence:

1. Immediately write sanitized `ctl_error_log`.
2. Leave watermark unchanged.
3. Mark run FAILED.
4. Move queue to RETRY_WAIT or FAILED.
5. Preserve every attempt.

Use one consistent status vocabulary.

## 23. Idempotency, concurrency, recovery and observability

Implement and test atomic queue claiming, worker ownership, leases, heartbeats, abandoned-work recovery, attempt counts, retry limits/scheduling, stable logical-work IDs, deterministic idempotency keys, bounded scope, configuration snapshots/hashes, deterministic writes, business-key merges, safe replay, watermark compare-and-swap and protection against regression.

Never advance a watermark before target commit and blocking validation:

    Target commit
    → blocking validation
    → watermark commit
    → run SUCCESS
    → queue SUCCESS

A replay after a crash is acceptable only when writes are idempotent.

On execution exceptions, immediately log the correct safe stage such as CONNECT, DISCOVER, READ, PARSE, VALIDATE, TRANSFORM, WRITE or CHECKPOINT. Classify retryability, sanitize details and finalize run/queue status. Never persist credentials, authorization headers, private keys or sensitive payload contents.

## 24. Implementation phases and safe stops

Implement in small phases:

0. Inspection, architecture map, baseline tests and capability confirmation.
1. DDL/bootstrap, preflight and onboarding.
2. Target propagation and platform metadata repositories.
3. Nomination configuration reads and approved ingestion-object creation.
4. `ingestion_object_id` propagation through design artifacts.
5. Source-to-Bronze mapping, generation, registration and activation.
6. Bronze-to-Silver mapping, merge keys, generation and activation.
7. Silver-to-Gold multi-input mapping, generation and activation.
8. Queue/run/error/watermark lifecycle and target execution.
9. Cross-platform tests, security, recovery, regression and documentation.

At every phase, reuse first, keep diffs small and run focused tests.

Stop at the current phase boundary with an evidence-based handoff when the deployed schema conflicts, infrastructure/client is absent, the phase cannot be safely validated, continuation requires a broad rewrite/new dependency/schema change, a critical risk cannot be resolved or a user decision would materially change architecture. Do not continue into an invalid dependent phase.

## 25. Required tests

Cover at minimum:

- Existing BRD/KPI/HITL/UI/API behavior.
- Target selection through checkpoints and callbacks.
- Platform repository equivalence.
- Idempotent bootstrap/onboarding.
- Relationship and secret-reference validation.
- Nomination reading `cfg_connection`.
- Rejected tables creating no ingestion objects.
- Approved/reapproved tables creating no duplicates.
- `ingestion_object_id` in `ai_store` JSON without schema changes.
- Profiling statistics remaining outside `cfg_mapping`.
- Exact mapping-version selection and inactive-runtime rejection.
- Structured LLM validation and nonexistent field/join rejection.
- Bronze, Silver and Gold activation.
- Multi-input Gold facts/dimensions.
- Execution-spec validation, URI allow-list and hash mismatch rejection.
- Runtime independence from `ai_store` and KB.
- Concurrent queue claims, lease recovery and multiple attempts.
- Retry without duplicate data.
- Crash after target commit before watermark commit.
- Watermark non-regression.
- Secret absence from metadata, logs, prompts and errors.
- Databricks, Snowflake and Fabric contract tests.
- Live tests where environments are available.

## 26. Quality gate

Score the finished implementation using evidence:

- Functional correctness: 2.5
- Reliability and idempotency: 2.0
- Simplicity and maintainability: 1.5
- Security: 1.5
- Cross-platform portability: 1.0
- Backward compatibility and testing: 1.5

Required: at least 8.5/10, with no unresolved critical security, data-loss, concurrency, idempotency or correctness issue and no fabricated test result.

## 27. Completion states

Report separately:

### Implementation complete

- Code complete for declared platform scope.
- Static validation and locally executable tests pass.
- Documentation/prerequisites complete.
- No unresolved critical defect.

### Deployment validated

- Live target connectivity verified.
- Metadata deployed and schema-validated.
- Live read/write, transaction and concurrency tests pass.
- Generated artifacts execute successfully.
- Recovery behavior verified.

Never claim deployment validation from mocks. Missing infrastructure is a deployment-validation gap, not a locally passed test.

## 28. Final handoff

Provide:

1. Evidence-based architecture map.
2. Existing components reused.
3. Confirmed gaps and resolutions.
4. Files changed and why.
5. Final stage-by-stage metadata flow.
6. Metadata reads/writes by stage.
7. Platform-specific decisions.
8. LLM and KB guardrails.
9. Mapping version/activation lifecycle.
10. Artifact registration and verification.
11. Queue, retry, concurrency and recovery behavior.
12. Watermark semantics.
13. Security findings.
14. Exact tests and results.
15. Tests not run and why.
16. Implementation-complete status by platform.
17. Deployment-validated status by platform.
18. Remaining prerequisites/limitations.
19. Evidence-based quality score.

Do not declare completion until the implemented scope, tests and documentation satisfy the acceptance criteria.
