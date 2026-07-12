// @ts-nocheck
import React, { useEffect, useMemo, useRef, useState } from 'react'
import { motion } from 'framer-motion'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { AlertTriangle, CheckCircle, CheckCircle2, ChevronDown, ChevronRight, Copy, Database, Download, Inbox, KeyRound, Loader2, PlusCircle, RotateCcw, Send, Shield, Table2, Timer, XCircle } from 'lucide-react'
import useAthenaStore from '../store/useAthenaStore'
import KpiReviewCard from '../components/hitl/KpiReviewCard'
import EditKpiModal from '../components/hitl/EditKpiModal'
import SemanticReviewCard from '../components/hitl/SemanticReviewCard'
import {
  getBronzeReview,
  getEnrichmentReviews,
  fetchKpiReviews,
  getRun,
  getPipelineKpis,
  getSilverReview,
  getSilverMergeKeyReview,
  getTableReviews,
  submitBronzeReview,
  submitDecisions as submitHitlDecisions,
  submitEnrichmentReview,
  submitSilverMergeKeyReview,
  submitSilverReview,
  submitTableReviews
} from '../api/athenaApi'
import { MOCK_KPIS_LIST } from '../data/mockData'
import { ENABLE_DEMO_FALLBACKS, getDemoRuns, isDemoFallbackRun } from '../utils/demoFallbacks'
import { getGateDisplayName } from '../utils/pipelinePhases'

const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms))
const REVIEW_HYDRATION_ATTEMPTS = 20
const REVIEW_HYDRATION_DELAY_MS = 1000
const ENABLE_DEMO_REVIEW_FALLBACKS = ENABLE_DEMO_FALLBACKS

async function waitForNextReviewGate(runId, updateRun, targetGate, attempts = REVIEW_HYDRATION_ATTEMPTS) {
  let latest = null
  for (let index = 0; index < attempts; index += 1) {
    latest = await getRun(runId)
    updateRun(runId, latest)
    if (Number(latest?.next_gate || 0) === Number(targetGate)) return latest
    if (['FAILED', 'SUCCESS', 'COMPLETED', 'PIPELINE_COMPLETED'].includes(String(latest?.status || '').toUpperCase())) return latest
    if (index < attempts - 1) await sleep(REVIEW_HYDRATION_DELAY_MS)
  }
  return latest
}

function hasRenderableReviewData(review, gate, isFileSource) {
  if (!review) return false

  if (gate === 1) {
    if (Array.isArray(review)) return review.length > 0
    return Boolean((review?.kpis || []).length)
  }
  if (gate === 2) {
    return isFileSource
      ? Boolean(review?.candidate_feed) || Boolean((review?.candidate_feeds || []).length) || Number(review?.next_gate || 0) === 2
      : Boolean((review?.nominated_tables || []).length) || Number(review?.next_gate || 0) === 2
  }
  if (gate === 3) {
    return Boolean(
      (review?.enriched_columns || []).length ||
      (review?.enriched_joins || []).length ||
      (review?.feed_semantic_summary || []).length ||
      Object.keys(review?.enriched_metadata || {}).length ||
      Object.keys(review?.semantic_counts || {}).length ||
      (review?.pii_columns || []).length ||
      (review?.join_key_columns || []).length ||
      (review?.measure_columns || []).length ||
      review?.resume_message ||
      Number(review?.next_gate || 0) === 3
    )
  }
  if (gate === 4) return Boolean((review?.bronze_review_artifact?.feeds || []).length)
  if (gate === 'silver_merge_key_review') return Boolean((review?.silver_merge_key_review_artifact?.feeds || []).length)
  if (gate === 5) return Boolean((review?.silver_review_artifact?.items || []).length)
  return false
}

async function waitForRenderableReview(fetcher, gate, isFileSource = false, attempts = REVIEW_HYDRATION_ATTEMPTS) {
  let latest = null
  for (let index = 0; index < attempts; index += 1) {
    latest = await fetcher()
    if (hasRenderableReviewData(latest, gate, isFileSource)) return latest
    if (index < attempts - 1) {
      await sleep(REVIEW_HYDRATION_DELAY_MS)
    }
  }
  const error = new Error('Review data was not ready after ' + attempts + ' backend attempt' + (attempts !== 1 ? 's' : '') + '.')
  Object.assign(error, { code: 'REVIEW_NOT_READY', latest })
  throw error
}

function isSuccessfulRun(run) {
  return ['SUCCESS', 'COMPLETED', 'PIPELINE_COMPLETED'].includes(String(run?.status || '').toUpperCase())
}

function findPreviousSuccessfulRun(allRuns, currentRun, isFileSource) {
  const targetRunId = String(currentRun?.id || currentRun?.run_id || '')
  const candidates = [
    ...(allRuns || []),
    ...(ENABLE_DEMO_FALLBACKS ? getDemoRuns() : []),
  ]
    .filter((run) => run && String(run.id || run.run_id || '') !== targetRunId)
    .filter(isSuccessfulRun)
    .filter((run) => {
      const candidateIsFile = run?.source === 'sftp' || run?.source === 'adls_gen2'
      return candidateIsFile === Boolean(isFileSource)
    })

  candidates.sort((left, right) => {
    const leftTime = new Date(left?.completed_at || left?.started_at || 0).getTime()
    const rightTime = new Date(right?.completed_at || right?.started_at || 0).getTime()
    return rightTime - leftTime
  })

  return candidates[0] || null
}

function buildBronzeScriptFromRun(sourceRun, currentRun, isFileSource) {
  const entity = isFileSource ? (currentRun?.sftp_entity || 'transactions') : 'claims'
  const sourceName = sourceRun?.brd_filename || sourceRun?.id || 'successful_run'
  return [
    '-- Demo fallback Bronze Code Review artifact',
    '-- Reused pattern from previous successful run: ' + sourceName,
    'CREATE OR REPLACE TABLE bronze.' + entity + ' AS',
    'SELECT *',
    'FROM ' + (isFileSource ? 'landing.vendor1_feed' : 'source.claims') + ';',
  ].join('\n')
}

function buildSilverScriptFromRun(sourceRun, currentRun, isFileSource) {
  const entity = isFileSource ? (currentRun?.sftp_entity || 'transactions') : 'claims'
  const sourceName = sourceRun?.brd_filename || sourceRun?.id || 'successful_run'
  const sourceTable = 'bronze.' + entity
  const targetTable = 'silver.' + entity + '_curated'
  const quarantineTable = 'silver.' + entity + '_quarantine'
  const idColumn = isFileSource ? 'transaction_id' : 'claim_id'
  const dateColumn = isFileSource ? 'transaction_date' : 'loss_date'
  const amountColumn = isFileSource ? 'transaction_amount' : 'claim_amount'
  const statusColumn = isFileSource ? 'transaction_status' : 'claim_status'
  return [
    '# Silver transformation script',
    '# Pattern reused from previous successful run: ' + sourceName,
    '# Purpose: validate, standardize, deduplicate, quarantine bad records, and upsert curated Delta rows.',
    'from delta.tables import DeltaTable',
    'from pyspark.sql import Window',
    'from pyspark.sql import functions as F',
    '',
    'SOURCE_TABLE = "' + sourceTable + '"',
    'TARGET_TABLE = "' + targetTable + '"',
    'QUARANTINE_TABLE = "' + quarantineTable + '"',
    'BUSINESS_KEYS = ["' + idColumn + '", "policy_id"]',
    'WATERMARK_COLUMN = "_ingested_at"',
    '',
    'bronze_df = spark.table(SOURCE_TABLE)',
    '',
    'standardized_df = (',
    '    bronze_df',
    '    .withColumn("' + idColumn + '", F.trim(F.col("' + idColumn + '").cast("string")))',
    '    .withColumn("policy_id", F.trim(F.col("policy_id").cast("string")))',
    '    .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))',
    '    .withColumn("' + statusColumn + '", F.upper(F.trim(F.col("' + statusColumn + '").cast("string"))))',
    '    .withColumn("' + dateColumn + '", F.to_date(F.col("' + dateColumn + '")))',
    '    .withColumn("' + amountColumn + '", F.col("' + amountColumn + '").cast("decimal(18,2)"))',
    '    .withColumn("_source_system", F.coalesce(F.col("source_system"), F.lit("athena")))',
    '    .withColumn("_silver_loaded_at", F.current_timestamp())',
    ')',
    '',
    'valid_condition = (',
    '    F.col("' + idColumn + '").isNotNull()',
    '    & F.col("policy_id").isNotNull()',
    '    & F.col("' + dateColumn + '").isNotNull()',
    '    & (F.col("' + amountColumn + '") >= F.lit(0))',
    ')',
    '',
    'valid_df = standardized_df.filter(valid_condition)',
    'quarantine_df = (',
    '    standardized_df',
    '    .filter(~valid_condition)',
    '    .withColumn("quarantine_reason", F.lit("missing key/date or invalid amount"))',
    '    .withColumn("quarantined_at", F.current_timestamp())',
    ')',
    '',
    'dedupe_window = Window.partitionBy(*BUSINESS_KEYS).orderBy(F.col(WATERMARK_COLUMN).desc_nulls_last())',
    'curated_df = (',
    '    valid_df',
    '    .withColumn("_rn", F.row_number().over(dedupe_window))',
    '    .filter(F.col("_rn") == 1)',
    '    .drop("_rn")',
    '    .withColumn("silver_record_key", F.sha2(F.concat_ws("||", *[F.col(key) for key in BUSINESS_KEYS]), 256))',
    '    .withColumn("record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in valid_df.columns])), 256))',
    ')',
    '',
    'quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE_TABLE)',
    '',
    'if spark.catalog.tableExists(TARGET_TABLE):',
    '    (',
    '        DeltaTable.forName(spark, TARGET_TABLE).alias("target")',
    '        .merge(curated_df.alias("source"), "target.silver_record_key = source.silver_record_key")',
    '        .whenMatchedUpdateAll()',
    '        .whenNotMatchedInsertAll()',
    '        .execute()',
    '    )',
    'else:',
    '    curated_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)',
    '',
    'spark.sql(f"OPTIMIZE {TARGET_TABLE}")',
  ].join('\n')
}

function buildDemoGateFallback(run, gate, isFileSource, allRuns) {
  const runId = run?.id || run?.run_id || 'demo-run'
  const previousSuccessfulRun = findPreviousSuccessfulRun(allRuns, run, isFileSource)

  if (gate === 1) {
    return {
      kpis: MOCK_KPIS_LIST.slice(0, 5).map((item, index) => ({
        queue_id: runId + ':demo-kpi-' + (index + 1),
        item_id: item.id,
        run_id: runId,
        source: run?.source || 'database',
        item_type: 'METADATA',
        name: item.kpi_name,
        definition: item.kpi_description,
        category: 'Business KPI',
        domain: 'Demo',
        confidence: item.ai_confidence_score,
        status: 'PENDING_REVIEW',
        grounded: item.grounding_status === 'GROUNDING_STRONG',
        explicit: item.derivation_type === 'explicit',
        decision: null,
      })),
      runId,
      source: run?.source || 'database',
    }
  }

  if (gate === 2) {
    if (isFileSource) {
      return {
        candidate_feeds: [
          {
            vendor: 'Vendor1',
            entity: 'transactions',
            file_name: 'transactions_2026_06.csv',
            format: 'csv',
            sample_row_count: 12840,
            columns: ['transaction_id', 'policy_id', 'amount', 'transaction_date', 'status'],
            primary_keys: ['transaction_id'],
            measures: ['amount'],
            semantic_type: 'finance_feed',
            file_path: '/demo/adls/vendor1/transactions_2026_06.csv',
          },
          {
            vendor: 'Vendor1',
            entity: 'employee',
            file_name: 'employee_2026_06.csv',
            format: 'csv',
            sample_row_count: 245,
            columns: ['employee_id', 'branch', 'region', 'manager_id'],
            primary_keys: ['employee_id'],
            measures: [],
            semantic_type: 'reference_feed',
            file_path: '/demo/adls/vendor1/employee_2026_06.csv',
          },
        ],
        next_gate: 2,
        resume_message: 'Demo fallback Feed Review is ready.',
      }
    }

    return {
      nominated_tables: [
        { database_name: 'insurance', schema_name: 'dbo', table_name: 'claims', confidence_score: 0.94, coverage_ratio: 0.88, matched_keywords: ['claims', 'policy'], nomination_reason: 'Strong overlap with business requirements.' },
        { database_name: 'insurance', schema_name: 'dbo', table_name: 'policies', confidence_score: 0.92, coverage_ratio: 0.83, matched_keywords: ['policy', 'customer'], nomination_reason: 'Relevant master data for KPI derivation.' },
      ],
      next_gate: 2,
      resume_message: 'Demo fallback Table Review is ready.',
    }
  }

  if (gate === 3) {
    const buildTableColumns = (prefix, amountName = 'amount') => [
      {
        column_name: `${prefix}_id`,
        suggested_display_name: `${prefix} id`,
        semantic_type: 'ID',
        business_description: `Unique identifier assigned to each ${prefix.replace(/_/g, ' ')} record for audit and reconciliation.`,
        enrichment_source: 'LLM + profiling',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: false,
      },
      {
        column_name: 'policy_id',
        suggested_display_name: 'Policy Identifier',
        semantic_type: 'SURROGATE_KEY',
        business_description: 'Business key that links claims to active policy records and coverage attributes.',
        enrichment_source: 'LLM + join inference',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: false,
      },
      {
        column_name: 'customer_id',
        suggested_display_name: 'Customer Identifier',
        semantic_type: 'ID',
        business_description: 'Customer reference used to connect policy holder, claimant, and service interaction data.',
        enrichment_source: 'profiling',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: true,
      },
      {
        column_name: amountName,
        suggested_display_name: amountName.replace(/_/g, ' '),
        semantic_type: 'MEASURE',
        business_description: `Business amount used for analytics, settlement monitoring, and downstream KPI calculations.`,
        enrichment_source: 'LLM + numeric profiling',
        is_measure: true,
        is_dimension: false,
        is_pii_candidate: false,
      },
      {
        column_name: 'inserted_date',
        suggested_display_name: 'inserted date',
        semantic_type: 'DATE',
        business_description: 'Record insertion timestamp used for lineage, audit freshness, and batch observability.',
        enrichment_source: 'cache',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: false,
      },
      {
        column_name: 'status_code',
        suggested_display_name: 'status code',
        semantic_type: 'DIMENSION',
        business_description: 'Operational status value used to segment active, pending, completed, and exception records.',
        enrichment_source: 'domain dictionary',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: false,
      },
    ]

    const semanticTables = [
      {
        table_name: 'claims',
        columns: buildTableColumns('claim', 'claim_amount'),
        table_summary: 'Core claim transaction table covering claim lifecycle, approved amounts, policy linkage, and customer relationship.',
      },
      {
        table_name: 'policies',
        columns: buildTableColumns('policy', 'premium_amount'),
        table_summary: 'Policy master table used to connect coverage, premium, customer, and policy status attributes to claims.',
      },
      {
        table_name: 'customers',
        columns: buildTableColumns('customer', 'lifetime_value'),
        table_summary: 'Customer dimension with policy holder and claimant profile attributes for segmentation and service analytics.',
      },
      {
        table_name: 'expenses_outstanding_estimates',
        columns: buildTableColumns('expense', 'gross_estimate'),
        table_summary: 'Summarizes estimated outstanding expenses related to claims, including legal and administrative reserves.',
      },
      {
        table_name: 'indemnity_outstanding_estimates',
        columns: buildTableColumns('indemnity', 'reserve_amount'),
        table_summary: 'Tracks projected indemnity reserves for open claims and supports reserve adequacy analysis.',
      },
      {
        table_name: 'measures',
        columns: buildTableColumns('measure', 'metric_value'),
        table_summary: 'Curated metric definitions and calculation fields used by Gold KPI generation.',
      },
      {
        table_name: 'payments',
        columns: buildTableColumns('payment', 'paid_amount'),
        table_summary: 'Payment transaction table used for settlement tracking, leakage checks, and payout trend analysis.',
      },
      {
        table_name: 'adjusters',
        columns: buildTableColumns('adjuster', 'workload_score'),
        table_summary: 'Adjuster assignment and capacity table used for productivity, SLA, and workload analytics.',
      },
    ]
    const semanticColumns = semanticTables.flatMap((table) => table.columns.map((column) => ({ ...column, table_name: table.table_name })))

    return {
      enriched_columns: semanticColumns,
      semantic_tables: semanticTables,
      enriched_joins: [
        { left: 'claims.policy_id', right: 'policies.policy_id', confidence: 0.94, relationship: 'many_to_one' },
        { left: 'claims.customer_id', right: 'customers.customer_id', confidence: 0.89, relationship: 'many_to_one' },
      ],
      semantic_counts: { id: 8, surrogate_key: 8, measure: 8, dimension: 16, date: 8, pii: 8 },
      pii_columns: ['customer_id', 'adjuster_id'],
      join_key_columns: ['policy_id', 'customer_id'],
      measure_columns: semanticTables.map((table) => table.columns.find((column) => column.is_measure)?.column_name).filter(Boolean),
      feed_semantic_summary: isFileSource ? semanticTables.map((table) => ({ vendor: 'Vendor1', entity: table.table_name, format: 'csv', column_count: table.columns.length, pii_count: table.columns.filter((column) => column.is_pii_candidate).length, join_key_count: table.columns.filter((column) => column.semantic_type === 'SURROGATE_KEY').length, measure_count: table.columns.filter((column) => column.is_measure).length, semantic_counts: { id: 1, measure: 1, dimension: 2, date: 1 }, sample_row_count: 12840, enriched_columns: table.columns, table_summary: table.table_summary })) : [],
      enriched_metadata: {
        confidence: 0.91,
        stage: 'Semantic Enrichment',
        table_summary: 'Claims semantic model with policy, customer, amount, date, status, and investigation attributes prepared for Bronze generation.',
      },
      table_summary: 'Claims semantic model with policy, customer, amount, date, status, and investigation attributes prepared for Bronze generation.',
      next_gate: 3,
      resume_message: 'Semantic Review is ready. Column profiling, semantic enrichment, and join inference completed.',
    }
  }

  if (gate === 4) {
    const sourceRunName = previousSuccessfulRun?.brd_filename || previousSuccessfulRun?.id || 'demo_successful_run'
    return {
      bronze_review_artifact: {
        feeds: [
          {
            vendor: 'Vendor1',
            entity: isFileSource ? 'transactions' : 'claims',
            source_type: isFileSource ? 'adls_gen2' : 'database',
            file_format: isFileSource ? 'csv' : 'table',
            primary_keys: ['policy_id'],
            watermark_column: 'ingested_at',
            landing_path: '/demo/reused/' + sourceRunName + '/bronze/input',
            bronze_output_path: '/demo/reused/' + sourceRunName + '/bronze/output',
            checkpoint_path: '/demo/reused/' + sourceRunName + '/bronze/checkpoint',
            generated_bronze_script: buildBronzeScriptFromRun(previousSuccessfulRun, run, isFileSource),
          },
        ],
      },
      next_gate: 4,
      resume_message: 'Demo fallback Bronze Code Review is ready.',
    }
  }

  if (gate === 5) {
    return {
      silver_review_artifact: {
        items: [
          {
            entity: isFileSource ? 'transactions_curated' : 'claims_curated',
            bronze_source: 'bronze.' + (isFileSource ? (run?.sftp_entity || 'transactions') : 'claims'),
            transformations: ['standardize schema', 'deduplicate records'],
            type_casts: ['amount -> decimal(18,2)'],
            dq_rules: ['policy_id not null'],
            pii_masking_rules: [],
            processing_mode: 'delta_upsert',
            generated_silver_script: buildSilverScriptFromRun(previousSuccessfulRun, run, isFileSource),
          },
        ],
      },
      next_gate: 5,
      resume_message: 'Demo fallback Silver Code Review is ready.',
    }
  }

  return null
}

function HitlQueue() {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedRunId = searchParams.get('runId') || ''
  const {
    runs,
    activeRunId,
    hitlQueues,
    addNotification,
    submitDecisions: storeSubmitDecisions,
    addRun,
    updateRun,
    setHitlQueue,
    setHitlSourceRunId,
    setActiveRun
  } = useAthenaStore()

  const reviewRuns = useMemo(
    () =>
      runs.filter(isReviewGateAccessible),
    [runs]
  )

  const initialReviewRun =
    reviewRuns.find((run) => run.id === activeRunId) || null
  const [selectedRunId, setSelectedRunId] = useState(requestedRunId || activeRunId || initialReviewRun?.id || null)
  const [statusFilter, setStatusFilter] = useState('All')
  const [localDecisions, setLocalDecisions] = useState({})
  const [editedKpis, setEditedKpis] = useState({})
  const [editingKpi, setEditingKpi] = useState(null)
  const [rejectionReasons, setRejectionReasons] = useState({})
  const [submitting, setSubmitting] = useState(false)
  const [hydrating, setHydrating] = useState(false)
  const [tableReview, setTableReview] = useState(null)
  const [tableReviewDecisions, setTableReviewDecisions] = useState({})
  const [selectedTables, setSelectedTables] = useState({})
  const [enrichmentReview, setEnrichmentReview] = useState(null)
  const [semanticDecisions, setSemanticDecisions] = useState({})
  const [semanticDrafts, setSemanticDrafts] = useState({})
  const [semanticRejectionReasons, setSemanticRejectionReasons] = useState({})
  const [semanticValidationError, setSemanticValidationError] = useState('')
  const [bronzeReview, setBronzeReview] = useState(null)
  const [silverMergeKeyReview, setSilverMergeKeyReview] = useState(null)
  const [silverReview, setSilverReview] = useState(null)
  const [gate3Decision, setGate3Decision] = useState('APPROVED')
  const [gateDecision, setGateDecision] = useState('')
  const [codeReviewDecisions, setCodeReviewDecisions] = useState({})
  const [codeReviewDraftItems, setCodeReviewDraftItems] = useState([])
  const [selectedRunDetail, setSelectedRunDetail] = useState(null)
  const hydrationRequestRef = useRef(0)
  const reviewSessionKeyRef = useRef('')
  const requestedGate = Number(searchParams.get('gate') || 0)
  const requestedReviewKey = searchParams.get('review') || ''
  const shouldSuppressRequestedInitialReview = isSuppressedInitialReviewRun({
    id: requestedRunId,
    next_gate: requestedGate,
  })

  const REVIEWER_ID = 'reviewer@nousinfo.com'
  const currentRun = useMemo(() => {
    const summaryRun = runs.find((run) => run.id === selectedRunId) || null
    if (selectedRunDetail?.id === selectedRunId) {
      if (!summaryRun) return selectedRunDetail
      const merged = {
        ...selectedRunDetail,
        ...summaryRun,
      }
      if (!summaryRun?.pipeline_steps?.length && selectedRunDetail?.pipeline_steps?.length) {
        merged.pipeline_steps = selectedRunDetail.pipeline_steps
      }
      if (!summaryRun?.stages?.length && selectedRunDetail?.stages?.length) {
        merged.stages = selectedRunDetail.stages
      }
      return merged
    }
    return summaryRun
  }, [runs, selectedRunDetail, selectedRunId])
  const gateToReview = Number(requestedGate || (!requestedReviewKey ? currentRun?.next_gate : 0) || 0)
  const reviewKeyToReview = requestedReviewKey || (!requestedGate ? currentRun?.next_review_key : '') || ''
  const isReviewableRun = isReviewGateAccessible(currentRun) || (Boolean(selectedRunId) && (gateToReview > 0 || Boolean(reviewKeyToReview)))
  const isSilverMergeKeyReview = reviewKeyToReview === 'silver_merge_key_review'
  const isGate1 = gateToReview === 1
  const isGate2 = gateToReview === 2
  const isGate3 = gateToReview === 3
  const isGate4 = gateToReview === 4
  const isGate5 = gateToReview === 5
  const runSource = currentRun?.source || selectedRunDetail?.source || ''
  const isSftpRun = runSource === 'sftp' || runSource === 'adls_gen2'
  const gate1Name = getGateDisplayName(1)
  const gate2Name = getGateDisplayName(2, runSource)
  const gate3Name = getGateDisplayName(3)
  const gate4Name = getGateDisplayName(4)
  const gate5Name = getGateDisplayName(5)
  const activeReviewName = isSilverMergeKeyReview
    ? 'Silver Merge Key Review'
    : isGate5 ? gate5Name : isGate4 ? gate4Name : isGate3 ? gate3Name : isGate2 ? gate2Name : gate1Name
  const rawQueue = useMemo(
    () => (isGate1 ? hitlQueues[selectedRunId] || (currentRun?.kpis || []) : []),
    [currentRun?.kpis, hitlQueues, isGate1, selectedRunId]
  )
  const queue = useMemo(
    () => filterReviewQueue(rawQueue, selectedRunId, runSource),
    [rawQueue, selectedRunId, runSource]
  )

  useEffect(() => {
    if (!shouldSuppressRequestedInitialReview) return
    setSelectedRunId(null)
    setActiveRun(null)
    setSearchParams({})
    navigate('/app/data-discovery', { replace: true })
  }, [navigate, setActiveRun, setSearchParams, shouldSuppressRequestedInitialReview])

  useEffect(() => {
    if (shouldSuppressRequestedInitialReview) return
    if (!requestedRunId || requestedRunId === selectedRunId) return
    setSelectedRunId(requestedRunId)
    setActiveRun(requestedRunId)
    setTableReview(null)
    setEnrichmentReview(null)
    setBronzeReview(null)
    setSilverMergeKeyReview(null)
    setSilverReview(null)
    setSelectedTables({})
    setLocalDecisions({})
    setSemanticDrafts({})
  }, [requestedRunId, selectedRunId, setActiveRun, shouldSuppressRequestedInitialReview])

  useEffect(() => {
    if (selectedRunId || !activeRunId) return
    let cancelled = false

    const hydrateActiveRun = async () => {
      try {
        const detail = await getRun(activeRunId)
        if (cancelled || !detail?.id) return

        if (!isReviewGateAccessible(detail)) return

        const alreadyKnown = runs.some((run) => run.id === detail.id)
        if (alreadyKnown) updateRun(detail.id, detail)
        else addRun(detail)

        setSelectedRunDetail(detail)
        setSelectedRunId(detail.id)
      } catch (error) {
        if (!cancelled) {
          console.warn('[HitlQueue] Failed to hydrate active review run', error)
        }
      }
    }

    hydrateActiveRun()
    return () => {
      cancelled = true
    }
  }, [activeRunId, addRun, runs, selectedRunId, updateRun])

  useEffect(() => {
    if (!selectedRunId || currentRun) return
    let cancelled = false

    const hydrateSelectedRun = async () => {
      try {
        const detail = await getRun(selectedRunId)
        if (cancelled || !detail?.id) return
        if (!isReviewGateAccessible(detail)) {
          setSelectedRunId(null)
          if (isSuppressedInitialReviewRun(detail)) {
            setActiveRun(null)
            setSearchParams({})
            navigate('/app/data-discovery', { replace: true })
          }
          return
        }

        const alreadyKnown = runs.some((run) => run.id === detail.id)
        if (alreadyKnown) updateRun(detail.id, detail)
        else addRun(detail)

        setSelectedRunDetail(detail)
      } catch (error) {
        if (!cancelled) {
          console.warn('[HitlQueue] Failed to hydrate selected review run', error)
        }
      }
    }

    hydrateSelectedRun()
    return () => {
      cancelled = true
    }
  }, [addRun, currentRun, navigate, runs, selectedRunId, setActiveRun, setSearchParams, updateRun])

  useEffect(() => {
    if (!selectedRunId || !currentRun) return
    if (!requestedGate && !requestedReviewKey) return
    if (matchesRequestedReview(currentRun, requestedGate, requestedReviewKey)) return
    if (!isRunActivelyProcessing(currentRun)) return

    addNotification({
      type: 'info',
      title: 'Review no longer active',
      message: 'The pipeline moved to another stage. Returning to the monitor.',
      duration: 3500,
    })
    setActiveRun(selectedRunId)
    navigate('/app/data-discovery', {
      replace: true,
      state: { activeRunId: selectedRunId },
    })
  }, [addNotification, currentRun, navigate, requestedGate, requestedReviewKey, selectedRunId, setActiveRun])

  useEffect(() => {
    if (requestedRunId) return
    if (activeRunId && selectedRunId !== activeRunId) {
      setSelectedRunId(activeRunId)
      setTableReview(null)
      setEnrichmentReview(null)
      setBronzeReview(null)
      setSilverMergeKeyReview(null)
      setSilverReview(null)
      setSelectedTables({})
      setLocalDecisions({})
      setSemanticDrafts({})
      return
    }

    const selectedStillExists =
      selectedRunId &&
      (runs.some((run) => run.id === selectedRunId) || selectedRunDetail?.id === selectedRunId)
    const selectedNeedsReview = currentRun && isReviewableRun

    if (selectedStillExists && selectedNeedsReview) return

    if (selectedRunId && selectedRunId !== activeRunId) {
      setSelectedRunId(null)
      setTableReview(null)
      setEnrichmentReview(null)
      setBronzeReview(null)
      setSilverMergeKeyReview(null)
      setSilverReview(null)
      setSelectedTables({})
      setLocalDecisions({})
      setSemanticDrafts({})
    }
  }, [runs, selectedRunId, currentRun, isReviewableRun, activeRunId, selectedRunDetail?.id, requestedRunId])

  useEffect(() => {
    const nextSessionKey = `${selectedRunId || 'none'}:${gateToReview || 0}:${reviewKeyToReview || 'none'}`
    if (reviewSessionKeyRef.current === nextSessionKey) return

    reviewSessionKeyRef.current = nextSessionKey
    setTableReview(null)
    setEnrichmentReview(null)
    setSemanticDecisions({})
    setSemanticDrafts({})
    setSemanticRejectionReasons({})
    setSemanticValidationError('')
    setBronzeReview(null)
    setSilverMergeKeyReview(null)
    setSilverReview(null)
    setTableReviewDecisions({})
    setSelectedTables({})
    setGateDecision('')
    setCodeReviewDecisions({})
    setCodeReviewDraftItems([])
    hydrationRequestRef.current += 1
  }, [selectedRunId, gateToReview, reviewKeyToReview])

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (!selectedRunId) return
    let cancelled = false
    const requestId = ++hydrationRequestRef.current
    const isCurrentHydration = () => !cancelled && requestId === hydrationRequestRef.current

    const hydrate = async () => {
      setHydrating(true)
      try {
        if (isDemoFallbackRun(currentRun) && ENABLE_DEMO_REVIEW_FALLBACKS) {
          const fallbackPatch = {
            ...buildDemoGateFallback(currentRun, gateToReview || 1, isSftpRun, runs),
            demo_review_fallback: true,
            review_fallback_reason: 'Backend review hydration was skipped for the saved demo run.',
          }

          if (gateToReview === 3) {
            setEnrichmentReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 4) {
            setBronzeReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (isSilverMergeKeyReview) {
            setSilverMergeKeyReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 5) {
            setSilverReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 2) {
            setTableReview(fallbackPatch)
            setSelectedTables((prev) => {
              const next = { ...prev }
              const items = isSftpRun ? getSftpFeeds(fallbackPatch) : (fallbackPatch.nominated_tables || [])
              for (const table of items) {
                const key = isSftpRun ? sftpFeedKey(table) : tableReviewKey(table)
                next[key] = true
              }
              return next
            })
            updateRun(selectedRunId, fallbackPatch)
          } else {
            const mappedDemoKpis = (fallbackPatch.kpis || []).map(mapHitlRow)
            setHitlQueue(selectedRunId, mappedDemoKpis)
            updateRun(selectedRunId, {
              kpis: mappedDemoKpis,
              next_gate: 1,
              resume_message: 'Demo fallback KPI review is ready.',
              demo_review_fallback: true,
              review_fallback_reason: fallbackPatch.review_fallback_reason,
            })
          }

          return
        }

        if (isGate3) {
          const review = await waitForRenderableReview(() => getEnrichmentReviews(selectedRunId), 3)
          if (!isCurrentHydration()) return
          if (!reviewPayloadMatchesRun(review, selectedRunId, runSource)) return
          setEnrichmentReview(review)
          setSemanticDrafts({})
          setGate3Decision('APPROVED')
          updateRun(selectedRunId, {
            enriched_metadata: review.enriched_metadata || {},
            enriched_columns: review.enriched_columns || [],
            enriched_joins: review.enriched_joins || [],
            semantic_counts: review.semantic_counts || {},
            pii_columns: review.pii_columns || [],
            join_key_columns: review.join_key_columns || [],
            measure_columns: review.measure_columns || [],
            feed_semantic_summary: review.feed_semantic_summary || [],
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            gate3_approved: review.gate3_approved
          })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 3, source: runSource } }))
          return
        }

        if (isGate4) {
          const review = await waitForRenderableReview(() => getBronzeReview(selectedRunId), 4)
          if (!isCurrentHydration()) return
          if (!reviewPayloadMatchesRun(review, selectedRunId, runSource)) return
          setBronzeReview(review)
          updateRun(selectedRunId, {
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            bronze_review_artifact: review.bronze_review_artifact || {}
          })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 4, source: runSource } }))
          return
        }

        if (isSilverMergeKeyReview) {
          const review = await waitForRenderableReview(() => getSilverMergeKeyReview(selectedRunId), 'silver_merge_key_review')
          if (!isCurrentHydration()) return
          if (!reviewPayloadMatchesRun(review, selectedRunId, runSource)) return
          setSilverMergeKeyReview(review)
          updateRun(selectedRunId, {
            next_gate: review.next_gate,
            next_review_key: review.next_review_key,
            resume_message: review.resume_message,
            silver_merge_key_review_artifact: review.silver_merge_key_review_artifact || {}
          })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, review: 'silver_merge_key_review', source: runSource } }))
          return
        }

        if (isGate5) {
          const review = await waitForRenderableReview(() => getSilverReview(selectedRunId), 5)
          if (!isCurrentHydration()) return
          if (!reviewPayloadMatchesRun(review, selectedRunId, runSource)) return
          setSilverReview(review)
          updateRun(selectedRunId, {
            next_gate: review.next_gate,
            resume_message: review.resume_message,
            silver_review_artifact: review.silver_review_artifact || {}
          })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 5, source: runSource } }))
          return
        }

        if (isGate2) {
          const review = await waitForRenderableReview(() => getTableReviews(selectedRunId), 2, isSftpRun)
          if (!isCurrentHydration()) return
          if (!reviewPayloadMatchesRun(review, selectedRunId, runSource)) return
          setTableReview(review)
          setSelectedTables((prev) => {
            const next = { ...prev }
            const items = isSftpRun ? getSftpFeeds(review) : (review.nominated_tables || [])
            for (const table of items) {
              const key = isSftpRun ? sftpFeedKey(table) : tableReviewKey(table)
              if (!(key in next)) next[key] = true
            }
            return next
          })
          updateRun(selectedRunId, {
            nominated_tables: review.nominated_tables || [],
            certified_tables: review.certified_tables || [],
            candidate_feed: review.candidate_feed || null,
            candidate_feeds: review.candidate_feeds || [],
            next_gate: review.next_gate,
            resume_message: review.resume_message
          })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 2, source: runSource } }))
          return
        }

        let expectedSource = runSource
        if (!expectedSource) {
          try {
            const detail = await getRun(selectedRunId)
            if (!isCurrentHydration()) return
            expectedSource = detail?.source || expectedSource
            if (detail?.id) updateRun(selectedRunId, detail)
          } catch {
            // KPI review can still be validated by run_id when source is absent from the run summary.
          }
        }

        const reviewData = await waitForRenderableReview(() => fetchKpiReviews(selectedRunId), 1)
        if (!isCurrentHydration()) return
        if (!reviewPayloadMatchesRun(reviewData, selectedRunId, expectedSource)) {
          setHitlQueue(selectedRunId, [])
          updateRun(selectedRunId, { kpis: [] })
          addNotification({
            type: 'error',
            title: 'Review Source Mismatch',
            message: 'Blocked KPI review data because it does not match the selected run source.',
            duration: 5000
          })
          return
        }
        if (reviewData.kpis && reviewData.kpis.length > 0) {
          const mapped = filterReviewQueue(reviewData.kpis.map(mapHitlRow), selectedRunId, expectedSource)
          setHitlQueue(selectedRunId, mapped)
          setHitlSourceRunId(selectedRunId, selectedRunId)
          updateRun(selectedRunId, { kpis: mapped })
          window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 1, source: expectedSource } }))
          return
        }

        const fallback = await getPipelineKpis(selectedRunId)
        if (!isCurrentHydration()) return
        if (!reviewPayloadMatchesRun(fallback, selectedRunId, expectedSource)) {
          setHitlQueue(selectedRunId, [])
          updateRun(selectedRunId, { kpis: [] })
          return
        }
        const fallbackKpis = filterReviewQueue((fallback.kpis || []).map(mapHitlRow), selectedRunId, expectedSource)
        setHitlQueue(selectedRunId, fallbackKpis)
        setHitlSourceRunId(selectedRunId, fallback.runId)
        updateRun(selectedRunId, { kpis: fallbackKpis, kpi_source_run_id: fallback.runId })
        window.dispatchEvent(new CustomEvent('athena:review-gate-ready', { detail: { runId: selectedRunId, gate: 1, source: expectedSource } }))
      } catch (error) {
        if (!isCurrentHydration()) return
        const demoFallback = ENABLE_DEMO_REVIEW_FALLBACKS && isDemoFallbackRun(currentRun)
          ? buildDemoGateFallback(currentRun, gateToReview || 1, isSftpRun, runs)
          : null
        if (demoFallback) {
          const fallbackPatch = {
            ...demoFallback,
            demo_review_fallback: true,
            review_fallback_reason: error.message || 'Backend review data did not load in time.',
          }
          if (gateToReview === 3) {
            setEnrichmentReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 4) {
            setBronzeReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 5) {
            setSilverReview(fallbackPatch)
            updateRun(selectedRunId, fallbackPatch)
          } else if (gateToReview === 2) {
            setTableReview(fallbackPatch)
            setSelectedTables((prev) => {
              const next = { ...prev }
              const items = isSftpRun ? getSftpFeeds(fallbackPatch) : (fallbackPatch.nominated_tables || [])
              for (const table of items) {
                const key = isSftpRun ? sftpFeedKey(table) : tableReviewKey(table)
                next[key] = true
              }
              return next
            })
            updateRun(selectedRunId, fallbackPatch)
          } else {
            const mappedDemoKpis = (fallbackPatch.kpis || []).map(mapHitlRow)
            setHitlQueue(selectedRunId, mappedDemoKpis)
            updateRun(selectedRunId, { kpis: mappedDemoKpis, resume_message: 'Demo fallback KPI review is ready.', demo_review_fallback: true, review_fallback_reason: fallbackPatch.review_fallback_reason })
          }

          addNotification({
            type: 'amber',
            title: 'Demo Fallback Used',
            message: activeReviewName + ' loaded fallback content after backend review data failed.',
            duration: 7000
          })
          return
        }

        addNotification({
          type: 'error',
          title: activeReviewName + ' Load Failed',
          message: error.message || (isGate2 ? 'Unable to load table review data.' : isGate3 ? 'Unable to load column review data.' : isGate4 ? 'Unable to load Bronze review data.' : isSilverMergeKeyReview ? 'Unable to load Silver merge-key review data.' : isGate5 ? 'Unable to load Silver review data.' : 'Unable to load KPI review data.'),
          duration: 5000
        })
      } finally {
        if (isCurrentHydration()) setHydrating(false)
      }
    }

    hydrate()
    return () => {
      cancelled = true
    }
    // Hydration is keyed by run, gate, and source. Full currentRun/runs objects would restart
    // in-flight review requests after every store merge.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRunId, isGate2, isGate3, isGate4, isGate5, isSilverMergeKeyReview, activeReviewName, gate1Name, gate2Name, gate3Name, gate4Name, gate5Name, isSftpRun, setHitlQueue, setHitlSourceRunId, updateRun, addNotification, runSource])

  const filteredQueue = useMemo(() => {
    if (statusFilter === 'All') return queue
    return queue.filter((item) => {
      const decision = localDecisions[reviewItemKey(item)] || item.decision
      if (statusFilter === 'Pending') return !decision
      return decision === statusFilter
    })
  }, [queue, statusFilter, localDecisions])

  const kpiCounts = useMemo(() => ({
    total: queue.length,
    pending: queue.filter((item) => !localDecisions[reviewItemKey(item)] && !item.decision).length,
    approved: Object.values(localDecisions).filter((value) => value === 'APPROVED').length,
    edited: Object.values(localDecisions).filter((value) => value === 'EDITED').length,
    rejected: Object.values(localDecisions).filter((value) => value === 'REJECTED').length
  }), [queue, localDecisions])

  const selectedTableCount = (tableReview?.nominated_tables || []).filter((table) => selectedTables[tableReviewKey(table)]).length
  const availableSftpFeeds = getSftpFeeds(tableReview)
  const availableTableReviews = tableReview?.nominated_tables || []
  const reviewedTableCount = availableTableReviews.filter((table) => tableReviewDecisions[tableReviewKey(table)]).length
  const selectedFeedCount = availableSftpFeeds.filter((feed) => selectedTables[sftpFeedKey(feed)]).length
  const totalFeedCount = availableSftpFeeds.length
  const bronzeReviewFeeds = useMemo(
    () => bronzeReview?.bronze_review_artifact?.feeds || [],
    [bronzeReview]
  )
  const silverReviewItems = useMemo(
    () => silverReview?.silver_review_artifact?.items || [],
    [silverReview]
  )
  const silverMergeKeyReviewFeeds = useMemo(
    () => silverMergeKeyReview?.silver_merge_key_review_artifact?.feeds || [],
    [silverMergeKeyReview]
  )
  const bronzeCodeReviewItems = useMemo(
    () => buildBronzeCodeReviewItems(bronzeReviewFeeds),
    [bronzeReviewFeeds]
  )
  const silverMergeKeyReviewItems = useMemo(
    () => buildSilverMergeKeyReviewItems(silverMergeKeyReviewFeeds),
    [silverMergeKeyReviewFeeds]
  )
  const silverCodeReviewItems = useMemo(
    () => buildSilverCodeReviewItems(silverReviewItems),
    [silverReviewItems]
  )
  const activeCodeReviewItems = isGate4 ? bronzeCodeReviewItems : isSilverMergeKeyReview ? silverMergeKeyReviewItems : isGate5 ? silverCodeReviewItems : []
  const reviewedCodeReviewCount = activeCodeReviewItems.filter((item) => codeReviewDecisions[item.key]).length
  const codeReviewGateDecision = getCodeReviewGateDecision(activeCodeReviewItems, codeReviewDecisions)
  const semanticReviewSource = useMemo(
    () => buildSemanticReviewSource(enrichmentReview, currentRun, selectedRunId),
    [enrichmentReview, currentRun, selectedRunId]
  )
  const semanticReviewItems = useMemo(
    () => toAthenaSemanticItems(semanticReviewSource, isSftpRun, selectedRunId),
    [semanticReviewSource, isSftpRun, selectedRunId]
  )
  const pendingSemanticReviewItems = semanticReviewItems.filter((item) => {
    const key = semanticReviewItemKey(item)
    return !semanticDecisions[key] && !item.decision
  })
  const allSemanticReviewed = semanticReviewItems.length > 0 && pendingSemanticReviewItems.length === 0
  const gateReviewReady = isGate4 ? bronzeReviewFeeds.length > 0 : isSilverMergeKeyReview ? silverMergeKeyReviewFeeds.length > 0 : isGate5 ? silverReviewItems.length > 0 : false
  const canSubmitReview = isReviewableRun && (isGate2
    ? (isSftpRun ? totalFeedCount > 0 : (tableReview?.nominated_tables || []).length > 0)
    : isGate3
    ? true
    : (isGate4 || isSilverMergeKeyReview || isGate5)
    ? true
    : queue.length > 0)

  const returnToMonitor = (runId) => {
    if (runId) setActiveRun(runId)
    setSelectedRunDetail(null)
    navigate('/app/data-discovery', {
      replace: true,
      state: runId ? { activeRunId: runId } : null,
    })
  }

  const openReviewGate = (runId, gate) => {
    if (!runId) return
    setSelectedRunId(runId)
    setActiveRun(runId)
    setSearchParams({ runId, gate: String(gate || '') })
  }

  const selectReviewRun = (runId) => {
    setSelectedRunId(runId)
    setActiveRun(runId)
    if (runId) {
      setSearchParams({ runId })
    } else {
      setSearchParams({})
    }
  }

  const shouldRedirectDemoKpiReview = isSuppressedInitialReviewRun(currentRun)

  useEffect(() => {
    if (!shouldRedirectDemoKpiReview) return
    setSelectedRunId(null)
    setSearchParams({})
    setActiveRun(null)
    navigate('/app/data-discovery', { replace: true })
  }, [navigate, setActiveRun, setSearchParams, shouldRedirectDemoKpiReview])

  const refreshRunAfterSubmitError = async (fallbackMessage) => {
    if (!selectedRunId) return null
    try {
      const refreshed = await getRun(selectedRunId)
      updateRun(selectedRunId, refreshed)
      return refreshed
    } catch {
      updateRun(selectedRunId, {
        id: selectedRunId,
        resume_message: fallbackMessage,
      })
      return null
    }
  }

  const handleApprove = (kpiId) => {
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'APPROVED' }))
  }

  const handleReject = (kpiId, reason) => {
    setRejectionReasons((prev) => ({ ...prev, [kpiId]: reason }))
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'REJECTED' }))
  }

  const handleEdit = (kpi) => {
    setEditingKpi(kpi)
  }

  const handleSaveEdit = (kpiId, updates) => {
    setLocalDecisions((prev) => ({ ...prev, [kpiId]: 'EDITED' }))
    setEditedKpis((prev) => ({ ...prev, [kpiId]: updates }))
    setEditingKpi(null)
  }

  const handleClearDecision = (kpiId) => {
    setLocalDecisions((prev) => {
      const next = { ...prev }
      delete next[kpiId]
      return next
    })
  }

  const handleAutoApproveAll = () => {
    const next = {}
    queue.forEach((item) => {
      if (!item.decision) next[reviewItemKey(item)] = 'APPROVED'
    })
    setLocalDecisions((prev) => ({ ...prev, ...next }))
  }

  const buildApprovedKpiDecisions = () => {
    return queue.map((item) => {
      const key = reviewItemKey(item)
      const decision = localDecisions[key] || item.decision || 'APPROVED'
      const edited = editedKpis[key]
      return {
        kpi_id: key,
        decision,
        reviewer: REVIEWER_ID,
        notes: edited?.notes || rejectionReasons[key] || '',
        edited_definition: edited?.definition || null
      }
    })
  }

  const setCodeReviewDecision = (itemKey, decision) => {
    if (!itemKey) return
    setCodeReviewDecisions((prev) => ({ ...prev, [itemKey]: decision }))
    setGateDecision('')
  }

  const handleAutoApproveCodeReviewItems = () => {
    setCodeReviewDecisions((prev) => {
      const next = { ...prev }
      activeCodeReviewItems.forEach((item) => {
        if (!next[item.key]) next[item.key] = 'APPROVED'
      })
      return next
    })
    setGateDecision('')
  }

  const setAllCodeReviewItemsDecision = (decision) => {
    const next = {}
    activeCodeReviewItems.forEach((item) => {
      next[item.key] = decision
    })
    setCodeReviewDecisions((prev) => ({ ...prev, ...next }))
    setGateDecision(decision)
  }

  const handleApproveSemanticItem = (id, draft) => {
    setSemanticValidationError('')
    if (draft) handleSemanticDraftChange(id, draft)
    setSemanticDecisions((prev) => ({ ...prev, [id]: 'APPROVED' }))
  }

  const handleSemanticDraftChange = (id, draft) => {
    if (!id) return
    setSemanticDrafts((prev) => ({ ...prev, [id]: draft }))
  }

  const handleRejectSemanticItem = (id, reason) => {
    setSemanticValidationError('')
    setSemanticRejectionReasons((prev) => ({ ...prev, [id]: reason || 'Rejected by reviewer' }))
    setSemanticDecisions((prev) => ({ ...prev, [id]: 'REJECTED' }))
  }

  const handleAutoApproveSemanticItems = () => {
    const next = {}
    semanticReviewItems.forEach((item) => {
      const key = semanticReviewItemKey(item)
      if (!semanticDecisions[key] && !item.decision) next[key] = 'APPROVED'
    })
    setSemanticValidationError('')
    setSemanticDecisions((prev) => ({ ...prev, ...next }))
  }

  const buildApprovedSemanticDecisions = () => {
    const next = {}
    semanticReviewItems.forEach((item) => {
      const key = semanticReviewItemKey(item)
      next[key] = semanticDecisions[key] || item.decision || 'APPROVED'
    })
    return next
  }

  const buildEditedEnrichmentMetadata = () => {
    const baseMetadata = semanticReviewSource?.enriched_metadata || enrichmentReview?.enriched_metadata || {}
    const baseColumns = Array.isArray(baseMetadata?.columns)
      ? baseMetadata.columns
      : Array.isArray(semanticReviewSource?.enriched_columns)
      ? semanticReviewSource.enriched_columns
      : []
    const draftByColumn = new Map()
    const tableSummaries = { ...(baseMetadata?.table_summaries || {}) }

    semanticReviewItems.forEach((item) => {
      const key = semanticReviewItemKey(item)
      const draft = semanticDrafts[key] || item.item_detail || {}
      const tableName = draft.table_name || item.item_detail?.table_name || item.item_id || ''
      if (tableName) tableSummaries[tableName] = draft.table_summary || item.item_detail?.table_summary || ''
      ;(draft.columns || []).forEach((column) => {
        const columnName = column.column_name || column.name || column.column
        if (!columnName) return
        draftByColumn.set(`${tableName}.${columnName}`, { ...column, table_name: tableName })
      })
    })

    const mergedColumns = baseColumns.length
      ? baseColumns.map((column) => {
          const tableName = semanticColumnTableName(column)
          const columnName = column.column_name || column.name || column.column
          const draft = draftByColumn.get(`${tableName}.${columnName}`)
          return draft ? { ...column, ...draft, table_name: column.table_name || tableName } : column
        })
      : Array.from(draftByColumn.values())

    return {
      ...baseMetadata,
      run_id: baseMetadata?.run_id || semanticReviewSource?.run_id || selectedRunId,
      columns: mergedColumns,
      table_summaries: tableSummaries,
    }
  }

  const handleSelectAllTables = () => {
    setSelectedTables((prev) => {
      const next = { ...prev }
      for (const table of availableTableReviews) {
        next[tableReviewKey(table)] = true
      }
      return next
    })
  }

  const handleAutoApproveTables = () => {
    const selected = {}
    const decisions = {}
    for (const table of availableTableReviews) {
      const key = tableReviewKey(table)
      selected[key] = true
      decisions[key] = 'APPROVED'
    }
    setSelectedTables((prev) => ({ ...prev, ...selected }))
    setTableReviewDecisions((prev) => ({ ...prev, ...decisions }))
  }

  const handleApproveTableReview = (table) => {
    const key = tableReviewKey(table)
    setTableReviewDecisions((prev) => ({ ...prev, [key]: 'APPROVED' }))
    setSelectedTables((prev) => ({ ...prev, [key]: true }))
  }

  const handleRejectTableReview = (table) => {
    const key = tableReviewKey(table)
    setTableReviewDecisions((prev) => ({ ...prev, [key]: 'REJECTED' }))
    setSelectedTables((prev) => {
      const next = { ...prev }
      delete next[key]
      return next
    })
  }

  const handleSelectAllFeeds = () => {
    const next = {}
    for (const feed of availableSftpFeeds) {
      next[sftpFeedKey(feed)] = true
    }
    setSelectedTables(next)
  }

  const selectedOrAllTableKeys = () => {
    const keys = (tableReview?.nominated_tables || []).map((table) => tableReviewKey(table))
    const selected = keys.filter((key) => selectedTables[key])
    return selected.length > 0 ? selected : keys
  }

  const selectedOrAllFeedKeys = () => {
    const keys = availableSftpFeeds.map((feed) => sftpFeedKey(feed))
    const selected = keys.filter((key) => selectedTables[key])
    return selected.length > 0 ? selected : keys
  }

  const handleSubmit = async () => {
    if (isGate2) {
      setSubmitting(true)
      try {
        const approvedTables = isSftpRun ? selectedOrAllFeedKeys() : selectedOrAllTableKeys()
        if (isSftpRun) handleSelectAllFeeds()
        else handleAutoApproveTables()
        await submitTableReviews(selectedRunId, approvedTables)
        updateRun(selectedRunId, {
            id: selectedRunId,
            status: 'PROCESSING',
            next_gate: 0,
            stage_confirmation: null,
            kpis: [],
          background_stage: isSftpRun ? 'schema' : 'discovery',
          resume_message: `${gate2Name} submitted. Metadata discovery is starting.`,
        })
        setTableReview(null)
        setSelectedTables({})
        addNotification({
          type: 'success',
          title: `${gate2Name} Submitted`,
          message: isSftpRun
            ? `Approved feeds were submitted for ${gate2Name}.`
            : 'Approved tables were submitted. Metadata discovery and profiling are resuming.',
          duration: 5000
        })
        const refreshed = await waitForNextReviewGate(selectedRunId, updateRun, 3)
        if (Number(refreshed?.next_gate || 0) === 3) {
          openReviewGate(selectedRunId, 3)
        } else {
          returnToMonitor(selectedRunId)
        }
      } catch (error) {
        await refreshRunAfterSubmitError(`${gate2Name} submit did not complete. Waiting on backend state.`)
        addNotification({
          type: 'error',
          title: `${gate2Name} Submit Failed`,
          message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate3) {
      setSubmitting(true)
      try {
        const nextSemanticDecisions = buildApprovedSemanticDecisions()
        setSemanticValidationError('')
        setSemanticDecisions(nextSemanticDecisions)
        const hasRejectedSemanticItem = semanticReviewItems.some((item) => {
          const key = semanticReviewItemKey(item)
          return nextSemanticDecisions[key] === 'REJECTED'
        })
        const editedEnrichmentMetadata = hasRejectedSemanticItem ? undefined : buildEditedEnrichmentMetadata()
        await submitEnrichmentReview(selectedRunId, !hasRejectedSemanticItem, editedEnrichmentMetadata)
        updateRun(selectedRunId, {
            id: selectedRunId,
            status: 'RUNNING',
            next_gate: 0,
            stage_confirmation: null,
            background_stage: 'bronze',
          resume_message: `${gate3Name} submitted. Bronze generation is starting.`,
        })
        setEnrichmentReview(null)
        setSemanticDecisions({})
        setSemanticDrafts({})
        setSemanticRejectionReasons({})
        setSemanticValidationError('')
        addNotification({
          type: 'success',
          title: `${gate3Name} Submitted`,
          message: !hasRejectedSemanticItem
            ? `${gate3Name} approved. Bronze generation is running in the background.`
            : 'Enrichment review was rejected and the run remains paused for rework.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        await refreshRunAfterSubmitError(`${gate3Name} submit did not complete. Waiting on backend state.`)
        addNotification({
          type: 'error',
          title: `${gate3Name} Submit Failed`,
          message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate4) {
      setSubmitting(true)
      try {
        if (!bronzeCodeReviewItems.length) {
          throw new Error(`${gate4Name} is not ready yet. Bronze scripts are still being generated.`)
        }
        const reviewAction = codeReviewGateDecision || gateDecision || 'APPROVED'
        await submitBronzeReview(selectedRunId, reviewAction, buildCodeReviewArtifact('bronze', codeReviewDraftItems, bronzeReview, codeReviewDecisions))
        updateRun(selectedRunId, {
          id: selectedRunId,
            status: 'RUNNING',
            next_gate: 0,
            next_review_key: null,
            stage_confirmation: null,
            background_stage: reviewAction === 'APPROVED' ? 'bronze_code_execution' : undefined,
          resume_message: reviewAction === 'APPROVED'
            ? 'Bronze review submitted. Bronze execution is starting.'
            : 'Bronze review was submitted.',
        })
        setBronzeReview(null)
        setCodeReviewDraftItems([])
        addNotification({
          type: 'success',
          title: `${gate4Name} Submitted`,
          message: 'Bronze review was submitted. Pipeline is resuming.',
          duration: 5000
        })
        setSilverMergeKeyReview(null)
        returnToMonitor(selectedRunId)
      } catch (error) {
        await refreshRunAfterSubmitError(`${gate4Name} submit did not complete. Waiting on backend state.`)
        addNotification({
          type: 'error',
          title: `${gate4Name} Submit Failed`,
          message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isSilverMergeKeyReview) {
      setSubmitting(true)
      try {
        if (!silverMergeKeyReviewItems.length) {
          throw new Error('Silver Merge Key Review is not ready yet. Merge-key review data is still being prepared.')
        }
        const reviewAction = codeReviewGateDecision || gateDecision || 'APPROVED'
        await submitSilverMergeKeyReview(selectedRunId, reviewAction, buildCodeReviewArtifact('silver_merge_key', codeReviewDraftItems, silverMergeKeyReview, codeReviewDecisions))
        updateRun(selectedRunId, {
          id: selectedRunId,
            status: 'RUNNING',
            next_gate: 0,
            next_review_key: null,
            stage_confirmation: null,
            background_stage: reviewAction === 'APPROVED' ? 'silver' : undefined,
          resume_message: reviewAction === 'APPROVED'
            ? 'Silver Merge Key Review submitted. Silver generation is starting.'
            : 'Silver Merge Key Review was submitted.',
        })
        setSilverMergeKeyReview(null)
        setCodeReviewDraftItems([])
        addNotification({
          type: 'success',
          title: 'Silver Merge Key Review Submitted',
          message: 'Silver Merge Key Review was submitted. Pipeline is resuming.',
          duration: 5000
        })
        setSilverReview(null)
        returnToMonitor(selectedRunId)
      } catch (error) {
        await refreshRunAfterSubmitError('Silver Merge Key Review submit did not complete. Waiting on backend state.')
        addNotification({
          type: 'error',
          title: 'Silver Merge Key Review Submit Failed',
          message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } finally {
        setSubmitting(false)
      }
      return
    }

    if (isGate5) {
      setSubmitting(true)
      try {
        if (!silverCodeReviewItems.length) {
          throw new Error(`${gate5Name} is not ready yet. Silver scripts are still being generated.`)
        }
        const reviewAction = codeReviewGateDecision || gateDecision || 'APPROVED'
        await submitSilverReview(selectedRunId, reviewAction, buildCodeReviewArtifact('silver', codeReviewDraftItems, silverReview, codeReviewDecisions))
        updateRun(selectedRunId, {
            id: selectedRunId,
            status: 'RUNNING',
            next_gate: 0,
            next_review_key: null,
            stage_confirmation: null,
            background_stage: reviewAction === 'APPROVED' ? 'silver_code_execution' : undefined,
          resume_message: reviewAction === 'APPROVED'
            ? `${gate5Name} submitted. Silver execution is starting.`
            : `${gate5Name} was submitted.`,
        })
        setSilverReview(null)
        setCodeReviewDraftItems([])
        addNotification({
          type: 'success',
          title: `${gate5Name} Submitted`,
          message: 'Silver review was submitted. Pipeline is resuming.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } catch (error) {
        await refreshRunAfterSubmitError(`${gate5Name} submit did not complete. Waiting on backend state.`)
        addNotification({
          type: 'error',
          title: `${gate5Name} Submit Failed`,
          message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
          duration: 5000
        })
        returnToMonitor(selectedRunId)
      } finally {
        setSubmitting(false)
      }
      return
    }

    setSubmitting(true)
    const hasQueueIds = queue.some((item) => item.queue_id)

    try {
      const decisions = buildApprovedKpiDecisions()
      setLocalDecisions((prev) => ({
        ...prev,
        ...Object.fromEntries(decisions.map((decision) => [decision.kpi_id, decision.decision]))
      }))

      if (hasQueueIds) {
        await submitHitlDecisions(selectedRunId, decisions)
      }
      storeSubmitDecisions(selectedRunId, decisions)
      const refreshed = hasQueueIds ? await getRun(selectedRunId) : null
      updateRun(selectedRunId, refreshed || { status: 'RUNNING' })
      updateRun(selectedRunId, { kpis: [] })
      setLocalDecisions({})
      setEditedKpis({})
      setRejectionReasons({})

      if (!hasQueueIds) {
        addNotification({
          type: 'amber',
          title: 'Decisions Recorded Locally',
          message: 'KPIs were loaded from fallback data. Database update was skipped.',
          duration: 5000
        })
      } else {
        addNotification({
          type: 'success',
          title: 'Decisions Saved',
          message: `${decisions.length} KPI decision${decisions.length !== 1 ? 's' : ''} saved. Pipeline resuming.`,
          duration: 5000
        })
      }
      returnToMonitor(selectedRunId)
    } catch (error) {
      await refreshRunAfterSubmitError(`${gate1Name} submit did not complete. Waiting on backend state.`)
      addNotification({
        type: 'error',
        title: `${gate1Name} Submit Failed`,
        message: error.message || 'Backend submit did not complete. Pipeline state was not advanced locally.',
        duration: 5000
      })
      returnToMonitor(selectedRunId)
    } finally {
      setSubmitting(false)
    }
  }

  if (shouldRedirectDemoKpiReview) {
    return null
  }

  if (selectedRunId && isReviewableRun && isGate1) {
    return (
      <div className="flex h-full min-h-0 flex-col gap-4">
        <div className="flex min-h-0 flex-1 items-start justify-center overflow-y-auto rounded-[28px] bg-[linear-gradient(180deg,#0a1020_0%,#070c16_100%)] px-5 py-6">
          <div className="flex w-full max-w-6xl flex-col overflow-hidden rounded-[24px] border border-[#1d2940] bg-[#121a2b] shadow-[0_28px_90px_rgba(0,0,0,0.42)]">
            <div className="flex flex-col gap-4 border-b border-[#1d2940] px-5 py-5 md:flex-row md:items-center md:justify-between">
              <div className="flex min-w-0 items-center gap-4">
                <div className="flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-[10px] border border-[#5a3d13] bg-[#3a2a16] text-[#f4a912]">
                  <Shield size={20} strokeWidth={2.2} />
                </div>
                <div className="min-w-0">
                  <h2 className="text-[18px] font-extrabold text-white">Action Required: {gate1Name}</h2>
                  <p className="mt-1 text-sm text-[#b9c1cf]">
                    {currentRun?.resume_message || 'Stage 04 completed. Review KPIs before the pipeline continues.'}
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  disabled
                  title="Add KPI is not connected to a backend create endpoint yet."
                  className="inline-flex h-11 items-center gap-2 rounded-[10px] bg-[#202b3a] px-4 text-sm font-semibold text-[#b9c1cf] opacity-80"
                >
                  <PlusCircle size={16} className="text-[#12b886]" />
                  Add KPI
                </button>
                <button
                  type="button"
                  onClick={handleAutoApproveAll}
                  className="inline-flex h-11 items-center gap-2 rounded-[10px] bg-[#202b3a] px-4 text-sm font-semibold text-[#b9c1cf] transition-colors hover:bg-[#263449] hover:text-white"
                >
                  <CheckCircle size={16} className="text-[#12b886]" />
                  Auto-Approve Pending
                </button>
              </div>
            </div>

            <div className="flex-1 space-y-4 overflow-y-auto px-5 py-5">
              {filteredQueue.length === 0 ? (
                <div className="flex min-h-[220px] items-center justify-center rounded-[18px] border border-dashed border-[#263247] bg-[#0d1524] text-center">
                  <div>
                    <p className="text-sm font-semibold text-white">
                      {queue.length === 0 ? `No KPIs in queue. Select a run with ${gate1Name} pending.` : 'No KPIs match the current filter.'}
                    </p>
                  </div>
                </div>
              ) : (
                filteredQueue.map((kpi) => (
                  <KpiReviewCard
                    key={reviewItemKey(kpi)}
                    kpi={{ ...kpi, ...(editedKpis[reviewItemKey(kpi)] || {}) }}
                    localDecision={localDecisions[reviewItemKey(kpi)]}
                    rejectionReason={rejectionReasons[reviewItemKey(kpi)]}
                    onApprove={(id) => id ? handleApprove(id) : handleClearDecision(reviewItemKey(kpi))}
                    onEdit={handleEdit}
                    onReject={handleReject}
                  />
                ))
              )}
            </div>

            <div className="flex shrink-0 items-center justify-between gap-4 border-t border-[#1d2940] bg-[#101726] px-5 py-4">
              <p className="text-sm text-[#c6d2e8]">
                <span className="font-semibold text-white">{kpiCounts.approved + kpiCounts.edited + kpiCounts.rejected}</span> / {kpiCounts.total} KPIs reviewed
              </p>
              <div className="flex items-center gap-3">
                <button type="button" onClick={() => returnToMonitor(selectedRunId)} className="btn-secondary">
                  Pause Pipeline
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={submitting}
                  className="btn-primary disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {submitting ? 'Submitting...' : 'Submit Decisions & Resume'}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (selectedRunId && isReviewableRun && isGate3) {
    return (
      <div className="flex h-full min-h-0 flex-col gap-4">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm font-semibold text-white">Enrichment Review</p>
            <p className="text-xs text-[#8fa0bf]">Athena semantic enrichment approval for the active run.</p>
          </div>
          {reviewRuns.length > 0 && (
            <select
              value={selectedRunId || ''}
              onChange={(event) => selectReviewRun(event.target.value)}
              className="h-10 rounded-xl border border-[#253044] bg-[#0a1220] px-3 text-xs text-[#c6d2e8] outline-none"
            >
              {reviewRuns.map((run) => (
                <option key={run.id} value={run.id}>
                  {run.id.slice(0, 14)} - {run.brd_filename} (Gate {run.next_gate})
                </option>
              ))}
            </select>
          )}
        </div>

        <div className="flex min-h-0 flex-1 items-stretch justify-center overflow-y-auto rounded-[28px] bg-[radial-gradient(circle_at_top,_rgba(44,87,150,0.2),_transparent_42%),linear-gradient(180deg,#09101c_0%,#060b14_100%)] px-3 py-4 sm:px-5 sm:py-6">
          <div className="flex min-h-0 w-full max-w-[940px] flex-1 flex-col overflow-hidden rounded-[18px] border border-[#1d2940] bg-[#0d1729] shadow-[0_30px_90px_rgba(0,0,0,0.46)]">
            <div className="flex flex-col gap-4 border-b border-[#1d2940] bg-[#10192c] px-6 py-5 md:flex-row md:items-center md:justify-between">
              <div className="flex min-w-0 items-center gap-3">
                <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-[14px] border border-[#29496f] bg-[#11213a]">
                  <Database size={22} className="text-[#78a9ff]" />
                </div>
                <div className="min-w-0">
                  <h2 className="text-xl font-bold text-white">Enrichment Review</h2>
                  <p className="mt-1 text-sm text-[#a9b6cc]">
                    Review extracted and enriched column metadata for {semanticReviewItems.length} item{semanticReviewItems.length !== 1 ? 's' : ''} before the pipeline continues.
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <div className="rounded-[12px] border border-[#22304b] bg-[#0b1424] px-4 py-2 text-xs text-[#c6d2e8]">
                  {selectedRunId?.slice(0, 14)} - {currentRun?.brd_filename || 'Active run'}
                </div>
                <button
                  type="button"
                  onClick={handleAutoApproveSemanticItems}
                  className="inline-flex h-11 items-center gap-2 rounded-[12px] border border-[#2e845c] bg-[#112d21] px-4 text-sm font-semibold text-[#65d69e] transition-colors hover:bg-[#153925]"
                >
                  <CheckCircle size={15} />
                  Auto-Approve Pending
                </button>
              </div>
            </div>

            <div className="flex-1 space-y-4 overflow-y-auto bg-[#0b1220] p-3 sm:p-4">
              {semanticReviewItems.length === 0 ? (
                <div className="flex h-full flex-col items-center justify-center gap-4 py-16 text-center">
                  <div className="flex h-14 w-14 items-center justify-center rounded-full bg-[#131d30]">
                    <Inbox size={28} className="text-[#6f809f]" />
                  </div>
                  <div>
                    <p className="font-medium text-white">No Items Available</p>
                    <p className="mt-1 text-sm text-[#8fa0bf]">
                      The pipeline did not return any items for semantic review.
                    </p>
                  </div>
                </div>
              ) : (
                semanticReviewItems.map((item) => {
                  const key = semanticReviewItemKey(item)
                  return (
                    <SemanticReviewCard
                      key={key}
                      item={item}
                      localDecision={semanticDecisions[key]}
                      rejectionReason={semanticRejectionReasons[key]}
                      onApprove={handleApproveSemanticItem}
                      onReject={handleRejectSemanticItem}
                      onDraftChange={handleSemanticDraftChange}
                    />
                  )
                })
              )}
            </div>

            <div className="flex shrink-0 flex-col gap-3 border-t border-[#1d2940] bg-[#10192c] px-6 pb-5 pt-4">
              {semanticValidationError && (
                <div className="flex items-center gap-2 rounded-lg border border-accent-red/30 bg-accent-red/10 px-3 py-2 text-sm text-accent-red">
                  <AlertTriangle size={14} />
                  <span>{semanticValidationError}</span>
                </div>
              )}
              <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                <p className="text-sm text-[#9ca9bd]">
                  <span className="font-medium text-white">
                    {semanticReviewItems.length - pendingSemanticReviewItems.length}
                  </span>{' '}
                  / {semanticReviewItems.length} items reviewed
                </p>
                <div className="flex gap-3">
                  <button type="button" onClick={() => returnToMonitor(selectedRunId)} className="btn-secondary">
                    Pause Pipeline
                  </button>
                  <button
                    onClick={handleSubmit}
                    disabled={submitting}
                    className="btn-primary disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {submitting ? 'Saving...' : 'Submit Decisions & Resume'}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  if (selectedRunId && isReviewableRun && isGate2 && !isSftpRun) {
    return (
      <div className="flex h-full min-h-0 flex-col gap-4">
        <div className="flex min-h-0 flex-1 items-start justify-center overflow-y-auto rounded-[28px] bg-[linear-gradient(180deg,#0a1020_0%,#070c16_100%)] px-5 py-6">
          <div className="flex w-full max-w-6xl flex-col overflow-hidden rounded-[24px] border border-[#1d2940] bg-[#121a2b] shadow-[0_28px_90px_rgba(0,0,0,0.42)]">
            <div className="flex flex-col gap-4 border-b border-[#1d2940] px-5 py-5 md:flex-row md:items-center md:justify-between">
              <div className="flex min-w-0 items-center gap-4">
                <div className="flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-[10px] border border-[#244a93] bg-[#142952] text-[#69a0ff]">
                  <Table2 size={20} strokeWidth={2.2} />
                </div>
                <div className="min-w-0">
                  <h2 className="text-[18px] font-extrabold text-white">Action Required: {gate2Name}</h2>
                  <p className="mt-1 text-sm text-[#b9c1cf]">
                    {tableReview?.resume_message || 'Stage 06 completed. Review nominated tables before the pipeline continues.'}
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => {
                    handleSelectAllTables()
                    const next = {}
                    availableTableReviews.forEach((table) => {
                      next[tableReviewKey(table)] = 'APPROVED'
                    })
                    setTableReviewDecisions((prev) => ({ ...prev, ...next }))
                  }}
                  className="inline-flex h-11 items-center gap-2 rounded-[10px] bg-[#202b3a] px-4 text-sm font-semibold text-[#b9c1cf] transition-colors hover:bg-[#263449] hover:text-white"
                >
                  <CheckCircle size={16} className="text-[#12b886]" />
                  Auto-Approve Pending
                </button>
              </div>
            </div>

            <div className="flex-1 space-y-4 overflow-y-auto px-5 py-5">
              {availableTableReviews.length === 0 ? (
                <div className="flex min-h-[220px] items-center justify-center rounded-[18px] border border-dashed border-[#263247] bg-[#0d1524] text-center">
                  <div>
                    <p className="text-sm font-semibold text-white">No nominated tables found for this run.</p>
                  </div>
                </div>
              ) : (
                availableTableReviews.map((table) => {
                  const key = tableReviewKey(table)
                  const decision = tableReviewDecisions[key]
                  const confidence = Number(table.confidence_score || table.semantic_score || 0)
                  const coverage = Number(table.coverage_ratio || table.lexical_score || 0)
                  const matchedItems = Array.isArray(table.matched_keywords) ? table.matched_keywords : []

                  return (
                    <div
                      key={key}
                      className={`rounded-[16px] border px-5 py-5 transition-colors ${
                        decision === 'APPROVED'
                          ? 'border-[#1f5d4e] bg-[#112d2b]'
                          : decision === 'REJECTED'
                          ? 'border-[#723148] bg-[#2c1823]'
                          : 'border-[#263247] bg-[#121a2b]'
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex items-center gap-2">
                            <Database size={14} className="text-[#6ea2ff]" />
                            <h3 className="truncate text-[15px] font-bold text-white">{table.table_name || table.name || table.entity || key}</h3>
                            <span className="rounded-full border border-[#2e394d] bg-[#202938] px-2 py-0.5 text-[10px] font-semibold text-[#d5deec]">
                              TABLE
                            </span>
                          </div>
                          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-[#9ca8bb]">
                            <span>{table.database_name || table.database || 'database'}</span>
                            <span>›</span>
                            <span>{table.schema_name || table.schema || 'schema'}</span>
                            <span className="rounded-[6px] border border-[#1f5d4e] bg-[#11322f] px-2 py-1 text-[10px] font-semibold text-[#33d6a2]">
                              Nominated
                            </span>
                          </div>
                        </div>
                      </div>

                      <div className="mt-4">
                        <div className="mb-2 flex items-center justify-between text-xs text-[#c6d2e8]">
                          <span>Confidence</span>
                          <span className="font-semibold text-[#ffb621]">{confidence.toFixed(3)}</span>
                        </div>
                        <div className="h-1.5 overflow-hidden rounded-full bg-[#243247]">
                          <div className="h-full rounded-full bg-[#ffb621]" style={{ width: `${Math.max(8, Math.min(100, confidence * 100))}%` }} />
                        </div>
                      </div>

                      <div className="mt-4 rounded-[10px] border border-[#263247] bg-[#0d1524] px-4 py-3 text-xs text-[#c8d2e5]">
                        {table.nomination_reason || `Business coverage=${coverage.toFixed(3)}${matchedItems.length ? `, signals: ${matchedItems.join(', ')}` : ''}`}
                      </div>

                      {matchedItems.length > 0 && (
                        <div className="mt-4">
                          <div className="mb-2 text-xs text-[#9ca8bb]">Matching KPIs</div>
                          <div className="flex flex-wrap gap-2">
                            {matchedItems.map((item) => (
                              <span key={`${key}:${item}`} className="rounded-full border border-[#2d64c3] bg-[#122a52] px-2 py-1 text-[10px] font-medium text-[#69a0ff]">
                                {item}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}

                      <div className="mt-4 flex gap-2">
                        <button
                          type="button"
                          onClick={() => handleApproveTableReview(table)}
                          className="flex-1 rounded-[10px] border border-[#14856d] bg-[#103533] px-4 py-2.5 text-sm font-semibold text-[#31d49f] transition-colors hover:bg-[#15413d]"
                        >
                          Approve
                        </button>
                        <button
                          type="button"
                          onClick={() => handleRejectTableReview(table)}
                          className="flex-1 rounded-[10px] border border-[#8a3148] bg-[#2a1823] px-4 py-2.5 text-sm font-semibold text-[#ff647f] transition-colors hover:bg-[#351d29]"
                        >
                          Reject
                        </button>
                      </div>
                    </div>
                  )
                })
              )}
            </div>

            <div className="flex shrink-0 items-center justify-between gap-4 border-t border-[#1d2940] bg-[#101726] px-5 py-4">
              <p className="text-sm text-[#c6d2e8]">
                <span className="font-semibold text-white">{reviewedTableCount}</span> / {availableTableReviews.length} tables reviewed
              </p>
              <div className="flex items-center gap-3">
                <button type="button" onClick={() => returnToMonitor(selectedRunId)} className="btn-secondary">
                  Pause Pipeline
                </button>
                <button
                  onClick={handleSubmit}
                  disabled={submitting}
                  className="btn-primary disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {submitting ? 'Submitting...' : 'Submit Decisions & Resume'}
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-full gap-4">
      <div className="overflow-hidden rounded-[16px] border border-[#1d2940] bg-[#0f1829] shadow-[0_20px_80px_rgba(0,0,0,0.28)]">
        <div className="flex flex-col gap-4 px-8 py-9 md:flex-row md:items-center md:justify-between">
          <div className="flex min-w-0 items-center gap-4">
            <div className="flex h-[52px] w-[52px] flex-shrink-0 items-center justify-center rounded-[10px] border border-[#5a3d13] bg-[#3a2a16] text-[#f4a912]">
              <Shield size={25} strokeWidth={2.2} />
            </div>
            <div className="min-w-0">
              <h1 className="text-[26px] font-extrabold leading-tight text-white">
                Action Required: {activeReviewName}
              </h1>
              <p className="mt-1 text-[18px] font-medium leading-snug text-[#b9c1cf]">
                {isGate5
                  ? (silverReview?.resume_message || 'Stage 05 completed. Review generated Silver scripts before the pipeline continues.')
                  : isSilverMergeKeyReview
                  ? (silverMergeKeyReview?.resume_message || 'Review merge keys before Silver generation continues.')
                  : isGate4
                  ? (bronzeReview?.resume_message || 'Stage 04 completed. Review generated Bronze artifacts before the pipeline continues.')
                  : isGate3
                  ? (semanticReviewSource?.resume_message || 'Stage 03 completed. Review semantic enrichment before the pipeline continues.')
                  : isGate2
                  ? (tableReview?.resume_message || (isSftpRun ? 'Stage 02 completed. Review discovered feeds before the pipeline continues.' : 'Stage 02 completed. Review nominated tables before the pipeline continues.'))
                  : 'Stage 04 completed. Review KPIs before the pipeline continues.'}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            {!isGate2 && !isGate3 && !isGate4 && !isSilverMergeKeyReview && !isGate5 && (
              <button
                type="button"
                disabled
                title="Add KPI is not connected to a backend create endpoint yet."
                className="inline-flex h-12 items-center gap-2 rounded-[10px] bg-[#202b3a] px-5 text-[17px] font-bold text-[#b9c1cf] opacity-80"
              >
                <PlusCircle size={18} className="text-[#12b886]" />
                Add KPI
              </button>
            )}

            <button
              onClick={isGate3 ? handleAutoApproveSemanticItems : (isGate4 || isSilverMergeKeyReview || isGate5) ? handleAutoApproveCodeReviewItems : isGate2 ? (isSftpRun ? handleSelectAllFeeds : handleAutoApproveTables) : handleAutoApproveAll}
              className="inline-flex h-12 items-center gap-2 rounded-[10px] bg-[#202b3a] px-5 text-[17px] font-bold text-[#b9c1cf] transition-colors hover:bg-[#263449] hover:text-white"
            >
              <CheckCircle size={18} className="text-[#12b886]" />
              {isGate2 ? 'Auto-Select Pending' : 'Auto-Approve Pending'}
            </button>
          </div>
        </div>

        {(reviewRuns.length > 0 || (!isGate2 && !isGate3 && !isGate4 && !isSilverMergeKeyReview && !isGate5)) && (
          <div className="flex flex-wrap items-center gap-3 border-t border-[#1d2940] px-8 py-4">
          {reviewRuns.length > 0 && (
            <select
              value={selectedRunId || ''}
              onChange={(event) => selectReviewRun(event.target.value)}
              className="h-10 rounded-xl border border-[#253044] bg-[#0a1220] px-3 text-xs text-[#c6d2e8] outline-none"
            >
              {reviewRuns.map((run) => (
                <option key={run.id} value={run.id}>
                  {run.id.slice(0, 14)} - {run.brd_filename} ({run.next_review_key === 'silver_merge_key_review' ? 'Silver Merge Key Review' : `Gate ${run.next_gate}`})
                </option>
              ))}
            </select>
          )}

          {!isGate2 && !isGate3 && !isGate4 && !isSilverMergeKeyReview && !isGate5 && (
            <select
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value)}
              className="h-10 rounded-xl border border-[#253044] bg-[#0a1220] px-3 text-xs text-[#c6d2e8] outline-none"
            >
              <option value="All">All</option>
              <option value="Pending">Pending</option>
              <option value="APPROVED">Approved</option>
              <option value="EDITED">Edited</option>
              <option value="REJECTED">Rejected</option>
            </select>
          )}
          </div>
        )}
      </div>

      <div className="flex gap-4 flex-1 min-h-0">
        <div className="flex-1 overflow-y-auto pr-1 space-y-4 pb-20">
          {selectedRunId && isReviewableRun ? (
            isSilverMergeKeyReview ? (
            <CodeReviewPanel
              title="Silver Merge Key Review"
              description={`Review ${silverMergeKeyReviewFeeds.length} merge-key set${silverMergeKeyReviewFeeds.length !== 1 ? 's' : ''} before Silver generation continues.`}
              lineageLabel="View Source -> Bronze -> Silver Lineage"
              onViewLineage={() => navigate(`/app/data-migration?runId=${encodeURIComponent(selectedRunId)}`)}
              emptyMessage="Merge-key review data is not loaded yet. Keep the monitor open while the backend prepares this review."
              items={silverMergeKeyReviewItems}
              loading={hydrating}
              reviewedCount={reviewedCodeReviewCount}
              totalCount={silverMergeKeyReviewItems.length}
              gateDecision={codeReviewGateDecision || gateDecision}
              decisions={codeReviewDecisions}
              sessionKey={reviewSessionKeyRef.current}
              onSetItemDecision={setCodeReviewDecision}
              onAutoApprovePending={handleAutoApproveCodeReviewItems}
              onSetAllDecision={setAllCodeReviewItemsDecision}
              onDraftItemsChange={setCodeReviewDraftItems}
              onPause={() => returnToMonitor(selectedRunId)}
              onSubmit={handleSubmit}
              submitting={submitting}
              disabled={submitting || !gateReviewReady}
              submitLabel="Submit & Generate Silver"
            />
            ) : isGate5 ? (
            <CodeReviewPanel
              title="Silver Code Review"
              description={`Review ${silverReviewItems.length} generated script${silverReviewItems.length !== 1 ? 's' : ''} before the pipeline continues.`}
              lineageLabel="View Source -> Bronze -> Silver Lineage"
              onViewLineage={() => navigate(`/app/data-migration?runId=${encodeURIComponent(selectedRunId)}`)}
              emptyMessage={`Silver scripts are not loaded yet. Keep the monitor open while ${gate5Name} is prepared.`}
              items={silverCodeReviewItems}
              loading={hydrating}
              reviewedCount={reviewedCodeReviewCount}
              totalCount={silverCodeReviewItems.length}
              gateDecision={codeReviewGateDecision || gateDecision}
              decisions={codeReviewDecisions}
              sessionKey={reviewSessionKeyRef.current}
              onSetItemDecision={setCodeReviewDecision}
              onAutoApprovePending={handleAutoApproveCodeReviewItems}
              onSetAllDecision={setAllCodeReviewItemsDecision}
              onDraftItemsChange={setCodeReviewDraftItems}
              onPause={() => returnToMonitor(selectedRunId)}
              onSubmit={handleSubmit}
              submitting={submitting}
              disabled={submitting || !gateReviewReady}
              submitLabel="Submit & View Generated Code"
            />
            ) : isGate4 ? (
            <CodeReviewPanel
              title="Bronze Code Review"
              description={`Review ${bronzeReviewFeeds.length} generated script${bronzeReviewFeeds.length !== 1 ? 's' : ''} before the pipeline continues.`}
              lineageLabel="View Source -> Bronze Lineage"
              onViewLineage={() => navigate(`/app/data-migration?runId=${encodeURIComponent(selectedRunId)}`)}
              emptyMessage={`Bronze scripts are not loaded yet. Keep the monitor open while ${gate4Name} is prepared.`}
              items={bronzeCodeReviewItems}
              loading={hydrating}
              reviewedCount={reviewedCodeReviewCount}
              totalCount={bronzeCodeReviewItems.length}
              gateDecision={codeReviewGateDecision || gateDecision}
              decisions={codeReviewDecisions}
              sessionKey={reviewSessionKeyRef.current}
              onSetItemDecision={setCodeReviewDecision}
              onAutoApprovePending={handleAutoApproveCodeReviewItems}
              onSetAllDecision={setAllCodeReviewItemsDecision}
              onDraftItemsChange={setCodeReviewDraftItems}
              onPause={() => returnToMonitor(selectedRunId)}
              onSubmit={handleSubmit}
              submitting={submitting}
              disabled={submitting || !gateReviewReady}
              submitLabel="Submit & Generate Silver"
            />
            ) : isGate3 ? (
            <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }} className="flex h-[calc(100vh-240px)] min-h-[620px] flex-col overflow-hidden rounded-xl border border-bg-border bg-bg-card shadow-2xl">
              <div className="flex shrink-0 flex-col gap-4 border-b border-bg-border bg-bg-base/50 p-6 md:flex-row md:items-center md:justify-between">
                <div className="flex items-center gap-3">
                  <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent-blue/15">
                    <Database size={20} className="text-accent-blue" />
                  </div>
                  <div>
                    <h2 className="text-xl font-bold text-text-primary">Enrichment Review</h2>
                    <p className="text-sm text-text-secondary">
                      Review extracted and enriched column metadata for {semanticReviewItems.length} item{semanticReviewItems.length !== 1 ? 's' : ''} before the pipeline continues.
                    </p>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={handleAutoApproveSemanticItems}
                  className="btn-secondary flex items-center gap-2 text-sm"
                >
                  <CheckCircle size={14} className="text-accent-green" />
                  Auto-Approve Pending
                </button>
              </div>

              <div className="flex-1 space-y-4 overflow-y-auto bg-bg-base/20 p-6">
                {hydrating && semanticReviewItems.length === 0 ? (
                  <div className="flex h-full flex-col items-center justify-center gap-4 py-16 text-center">
                    <Loader2 size={28} className="animate-spin text-accent-blue" />
                    <p className="font-medium text-text-primary">Loading semantic review artifacts…</p>
                  </div>
                ) : semanticReviewItems.length === 0 ? (
                  <div className="flex h-full flex-col items-center justify-center gap-4 py-16 text-center">
                    <div className="flex h-14 w-14 items-center justify-center rounded-full bg-bg-hover">
                      <Inbox size={28} className="text-text-tertiary" />
                    </div>
                    <div>
                      <p className="font-medium text-text-primary">No Items Available</p>
                      <p className="mt-1 text-sm text-text-secondary">
                        The pipeline did not return any items for semantic review.
                      </p>
                    </div>
                  </div>
                ) : (
                  semanticReviewItems.map((item) => {
                    const key = semanticReviewItemKey(item)
                    return (
                      <SemanticReviewCard
                        key={key}
                        item={item}
                        localDecision={semanticDecisions[key]}
                        rejectionReason={semanticRejectionReasons[key]}
                        onApprove={handleApproveSemanticItem}
                        onReject={handleRejectSemanticItem}
                      />
                    )
                  })
                )}
              </div>

              <div className="flex shrink-0 flex-col gap-3 border-t border-bg-border bg-bg-base/50 px-6 pb-5 pt-4">
                {semanticValidationError && (
                  <div className="flex items-center gap-2 rounded-lg border border-accent-red/30 bg-accent-red/10 px-3 py-2 text-sm text-accent-red">
                    <AlertTriangle size={14} />
                    <span>{semanticValidationError}</span>
                  </div>
                )}
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <p className="text-sm text-text-secondary">
                    <span className="font-medium text-text-primary">
                      {semanticReviewItems.length - pendingSemanticReviewItems.length}
                    </span>{' '}
                    / {semanticReviewItems.length} items reviewed
                  </p>
                  <div className="flex gap-3">
                    <button type="button" onClick={() => returnToMonitor(selectedRunId)} className="btn-secondary">
                      Pause Pipeline
                    </button>
                    <button
                      onClick={handleSubmit}
                      disabled={submitting}
                      className="btn-primary disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      {submitting ? 'Saving...' : 'Submit Decisions & Resume'}
                    </button>
                  </div>
                </div>
              </div>
            </motion.div>
            ) : isGate2 ? (
            hydrating ? (
              <div className="flex min-h-[180px] items-center justify-center rounded-2xl border border-[#22304b] bg-[#0b1424] p-6 text-sm text-[#9fb0ca]">
                <span className="inline-flex items-center gap-2"><Loader2 size={16} className="animate-spin text-[#4fa3ff]" /> Loading table review artifacts…</span>
              </div>
            ) : (isSftpRun
              ? (availableSftpFeeds.length === 0)
              : (tableReview?.nominated_tables || []).length === 0) ? (
              <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
                {isSftpRun ? 'No discovered feeds found for this run.' : 'No nominated tables found for this run.'}
              </div>
            ) : (
              isSftpRun
                ? (availableSftpFeeds.map((feed) => {
                    const key = sftpFeedKey(feed)
                    return (
                      <label key={key} className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-5 flex items-start gap-3 cursor-pointer hover:border-[#35507d] transition-colors">
                        <input
                          type="checkbox"
                          checked={!!selectedTables[key]}
                          onChange={() => setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))}
                          className="mt-1 accent-accent-blue"
                        />
                        <SftpFeedReviewBody feed={feed} />
                      </label>
                    )
                  }))
                : ((tableReview?.nominated_tables || []).map((table) => {
                    const key = tableReviewKey(table)
                    return (
                      <label key={key} className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-5 flex items-start gap-3 cursor-pointer hover:border-[#35507d] transition-colors">
                        <input
                          type="checkbox"
                          checked={!!selectedTables[key]}
                          onChange={() => setSelectedTables((prev) => ({ ...prev, [key]: !prev[key] }))}
                          className="mt-1 accent-accent-blue"
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <Table2 size={15} className="text-text-tertiary" />
                            <h3 className="text-base font-bold text-text-primary break-all">{key}</h3>
                          </div>
                          <div className="flex items-center gap-2 text-xs text-text-tertiary flex-wrap mt-2">
                            <span>Match confidence {Number(table.confidence_score || 0).toFixed(3)}</span>
                            <span className="opacity-40">-</span>
                            <span>Business coverage {Number(table.coverage_ratio || 0).toFixed(3)}</span>
                            {(table.matched_keywords || []).length > 0 && (
                              <>
                                <span className="opacity-40">-</span>
                                <span>{formatSemanticSignalLabel(table)}</span>
                              </>
                            )}
                          </div>
                          {table.nomination_reason && (
                            <p className="text-sm text-text-secondary leading-relaxed mt-2">{table.nomination_reason}</p>
                          )}
                        </div>
                      </label>
                    )
                  }))
            )
            ) : filteredQueue.length === 0 ? (
            <div className="flex items-center justify-center h-40 text-gray-600 text-sm">
              {queue.length === 0 ? `No KPIs in queue. Select a run with ${gate1Name} pending.` : 'No KPIs match the current filter.'}
            </div>
          ) : (
            filteredQueue.map((kpi) => (
              <KpiReviewCard
                key={reviewItemKey(kpi)}
                kpi={{ ...kpi, ...(editedKpis[reviewItemKey(kpi)] || {}) }}
                localDecision={localDecisions[reviewItemKey(kpi)]}
                rejectionReason={rejectionReasons[reviewItemKey(kpi)]}
                onApprove={(id) => id ? handleApprove(id) : handleClearDecision(reviewItemKey(kpi))}
                onEdit={handleEdit}
                onReject={handleReject}
              />
            ))
            )
          ) : (
            <div className="flex items-center justify-center h-40 rounded-2xl border border-dashed border-bg-border bg-bg-card/40 text-center px-6">
              <div>
                <p className="text-sm font-semibold text-gray-300">No pending gate review</p>
                <p className="text-xs text-gray-500 mt-1">This page shows runs paused at KPI, table/feed, enrichment, bronze, and silver review.</p>
              </div>
            </div>
          )}
        </div>

        <div className="w-72 flex-shrink-0 flex flex-col gap-3">
          <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-4">
            <h3 className="text-xs uppercase tracking-wider text-gray-500 mb-3">Review Progress</h3>
            <div className="space-y-2">
              {selectedRunId && isReviewableRun ? (
                isGate2 ? (
                <>
                  <CountRow label={isSftpRun ? 'Total Feeds' : 'Total Tables'} value={isSftpRun ? totalFeedCount : (tableReview?.nominated_tables || []).length} color="text-gray-300" />
                  <CountRow label="Selected" value={isSftpRun ? selectedFeedCount : selectedTableCount} color="text-accent-green" pulse={(isSftpRun ? selectedFeedCount : selectedTableCount) > 0} />
                  <CountRow label="Unselected" value={Math.max(0, (isSftpRun ? (totalFeedCount - selectedFeedCount) : ((tableReview?.nominated_tables || []).length - selectedTableCount)))} color="text-accent-amber" />
                </>
                ) : isGate3 ? (
                <>
                  <CountRow label="Items" value={semanticReviewItems.length} color="text-gray-300" />
                  <CountRow label="Reviewed" value={semanticReviewItems.length - pendingSemanticReviewItems.length} color="text-accent-green" pulse={allSemanticReviewed} />
                  <CountRow label="Pending" value={pendingSemanticReviewItems.length} color="text-accent-amber" />
                </>
                ) : isGate4 ? (
                <>
                  <CountRow label="Bronze Plans" value={bronzeReviewFeeds.length} color="text-gray-300" />
                  <CountRow label="Reviewed" value={reviewedCodeReviewCount} color="text-accent-green" pulse={reviewedCodeReviewCount > 0} />
                  <CountRow label="Pending" value={Math.max(0, bronzeCodeReviewItems.length - reviewedCodeReviewCount)} color="text-accent-amber" />
                </>
                ) : isSilverMergeKeyReview ? (
                <>
                  <CountRow label="Merge-key Sets" value={silverMergeKeyReviewFeeds.length} color="text-gray-300" />
                  <CountRow label="Reviewed" value={reviewedCodeReviewCount} color="text-accent-green" pulse={reviewedCodeReviewCount > 0} />
                  <CountRow label="Pending" value={Math.max(0, silverMergeKeyReviewItems.length - reviewedCodeReviewCount)} color="text-accent-amber" />
                </>
                ) : isGate5 ? (
                <>
                  <CountRow label="Silver Scripts" value={silverReviewItems.length} color="text-gray-300" />
                  <CountRow label="Reviewed" value={reviewedCodeReviewCount} color="text-accent-green" pulse={reviewedCodeReviewCount > 0} />
                  <CountRow label="Pending" value={Math.max(0, silverCodeReviewItems.length - reviewedCodeReviewCount)} color="text-accent-amber" />
                </>
                ) : (
                <>
                  <CountRow label="Total" value={kpiCounts.total} color="text-gray-300" />
                  <CountRow label="Pending" value={kpiCounts.pending} color="text-accent-amber" pulse={kpiCounts.pending > 0} />
                  <CountRow label="Approved" value={kpiCounts.approved} color="text-accent-green" />
                  <CountRow label="Edited" value={kpiCounts.edited} color="text-accent-purple" />
                  <CountRow label="Rejected" value={kpiCounts.rejected} color="text-accent-red" />
                </>
                )
              ) : (
                <span className="text-gray-500 text-sm">No active gate review</span>
              )}
            </div>

            <div className="mt-3">
              <div className="h-1.5 bg-bg-border rounded-full overflow-hidden">
                <div
                  className="h-full bg-accent-green transition-all duration-500 rounded-full"
                  style={{
                    width: `${isGate3
                      ? 100
                      : isGate4 || isSilverMergeKeyReview || isGate5
                      ? (activeCodeReviewItems.length > 0 ? (reviewedCodeReviewCount / activeCodeReviewItems.length) * 100 : (gateReviewReady ? 100 : 0))
                      : isGate2
                      ? (isSftpRun
                          ? (totalFeedCount > 0 ? (selectedFeedCount / totalFeedCount) * 100 : 0)
                          : ((tableReview?.nominated_tables || []).length > 0 ? (selectedTableCount / (tableReview?.nominated_tables || []).length) * 100 : 0))
                      : (kpiCounts.total > 0 ? ((kpiCounts.approved + kpiCounts.edited + kpiCounts.rejected) / kpiCounts.total) * 100 : 0)}%`
                  }}
                />
              </div>
            </div>
          </div>

          <div className="rounded-[24px] border border-[#1d2940] bg-[#0d1729] p-4">
            <div className="flex items-center gap-2 mb-2">
              <Timer size={14} className="text-accent-amber" />
              <span className="text-xs font-medium text-gray-300">Review State</span>
            </div>
            <p className="text-2xl font-mono font-bold text-accent-amber">{hydrating ? 'SYNC' : 'READY'}</p>
            <p className="text-[10px] text-gray-600 mt-1">Pipeline is paused</p>
          </div>

          <button
            onClick={isGate3 ? handleAutoApproveSemanticItems : (isGate4 || isSilverMergeKeyReview || isGate5) ? handleAutoApproveCodeReviewItems : isGate2 ? (isSftpRun ? handleSelectAllFeeds : handleAutoApproveTables) : handleAutoApproveAll}
            className="flex items-center justify-center gap-2 px-4 py-3 bg-accent-green/10 hover:bg-accent-green/20 border border-accent-green/25 text-accent-green text-sm font-semibold rounded-xl transition-colors"
          >
            <CheckCircle size={15} />
            {isGate3 ? 'Auto-Approve Pending' : isGate4 || isSilverMergeKeyReview || isGate5 ? 'Auto-Approve Pending' : isGate2 ? (isSftpRun ? 'Select All Feeds' : 'Select All Tables') : 'Auto-approve All'}
          </button>

          <div className="rounded-[20px] border border-[#22304b] bg-[#0d1729] p-3">
            <div className="flex items-start gap-2">
              <Shield size={12} className="text-gray-600 mt-0.5 flex-shrink-0" />
              <p className="text-[10px] text-gray-600 leading-relaxed">
                {isGate2
                  ? (isSftpRun
                    ? `${gate2Name} validates the discovered SFTP feeds. Review entity, source file, sample rows, columns, keys, and measures before approving the feed set.`
                    : 'Certified tables become the source set for metadata discovery, profiling, and enrichment.')
                  : isGate3
                  ? `Approving ${gate3Name} generates Bronze review artifacts. Rejecting keeps the run paused for rework.`
                  : isGate4
                  ? `Approving ${gate4Name} accepts the Bronze scripts and starts Silver script generation.`
                  : isSilverMergeKeyReview
                  ? 'Approving Silver Merge Key Review accepts the merge keys and starts Silver generation.'
                  : isGate5
                  ? `Approving ${gate5Name} accepts the Silver scripts and continues downstream validation.`
                  : 'Approvals are final once submitted. Rejected KPIs will be excluded from the final export.'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {canSubmitReview && !isGate3 && !isGate4 && !isSilverMergeKeyReview && !isGate5 && (
        <motion.div
          initial={{ y: 80, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: 80, opacity: 0 }}
          className="fixed bottom-0 left-0 right-0 z-30 flex items-center justify-between px-6 py-4 bg-bg-card border-t border-bg-border shadow-2xl"
          style={{ background: 'rgba(17,24,39,0.95)', backdropFilter: 'blur(10px)' }}
        >
          <div className="flex items-center gap-4 text-sm">
            {isGate4 || isSilverMergeKeyReview || isGate5 ? (
              <>
                <span className={gateDecision === 'APPROVED' ? 'text-accent-green font-semibold' : gateDecision === 'REJECTED' ? 'text-accent-red font-semibold' : 'text-accent-blue font-semibold'}>
                  {gateDecision === 'APPROVED' ? 'Approve selected' : gateDecision === 'REJECTED' ? 'Reject selected' : 'Regenerate selected'}
                </span>
                <span className="text-gray-500">{isGate4 ? `${bronzeReviewFeeds.length} Bronze plan(s)` : isSilverMergeKeyReview ? `${silverMergeKeyReviewFeeds.length} merge-key set(s)` : `${silverReviewItems.length} Silver script(s)`}</span>
              </>
            ) : isGate3 ? (
              <>
                <span className={gate3Decision === 'APPROVED' ? 'text-accent-green font-semibold' : 'text-accent-red font-semibold'}>
                  {gate3Decision === 'APPROVED' ? 'Approve selected' : 'Reject selected'}
                </span>
                <span className="text-gray-500">{semanticReviewItems.length} semantic item(s)</span>
              </>
            ) : isGate2 ? (
              <>
                <span className="text-accent-green font-semibold">{isSftpRun ? selectedFeedCount : selectedTableCount} selected</span>
                <span className="text-gray-500">{Math.max(0, (isSftpRun ? (totalFeedCount - selectedFeedCount) : ((tableReview?.nominated_tables || []).length - selectedTableCount)))} unselected</span>
              </>
            ) : (
              <>
                {kpiCounts.approved > 0 && <span className="text-accent-green font-semibold">{kpiCounts.approved} approved</span>}
                {kpiCounts.edited > 0 && <span className="text-accent-purple font-semibold">{kpiCounts.edited} edited</span>}
                {kpiCounts.rejected > 0 && <span className="text-accent-red font-semibold">{kpiCounts.rejected} rejected</span>}
                {kpiCounts.pending > 0 && <span className="text-gray-500">{kpiCounts.pending} still pending</span>}
              </>
            )}
          </div>

          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="flex items-center gap-2 px-6 py-2.5 bg-accent-blue hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed text-white text-sm font-bold rounded-xl transition-colors shadow-lg"
          >
            {submitting ? (
              <>
                <Loader2 size={14} className="animate-spin" />
                Submitting...
              </>
            ) : (
              <>
                {isGate3 || isGate2 || isGate4 || isSilverMergeKeyReview || isGate5 ? <CheckCircle2 size={14} /> : <Send size={14} />}
                {isGate5 ? `Submit ${gate5Name} & Continue Pipeline ->` : isSilverMergeKeyReview ? 'Submit Silver Merge Key Review & Generate Silver ->' : isGate4 ? `Submit ${gate4Name} & Review Merge Keys ->` : isGate3 ? `Submit ${gate3Name} & Generate Bronze ->` : isGate2 ? `Submit ${gate2Name} & Resume Pipeline ->` : 'Submit All Decisions & Resume Pipeline ->'}
              </>
            )}
          </button>
        </motion.div>
      )}

      <EditKpiModal
        kpi={editingKpi}
        isOpen={!!editingKpi}
        onClose={() => setEditingKpi(null)}
        onSave={handleSaveEdit}
      />
    </div>
  )
}

function mapHitlRow(row) {
  const decision = normalizeReviewDecision(row.decision) || normalizeReviewDecision(row.gate_status)
  return {
    id: row.queue_id || row.id,
    queue_id: row.queue_id || row.id,
    item_id: row.item_id,
    run_id: row.run_id,
    source: row.source ? normalizeSourceValue(row.source) : undefined,
    item_type: row.item_type || 'METADATA',
    name: row.name,
    definition: row.definition,
    category: row.category,
    domain: row.domain,
    confidence: row.confidence,
    status: row.status,
    grounded: row.grounded,
    explicit: row.explicit,
    kpi_detail: row.kpi_detail || {},
    modified_detail: row.modified_detail || null,
    gate_status: row.gate_status,
    decision,
    reviewer_id: row.reviewer_id,
    rejection_reason: row.rejection_reason,
    auto_approved: row.auto_approved === true || row.auto_approved === 'true',
    queued_at: row.queued_at,
    decided_at: row.decided_at,
    timeout_at: row.timeout_at
  }
}

function normalizeSemanticColumns(columns = []) {
  const list = Array.isArray(columns)
    ? columns
    : Object.entries(columns || {}).map(([name, detail]) => ({
        ...(typeof detail === 'object' && detail !== null ? detail : {}),
        column_name: name,
        semantic_type: typeof detail === 'string' ? detail : detail?.semantic_type,
      }))

  return list.map((column, index) => {
    if (typeof column === 'string') {
      return {
        column_name: column,
        suggested_display_name: column,
        semantic_type: 'DIMENSION',
        business_description: '',
        enrichment_source: 'semantic_enrichment',
        is_measure: false,
        is_dimension: true,
        is_pii_candidate: false,
      }
    }

    return {
      column_name: column.column_name || column.name || column.column || `column_${index + 1}`,
      suggested_display_name:
        column.suggested_display_name || column.display_name || column.column_name || column.name || column.column || `Column ${index + 1}`,
      semantic_type: column.semantic_type || column.type || 'DIMENSION',
      business_description:
        column.business_description || column.description || column.summary || column.nomination_reason || '',
      enrichment_source: column.enrichment_source || column.source || 'semantic_enrichment',
      is_measure: !!column.is_measure,
      is_dimension: !!column.is_dimension,
      is_pii_candidate: !!(column.is_pii_candidate || column.is_pii),
    }
  })
}

function buildSemanticReviewSource(enrichmentReview, currentRun, runId) {
  const source = {
    ...(currentRun || {}),
    ...(enrichmentReview || {}),
  }
  const enrichedColumns = source.enriched_columns || []
  const enrichedColumnCount = Array.isArray(enrichedColumns)
    ? enrichedColumns.length
    : Object.keys(enrichedColumns || {}).length

  const hasModernArtifact = Boolean(
    enrichedColumnCount ||
    (source.enriched_joins || []).length ||
    (source.feed_semantic_summary || []).length ||
    Object.keys(source.enriched_metadata || {}).length ||
    Object.keys(source.semantic_counts || {}).length ||
    (source.pii_columns || []).length ||
    (source.join_key_columns || []).length ||
    (source.measure_columns || []).length
  )

  if (!source.run_id && !source.id) source.run_id = runId

  if (!hasModernArtifact) {
    return {
      ...source,
      queue_id: source.queue_id || `${runId || source.id || 'run'}-semantic-enrichment-fallback`,
      table_name: source.table_name || source.entity || source.display_name || source.name || 'Semantic Review',
      enriched_columns: [],
      resume_message: source.resume_message || 'Semantic Enrichment completed. Review enriched column metadata before the pipeline continues.',
      is_fallback_artifact: true,
    }
  }

  if (enrichedColumnCount === 0 && source.enriched_metadata) {
    source.enriched_columns = source.enriched_metadata
  }

  return source
}

function toAthenaSemanticItems(enrichmentReview, isSftpRun, runId) {
  if (!enrichmentReview) return []
  const semanticTables = enrichmentReview.semantic_tables || enrichmentReview.tables || enrichmentReview.table_semantics || []

  if (semanticTables.length > 0) {
    return semanticTables.map((table, index) => ({
      queue_id: table.queue_id || `${runId || 'run'}-semantic-table-${table.table_name || table.name || index}`,
      item_id: table.table_name || table.name || table.entity || `Table ${index + 1}`,
      item_type: 'ENRICHMENT',
      item_detail: {
        table_name: table.table_name || table.name || table.entity || `Table ${index + 1}`,
        columns: normalizeSemanticColumns(table.columns || table.enriched_columns || []),
        table_summary: table.table_summary || table.summary || `${table.table_name || table.name || 'Table'} semantic enrichment summary.`,
      },
      decision: table.decision,
      reviewer_id: table.reviewer_id,
      rejection_reason: table.rejection_reason,
      queued_at: table.queued_at,
      decided_at: table.decided_at,
    }))
  }

  const feeds = enrichmentReview.feed_semantic_summary || []

  if (isSftpRun && feeds.length > 0) {
    return feeds.map((feed, index) => ({
      queue_id: feed.queue_id || `${runId || 'run'}-semantic-feed-${index}`,
      item_id: feed.feed_id || feed.entity || feed.table_name || `Feed ${index + 1}`,
      item_type: 'ENRICHMENT',
      item_detail: {
        table_name: feed.feed_id || feed.entity || feed.table_name || `Feed ${index + 1}`,
        columns: normalizeSemanticColumns(feed.enriched_columns || feed.columns || feed.semantic_columns || []),
        table_summary: feed.table_summary || feed.summary || `${feed.entity || feed.table_name || 'Feed'} column extraction summary.`,
      },
      decision: feed.decision,
      reviewer_id: feed.reviewer_id,
      rejection_reason: feed.rejection_reason,
      queued_at: feed.queued_at,
      decided_at: feed.decided_at,
    }))
  }

  const flatEnrichedColumns = Array.isArray(enrichmentReview.enriched_columns)
    ? enrichmentReview.enriched_columns
    : []
  const groupedColumns = groupSemanticColumnsByTable(flatEnrichedColumns)

  if (groupedColumns.length > 0) {
    return groupedColumns.map(({ tableName, columns }, index) => ({
      queue_id: `${runId || 'run'}-semantic-table-${tableName || index}`,
      item_id: tableName || `Table ${index + 1}`,
      item_type: 'ENRICHMENT',
      item_detail: {
        table_name: tableName || `Table ${index + 1}`,
        columns: normalizeSemanticColumns(columns),
        table_summary:
          enrichmentReview.table_summaries?.[tableName] ||
          enrichmentReview.enriched_metadata?.table_summaries?.[tableName] ||
          `${tableName || 'Table'} semantic labels prepared for column review.`,
      },
      decision: enrichmentReview.decision,
      reviewer_id: enrichmentReview.reviewer_id,
      rejection_reason: enrichmentReview.rejection_reason,
      queued_at: enrichmentReview.queued_at,
      decided_at: enrichmentReview.decided_at,
    }))
  }

  return [
    {
      queue_id: enrichmentReview.queue_id || `${runId || 'run'}-semantic-enrichment`,
      item_id: enrichmentReview.entity || enrichmentReview.table_name || 'Semantic Review',
      item_type: 'ENRICHMENT',
      item_detail: {
        table_name: enrichmentReview.entity || enrichmentReview.table_name || 'Semantic Review',
        columns: normalizeSemanticColumns(enrichmentReview.enriched_columns || []),
        table_summary: enrichmentReview.table_summary || enrichmentReview.summary || 'Column extraction and enrichment summary.',
      },
      decision: enrichmentReview.decision,
      reviewer_id: enrichmentReview.reviewer_id,
      rejection_reason: enrichmentReview.rejection_reason,
      queued_at: enrichmentReview.queued_at,
      decided_at: enrichmentReview.decided_at,
    },
  ]
}

function semanticColumnTableName(column) {
  const value =
    column?.table_name ||
    column?.table ||
    column?.entity ||
    column?.source_table ||
    column?.source_table_name ||
    column?.qualified_table ||
    ''
  const text = String(value || '').trim()
  if (!text) return ''
  return text.split('.').filter(Boolean).pop() || text
}

function groupSemanticColumnsByTable(columns) {
  const grouped = new Map()

  for (const column of columns || []) {
    const tableName = semanticColumnTableName(column)
    if (!tableName) continue
    if (!grouped.has(tableName)) grouped.set(tableName, [])
    grouped.get(tableName).push(column)
  }

  if (grouped.size === 0) return []
  return Array.from(grouped.entries()).map(([tableName, groupedColumns]) => ({
    tableName,
    columns: groupedColumns,
  }))
}

function semanticReviewItemKey(item) {
  return item?.queue_id || item?.id || item?.item_id
}

function formatSemanticSignalLabel(table) {
  const signalCount = Array.isArray(table?.matched_keywords) ? table.matched_keywords.length : 0
  if (signalCount <= 0) return 'Semantic signals available'
  if (signalCount === 1) return '1 semantic signal detected'
  return `${signalCount} semantic signals detected`
}

function normalizeReviewDecision(value) {
  const normalized = String(value || '').toUpperCase()
  return ['APPROVED', 'REJECTED', 'EDITED'].includes(normalized) ? normalized : null
}

function reviewItemKey(item) {
  return item?.queue_id || item?.id || item?.item_id
}

function normalizeSourceValue(source) {
  return String(source || '').toLowerCase()
}

function getReviewItemRunId(item) {
  if (item?.run_id) return String(item.run_id)
  const key = String(item?.queue_id || item?.item_id || item?.id || '')
  return key.includes(':') ? key.split(':')[0] : ''
}

function filterReviewQueue(items, runId, source) {
  const expectedRunId = String(runId || '')
  const expectedSource = normalizeSourceValue(source)
  return (items || []).filter((item) => {
    const itemRunId = getReviewItemRunId(item)
    if (expectedRunId && itemRunId && itemRunId !== expectedRunId) return false
    const itemSource = normalizeSourceValue(item?.source)
    if (itemSource && expectedSource && itemSource !== expectedSource) return false
    return true
  })
}

function reviewPayloadMatchesRun(payload, runId, source) {
  if (!payload) return false
  if (payload.run_id && String(payload.run_id) !== String(runId || '')) return false
  if (payload.runId && String(payload.runId) !== String(runId || '')) return false
  const payloadSource = normalizeSourceValue(payload.source)
  const expectedSource = normalizeSourceValue(source)
  if (payloadSource && expectedSource && payloadSource !== expectedSource) return false
  return true
}

function hasReviewGate(run) {
  if (run?.next_review_key) return true
  const gate = Number(run?.next_gate || 0)
  return gate >= 1 && gate <= 5
}

function hasGatePayload(run) {
  return Boolean(
    (run?.kpis || []).length ||
    (run?.nominated_tables || []).length ||
    run?.candidate_feed ||
    (run?.candidate_feeds || []).length ||
    (run?.enriched_columns || []).length ||
    (run?.enriched_joins || []).length ||
    (run?.feed_semantic_summary || []).length ||
    Object.keys(run?.enriched_metadata || {}).length ||
    (run?.bronze_review_artifact?.feeds || []).length ||
    (run?.silver_merge_key_review_artifact?.feeds || []).length ||
    (run?.silver_review_artifact?.items || []).length ||
    run?.resume_message
  )
}

function isSuppressedInitialReviewRun(run) {
  const runId = String(run?.id || run?.run_id || '')
  return (
    Number(run?.next_gate || 0) === 1 &&
    (
      runId === 'run_a3f8c2' ||
      isDemoFallbackRun(run) ||
      Boolean(run?.demo_review_fallback) ||
      String(run?.review_fallback_reason || '').toLowerCase().includes('fallback')
    )
  )
}

function isReviewGateAccessible(run) {
  if (isSuppressedInitialReviewRun(run)) return false
  if (!hasReviewGate(run)) return false
  if (run?.stage_confirmation?.awaiting_confirmation) return false

  const status = String(run?.status || '').toUpperCase()
  if (status === 'PAUSED_FOR_STAGE_CONFIRMATION') return false

  return (
    ['HITL_WAIT', 'PAUSED_FOR_HITL', 'PENDING_REVIEW'].includes(status) ||
    Boolean(run?.next_review_key) ||
    (hasGatePayload(run) && !['RUNNING', 'PROCESSING', 'SUBMITTED', 'IN_PROGRESS'].includes(status))
  )
}

function isRunActivelyProcessing(run) {
  const status = String(run?.status || '').toUpperCase()
  return ['RUNNING', 'PROCESSING', 'SUBMITTED', 'IN_PROGRESS'].includes(status) || Boolean(run?.background_stage)
}

function matchesRequestedReview(run, requestedGate, requestedReviewKey) {
  if (!run || (!requestedGate && !requestedReviewKey)) return true
  if (requestedReviewKey) return run?.next_review_key === requestedReviewKey
  return Number(run?.next_gate || 0) === Number(requestedGate || 0)
}

function CountRow({ label, value, color, pulse }) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-2">
        {pulse && <span className="w-1.5 h-1.5 rounded-full bg-accent-amber animate-pulse" />}
        <span className="text-xs text-gray-500">{label}</span>
      </div>
      <span className={`text-sm font-bold font-mono ${color}`}>{value}</span>
    </div>
  )
}

function StatTile({ label, value }) {
  return (
    <div className="rounded-2xl border border-[#22304b] bg-[#0b1424] px-3 py-3">
      <div className="text-xs text-[#7f8eab]">{label}</div>
      <div className="text-lg font-bold text-text-primary mt-1">{value}</div>
    </div>
  )
}

function tableReviewKey(table) {
  const database = table.database_name || table.database || table.catalog || table.table_catalog
  const schema = table.schema_name || table.schema || table.table_schema
  const tableName = table.table_name || table.name || table.entity || table.table
  const qualified = tableName ? `${database || ''}.${schema || ''}.${tableName || ''}` : ''
  return qualified || String(table.id || table.key || table.full_name || table.table_id || JSON.stringify(table))
}

function sftpFeedKey(feed) {
  return [feed.vendor, feed.entity, feed.file_name || feed.feed_id].filter(Boolean).join('.')
}

function getSftpFeeds(review) {
  if (!review) return []
  if (Array.isArray(review.candidate_feeds) && review.candidate_feeds.length > 0) {
    return review.candidate_feeds
  }
  return review.candidate_feed ? [review.candidate_feed] : []
}

function SftpFeedReviewBody({ feed }) {
  const columns = Array.isArray(feed?.columns) ? feed.columns : []
  const primaryKeys = Array.isArray(feed?.primary_keys) ? feed.primary_keys : []
  const measures = Array.isArray(feed?.measures) ? feed.measures : []
  const entities = Array.isArray(feed?.entities) ? feed.entities : []

  return (
    <div className="min-w-0 flex-1">
      <div className="flex items-center gap-2 flex-wrap">
        <Table2 size={15} className="text-text-tertiary" />
        <h3 className="text-base font-bold text-text-primary break-all">
          {feed.vendor || 'Vendor'}.{feed.entity || feed.semantic_type || 'feed'}
        </h3>
        {feed.semantic_type && (
          <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-accent-blue/10 text-accent-blue border border-accent-blue/20">
            {feed.semantic_type}
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 gap-2 mt-3 text-xs text-text-tertiary">
        <div><span className="text-gray-500">File:</span> {feed.file_name || 'n/a'}</div>
        <div><span className="text-gray-500">Format:</span> {feed.format || 'unknown'}</div>
        <div><span className="text-gray-500">Rows:</span> {Number(feed.sample_row_count || 0)}</div>
        <div><span className="text-gray-500">Columns:</span> {columns.length}</div>
      </div>

      {entities.length > 0 && (
        <p className="text-xs text-text-secondary mt-2">
          Feed set entities: {entities.join(', ')}
        </p>
      )}

      {feed.file_path && (
        <p className="text-xs text-text-secondary mt-2 break-all">
          Path: {feed.file_path}
        </p>
      )}

      {primaryKeys.length > 0 && (
        <p className="text-xs text-text-secondary mt-2">
          Primary keys: {primaryKeys.join(', ')}
        </p>
      )}

      {measures.length > 0 && (
        <p className="text-xs text-text-secondary mt-1">
          Measures: {measures.join(', ')}
        </p>
      )}

      {columns.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mt-3">
          {columns.slice(0, 8).map((column) => (
            <span key={column} className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-bg-base border border-bg-border text-text-secondary">
              {column}
            </span>
          ))}
          {columns.length > 8 && (
            <span className="px-2 py-0.5 rounded-full text-[10px] font-medium bg-bg-base border border-bg-border text-text-secondary">
              +{columns.length - 8} more
            </span>
          )}
        </div>
      )}
    </div>
  )
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function FileSemanticFeedCard({ feed }) {
  const semanticCounts = Object.entries(feed?.semantic_counts || {})
  return (
    <div className="rounded-[20px] border border-[#22304b] bg-[#0f1a2e] p-4">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div>
          <div className="text-sm font-semibold text-text-primary">
            {feed.vendor || 'Vendor'}.{feed.entity || feed.feed_id || 'feed'}
          </div>
          <div className="text-xs text-text-secondary mt-1">
            {feed.format || 'unknown'}{feed.file_name ? ` • ${feed.file_name}` : ''}
          </div>
        </div>
        <div className="text-xs text-text-secondary">
          {Number(feed.sample_row_count || 0)} sample rows
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-3">
        <StatTile label="Columns" value={Number(feed.column_count || 0)} />
        <StatTile label="PII" value={Number(feed.pii_count || 0)} />
        <StatTile label="Join Keys" value={Number(feed.join_key_count || 0)} />
        <StatTile label="Measures" value={Number(feed.measure_count || 0)} />
      </div>

      {semanticCounts.length > 0 && (
        <div className="flex flex-wrap gap-2 mt-3">
          {semanticCounts.map(([key, value]) => (
            <span key={key} className="rounded-full border border-[#2c3f5f] bg-[#111b2d] px-2 py-1 text-[10px] font-medium text-text-secondary">
              {key}: {value}
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

export default HitlQueue

function CodeReviewPanel({
  title,
  description,
  emptyMessage,
  items,
  reviewedCount,
  totalCount,
  gateDecision,
  decisions,
  onSetItemDecision,
  onAutoApprovePending,
  onSetAllDecision,
  onPause,
  onSubmit,
  submitting,
  disabled,
  submitLabel,
  lineageLabel,
  onViewLineage,
  onDraftItemsChange,
  sessionKey,
  loading,
}) {
  const [expandedKey, setExpandedKey] = useState(null)
  const [draftItems, setDraftItems] = useState(items)
  const itemKeys = items.map((item) => item.key).join('|')

  useEffect(() => {
    setDraftItems(items)
    onDraftItemsChange?.(items)
    setExpandedKey(null)
    // Reset only when the reviewer actually changes run/gate.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionKey])

  useEffect(() => {
    if (!items.length) {
      return
    }
    setDraftItems((current) => {
      const currentByKey = new Map(current.map((item) => [item.key, item]))
      const next = items.map((item) => currentByKey.has(item.key) ? { ...item, ...currentByKey.get(item.key) } : item)
      onDraftItemsChange?.(next)
      return next
    })
    setExpandedKey((current) => (items.some((item) => item.key === current) ? current : null))
    // Track item identity only; preserving edited draft code while parent objects refresh.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemKeys])

  const updateItemCode = (key, code) => {
    setDraftItems((current) => {
      const next = current.map((item) => (
        item.key === key ? { ...item, code, edited: true } : item
      ))
      onDraftItemsChange?.(next)
      return next
    })
  }

  const updateItemFields = (key, fields) => {
    setDraftItems((current) => {
      const next = current.map((item) => (
        item.key === key ? { ...item, ...fields, code: patchMergeKeysInCode(item.code, fields.mergeKeys), edited: true } : item
      ))
      onDraftItemsChange?.(next)
      return next
    })
  }

  const decisionLabel =
    gateDecision === 'APPROVED' ? 'All approved' :
    gateDecision === 'REJECTED' ? 'Rejected item selected' :
    gateDecision === 'REGENERATE' ? 'Regenerate item selected' :
    `${reviewedCount}/${totalCount} reviewed`
  const decisionTone =
    gateDecision === 'APPROVED' ? 'border-emerald-500/35 bg-emerald-500/10 text-emerald-300' :
    gateDecision === 'REJECTED' ? 'border-red-500/35 bg-red-500/10 text-red-300' :
    gateDecision === 'REGENERATE' ? 'border-[#3f82ff]/35 bg-[#3f82ff]/10 text-[#78a9ff]' :
    'border-amber-500/35 bg-amber-500/10 text-amber-300'

  return (
    <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }} className="flex h-[calc(100vh-240px)] min-h-[620px] flex-col overflow-hidden rounded-xl border border-[#1d2940] bg-[#0f1829] shadow-2xl">
      <div className="flex shrink-0 flex-col gap-4 border-b border-[#1d2940] bg-[#101726] p-6 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-[#163b74] text-[#4fa3ff]">
            <Copy size={20} />
          </div>
          <div>
            <h2 className="text-xl font-extrabold text-white">{title}</h2>
            <p className="text-sm text-[#c6d2e8]">{description}</p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <span className={`inline-flex h-10 items-center rounded-lg border px-3 text-xs font-bold ${decisionTone}`}>
            {decisionLabel}
          </span>
          {onViewLineage && (
            <button
              type="button"
              onClick={onViewLineage}
              className="inline-flex h-10 items-center gap-2 rounded-lg border border-[#2c4f85] bg-[#102144] px-4 text-sm font-bold text-[#9fc0ff] transition-colors hover:bg-[#14305f]"
            >
              <Database size={14} />
              {lineageLabel || 'View Lineage'}
            </button>
          )}
          <button
            type="button"
            onClick={onAutoApprovePending}
            className="inline-flex h-10 items-center gap-2 rounded-lg bg-[#202b3a] px-4 text-sm font-bold text-[#c6d2e8] transition-colors hover:bg-[#263449] hover:text-white"
          >
            <CheckCircle size={14} className="text-[#12b886]" />
            Auto-Approve Pending
          </button>
        </div>
      </div>

      <div className="flex-1 space-y-4 overflow-y-auto bg-[#0b1220] p-6">
        {loading && draftItems.length === 0 ? (
          <div className="flex min-h-[180px] items-center justify-center rounded-2xl border border-[#22304b] bg-[#0b1424] p-6 text-sm text-[#9fb0ca]">
            <span className="inline-flex items-center gap-2"><Loader2 size={16} className="animate-spin text-[#4fa3ff]" /> Loading generated review artifacts…</span>
          </div>
        ) : draftItems.length === 0 ? (
          <div className="rounded-2xl border border-[#22304b] bg-[#0b1424] p-4 text-sm text-[#c6d2e8]">
            {emptyMessage}
          </div>
        ) : (
          draftItems.map((item) => (
            <CodeReviewItem
              key={item.key}
              item={item}
              expanded={expandedKey === item.key}
              onToggle={() => setExpandedKey((current) => (current === item.key ? null : item.key))}
              onCodeChange={(code) => updateItemCode(item.key, code)}
              onMergeKeysChange={(mergeKeys) => updateItemFields(item.key, { mergeKeys, primaryKeys: mergeKeys })}
              onApprove={() => onSetItemDecision(item.key, 'APPROVED')}
              onReject={() => onSetItemDecision(item.key, 'REJECTED')}
              onRegenerate={() => onSetItemDecision(item.key, 'REGENERATE')}
              decision={decisions[item.key] || ''}
            />
          ))
        )}
      </div>

      <div className="flex shrink-0 items-center justify-between gap-4 border-t border-[#1d2940] bg-[#101726] px-6 py-4">
        <p className="text-sm text-[#c6d2e8]">
          <span className="font-semibold text-white">{reviewedCount}</span> / {totalCount} items reviewed
        </p>
        <div className="flex items-center gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => onSetAllDecision('APPROVED')}
              aria-pressed={gateDecision === 'APPROVED'}
              className={`rounded-lg border px-3 py-2 text-xs font-bold transition-colors ${
                gateDecision === 'APPROVED'
                  ? 'border-emerald-400 bg-emerald-500/20 text-emerald-300'
                  : 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/15'
              }`}
            >
              <span className="inline-flex items-center gap-1">
                <CheckCircle2 size={13} />
                Approve Gate
              </span>
            </button>
            <button
              type="button"
              onClick={() => onSetAllDecision('REJECTED')}
              aria-pressed={gateDecision === 'REJECTED'}
              className={`rounded-lg border px-3 py-2 text-xs font-bold transition-colors ${
                gateDecision === 'REJECTED'
                  ? 'border-red-400 bg-red-500/20 text-red-300'
                  : 'border-red-500/30 bg-red-500/10 text-red-400 hover:bg-red-500/15'
              }`}
            >
              <span className="inline-flex items-center gap-1">
                <XCircle size={13} />
                Reject Gate
              </span>
            </button>
            <button
              type="button"
              onClick={() => onSetAllDecision('REGENERATE')}
              aria-pressed={gateDecision === 'REGENERATE'}
              className={`rounded-lg border px-3 py-2 text-xs font-bold transition-colors ${
                gateDecision === 'REGENERATE'
                  ? 'border-[#3f82ff] bg-[#3f82ff]/20 text-[#78a9ff]'
                  : 'border-[#3f82ff]/30 bg-[#3f82ff]/10 text-[#78a9ff] hover:bg-[#3f82ff]/15'
              }`}
            >
              <span className="inline-flex items-center gap-1">
                <RotateCcw size={13} />
                Regenerate
              </span>
            </button>
          </div>
          <button type="button" onClick={onPause} className="btn-secondary">
            Pause Pipeline
          </button>
          <button
            type="button"
            onClick={onSubmit}
            disabled={disabled}
            className="inline-flex items-center gap-2 rounded-lg bg-accent-blue px-5 py-3 text-sm font-bold text-white shadow-lg transition-colors hover:bg-blue-600 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {submitting ? <Loader2 size={15} className="animate-spin" /> : <Copy size={15} />}
            {submitting ? 'Submitting...' : submitLabel}
          </button>
        </div>
      </div>
    </motion.div>
  )
}

function CodeReviewItem({ item, expanded, onToggle, onCodeChange, onMergeKeysChange, onApprove, onReject, onRegenerate, decision }) {
  const approved = decision === 'APPROVED'
  const rejected = decision === 'REJECTED'
  const regenerate = decision === 'REGENERATE'

  return (
    <div className={`rounded-xl border bg-[#101827] p-5 transition-colors ${
      approved
        ? 'border-emerald-500/40'
        : rejected
        ? 'border-red-500/40'
        : regenerate
        ? 'border-[#3f82ff]/45'
        : 'border-[#22304b]'
    }`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <div className="flex min-w-0 items-center gap-2">
            <Copy size={15} className="flex-shrink-0 text-[#4fa3ff]" />
            <h3 className="truncate text-sm font-extrabold text-white">{item.title}</h3>
            <span className="rounded-full bg-[#334155] px-2 py-1 text-[10px] font-extrabold text-[#d7dfed]">
              {item.type}
            </span>
            {item.edited && (
              <span className="rounded-full border border-[#3f82ff]/35 bg-[#3f82ff]/10 px-2 py-1 text-[10px] font-bold text-[#78a9ff]">
                Edited in UI
              </span>
            )}
          </div>
          <div className="mt-3 text-xs text-[#91a4cb]">Queued: {item.queuedAt || 'Pending review'}</div>
        </div>
        <button
          type="button"
          onClick={onToggle}
          className="inline-flex h-10 items-center gap-2 rounded-lg bg-[#202b3a] px-4 text-sm font-bold text-[#c6d2e8] transition-colors hover:bg-[#263449] hover:text-white"
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          {expanded ? 'Collapse' : 'Edit'}
        </button>
      </div>

      <CodeReviewSummary item={item} onMergeKeysChange={onMergeKeysChange} />

      <button
        type="button"
        onClick={onToggle}
        className="mt-4 inline-flex items-center gap-2 text-xs font-bold text-[#c6d2e8] hover:text-white"
      >
        <span className="text-[10px] text-[#91a4cb]">{expanded ? '⌃' : '⌄'}</span>
        {expanded ? 'Hide code' : 'Preview code'}
      </button>

      {expanded && (
        <div className="mt-3">
          <div className="mb-2 flex justify-end gap-2">
            <button
              type="button"
              onClick={() => copyCode(item.code)}
              className="inline-flex items-center gap-1 rounded-md border border-[#22304b] px-2 py-1 text-[10px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
            >
              <Copy size={11} />
              Copy
            </button>
            <button
              type="button"
              onClick={() => downloadCode(item)}
              className="inline-flex items-center gap-1 rounded-md border border-[#22304b] px-2 py-1 text-[10px] font-semibold text-[#aab8d0] hover:border-[#3f82ff] hover:text-white"
            >
              <Download size={11} />
              Download
            </button>
          </div>
          <textarea
            value={item.code || '# Generated code is not available yet.'}
            onChange={(event) => onCodeChange(event.target.value)}
            spellCheck={false}
            className="min-h-[180px] w-full resize-y rounded-lg border border-[#22304b] bg-[#09111f] p-3 font-mono text-xs leading-relaxed text-[#9bd384] outline-none transition-colors focus:border-[#3f82ff]"
          />
          <div className="mt-2 text-[11px] text-[#91a4cb]">
            Edits are local to this review screen. Copy/download uses the edited draft; backend approval still submits the gate decision.
          </div>
        </div>
      )}

      <div className="mt-4 grid gap-2 md:grid-cols-3">
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation()
            onApprove()
          }}
          aria-pressed={approved}
          className={`inline-flex items-center justify-center gap-2 rounded-lg border px-4 py-2 text-sm font-bold transition-colors ${
            approved
              ? 'border-emerald-400 bg-emerald-500/20 text-emerald-300'
              : 'border-emerald-500/35 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/15'
          }`}
        >
          ✓ Approve
        </button>
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation()
            onReject()
          }}
          aria-pressed={rejected}
          className={`inline-flex items-center justify-center gap-2 rounded-lg border px-4 py-2 text-sm font-bold transition-colors ${
            rejected
              ? 'border-red-400 bg-red-500/20 text-red-300'
              : 'border-red-500/35 bg-red-500/10 text-red-400 hover:bg-red-500/15'
          }`}
        >
          × Reject
        </button>
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation()
            onRegenerate()
          }}
          aria-pressed={regenerate}
          className={`inline-flex items-center justify-center gap-2 rounded-lg border px-4 py-2 text-sm font-bold transition-colors ${
            regenerate
              ? 'border-[#3f82ff] bg-[#3f82ff]/20 text-[#78a9ff]'
              : 'border-[#3f82ff]/35 bg-[#3f82ff]/10 text-[#78a9ff] hover:bg-[#3f82ff]/15'
          }`}
        >
          Regenerate
        </button>
      </div>
    </div>
  )
}

function CodeReviewSummary({ item, onMergeKeysChange }) {
  const keys = item.mergeKeys || item.primaryKeys || []
  const canEditMergeKeys = item.type === 'BRONZE' || item.mergeKeysEditable
  const fields = [
    ['Target', item.target],
    ['Source', item.source],
    ['Strategy', item.strategy],
    ['Watermark', item.watermark],
  ].filter(([, value]) => value)

  return (
    <div className="mt-4 rounded-xl border border-[#22304b] bg-[#0b1424] p-4">
      {(keys.length > 0 || canEditMergeKeys) && (
        <div>
          <div className="mb-2 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wide text-[#9ca9bd]">
            <KeyRound size={13} className="text-[#69a0ff]" />
            {item.type === 'BRONZE' ? 'Resolve Merge Keys' : 'Resolved Merge Keys'}
          </div>
          {canEditMergeKeys ? (
            <input
              value={keys.join(', ')}
              onChange={(event) => onMergeKeysChange?.(
                event.target.value.split(',').map((value) => value.trim()).filter(Boolean)
              )}
              className="h-10 w-full rounded-lg border border-[#31415f] bg-[#0a1220] px-3 font-mono text-xs text-[#d7e2f2] outline-none focus:border-[#78a9ff]"
              placeholder="ClaimID, PaymentID"
            />
          ) : (
            <div className="flex flex-wrap gap-2">
              {keys.map((key) => (
                <span key={`${item.key}:${key}`} className="rounded-md border border-[#2d64c3] bg-[#122a52] px-2 py-1 text-[10px] font-bold text-[#69a0ff]">
                  {key}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {fields.length > 0 && (
        <div className={`${keys.length ? 'mt-4 border-t border-[#22304b] pt-3' : ''} grid gap-2 text-xs text-[#c6d2e8] md:grid-cols-2`}>
          {fields.map(([label, value]) => (
            <div key={`${item.key}:${label}`} className="rounded-lg border border-[#1d2940] bg-[#101827] px-3 py-2">
              <div className="text-[10px] font-bold uppercase text-[#7787a3]">{label}</div>
              <div className="mt-1 break-all font-semibold text-[#d7e2f2]">{value}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function buildBronzeCodeReviewItems(feeds) {
  return feeds.map((feed, index) => ({
    key: `bronze-${feed.entity || feed.feed_name || feed.table_name || feed.vendor || 'script'}-${index}`,
    title: [feed.vendor, feed.entity || feed.feed_name || feed.table_name].filter(Boolean).join('.') || `bronze_script_${index + 1}`,
    type: 'BRONZE',
    queuedAt: formatReviewTimestamp(feed.queued_at || feed.created_at || feed.updated_at),
    code: feed.generated_bronze_script || feed.script_body || JSON.stringify(stripEmptyReviewFields(feed), null, 2),
    fileName: `${feed.entity || feed.feed_name || `bronze_script_${index + 1}`}.py`,
    primaryKeys: feed.primary_keys || feed.merge_keys || [],
    target: feed.target_table || feed.bronze_output_path,
    source: feed.landing_path || feed.source_type,
    strategy: feed.file_format || 'Bronze ingestion',
    watermark: feed.watermark_column,
    reviewPayload: feed,
  }))
}

function buildSilverMergeKeyReviewItems(feeds) {
  return feeds.map((feed, index) => ({
    key: `silver-merge-key-${feed.entity || feed.feed_name || feed.table_name || feed.vendor || 'review'}-${index}`,
    title: [feed.vendor, feed.entity || feed.feed_name || feed.table_name].filter(Boolean).join('.') || `merge_key_review_${index + 1}`,
    type: 'MERGE_KEY',
    mergeKeysEditable: true,
    queuedAt: formatReviewTimestamp(feed.queued_at || feed.created_at || feed.updated_at),
    code: feed.generated_bronze_script || feed.script_body || JSON.stringify(stripEmptyReviewFields(feed), null, 2),
    fileName: `${feed.entity || feed.feed_name || `merge_key_review_${index + 1}`}.py`,
    primaryKeys: feed.primary_keys || feed.merge_keys || [],
    mergeKeys: feed.merge_keys || feed.primary_keys || [],
    target: feed.target_table || feed.bronze_output_path,
    source: feed.landing_path || feed.source_type,
    strategy: 'Silver merge-key approval',
    watermark: feed.watermark_column,
    reviewPayload: feed,
  }))
}

function buildSilverCodeReviewItems(items) {
  return items.filter((item) => !isMergeKeyReviewItem(item)).map((item, index) => {
    const title = item.script_name || item.entity || item.target_table || item.table_name || `silver_script_${index + 1}`
    return {
      key: `silver-${title}-${index}`,
      title,
      type: 'SILVER',
      queuedAt: formatReviewTimestamp(item.queued_at || item.created_at || item.updated_at),
      code: item.generated_silver_script || item.script_body || JSON.stringify(stripEmptyReviewFields(item), null, 2),
      fileName: `${title}.py`,
      primaryKeys: item.primary_keys || item.merge_keys || [],
      mergeKeys: item.merge_keys || item.primary_keys || [],
      mergeKeySource: item.merge_key_source,
      target: item.silver_target || item.target_table || item.silver_table,
      source: item.bronze_source || item.source_table || item.bronze_table,
      strategy: item.merge_strategy || 'Silver transform',
      watermark: item.watermark_column,
      reviewPayload: item,
    }
  })
}

function buildCodeReviewArtifact(layer, draftItems, review, decisions = {}) {
  const items = Array.isArray(draftItems) ? draftItems : []
  if (layer === 'bronze') {
    return {
      ...(review?.bronze_review_artifact || {}),
      feeds: items.map((item) => ({
        ...(item.reviewPayload || {}),
        generated_bronze_script: item.code,
        script_body: item.code,
        primary_keys: item.primaryKeys || item.mergeKeys || [],
        merge_keys: item.mergeKeys || item.primaryKeys || [],
        review_status: decisions[item.key] || item.reviewStatus || 'PENDING',
      })),
    }
  }

  if (layer === 'silver_merge_key') {
    return {
      ...(review?.silver_merge_key_review_artifact || {}),
      feeds: items.map((item) => ({
        ...(item.reviewPayload || {}),
        generated_bronze_script: item.code,
        script_body: item.code,
        primary_keys: item.primaryKeys || item.mergeKeys || [],
        merge_keys: item.mergeKeys || item.primaryKeys || [],
        review_status: decisions[item.key] || item.reviewStatus || 'PENDING',
        review_type: 'silver_merge_key',
      })),
    }
  }

  return {
    ...(review?.silver_review_artifact || {}),
    items: items.map((item) => ({
      ...(item.reviewPayload || {}),
      generated_silver_script: item.code,
      script_body: item.code,
      primary_keys: item.primaryKeys || item.mergeKeys || [],
      merge_keys: item.mergeKeys || item.primaryKeys || [],
      merge_key_source: item.mergeKeySource || item.reviewPayload?.merge_key_source || 'reviewed_gate4',
      review_status: decisions[item.key] || item.reviewStatus || 'PENDING',
    })),
  }
}

function patchMergeKeysInCode(code, mergeKeys) {
  if (!Array.isArray(mergeKeys) || typeof code !== 'string') return code
  const value = JSON.stringify(mergeKeys)
  if (/^KEY_COLUMNS\s*=.*$/m.test(code)) {
    return code.replace(/^KEY_COLUMNS\s*=.*$/m, `KEY_COLUMNS = ${value}`)
  }
  if (/^primary_keys\s*=.*$/im.test(code)) {
    return code.replace(/^primary_keys\s*=.*$/im, `primary_keys = ${value}`)
  }
  return code
}

function isMergeKeyReviewItem(item) {
  const title = String(item?.script_name || item?.entity || item?.target_table || item?.table_name || item?.type || item?.item_type || '').toLowerCase()
  return Boolean(
    title.includes('merge_key') ||
    title.includes('merge key') ||
    item?.merge_key_source ||
    item?.merge_key_candidates ||
    item?.review_type === 'merge_key' ||
    item?.item_type === 'MERGE_KEY'
  )
}

function getCodeReviewGateDecision(items, decisions) {
  const itemDecisions = items.map((item) => decisions[item.key]).filter(Boolean)
  if (!itemDecisions.length) return ''
  if (itemDecisions.every((decision) => decision === 'REJECTED')) return 'REJECTED'
  if (itemDecisions.includes('REGENERATE')) return 'REGENERATE'
  return 'APPROVED'
}

function stripEmptyReviewFields(value) {
  if (!value || typeof value !== 'object') return value
  return Object.fromEntries(
    Object.entries(value).filter(([, fieldValue]) => {
      if (fieldValue === null || fieldValue === undefined || fieldValue === '') return false
      if (Array.isArray(fieldValue) && fieldValue.length === 0) return false
      return true
    })
  )
}

function formatReviewTimestamp(value) {
  if (!value) return new Date().toLocaleString('en-IN')
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString('en-IN')
}

async function copyCode(value) {
  await navigator.clipboard.writeText(value || '')
}

function downloadCode(item) {
  const blob = new Blob([item.code || ''], { type: 'text/plain;charset=utf-8' })
  const url = window.URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  const fallbackName = `${item.title || 'generated_script'}`.replace(/[^\w.-]+/g, '_').toLowerCase()
  const requestedName = item.fileName || fallbackName
  anchor.href = url
  anchor.download = requestedName.endsWith('.py') ? requestedName : `${requestedName}.py`
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
  window.URL.revokeObjectURL(url)
}
