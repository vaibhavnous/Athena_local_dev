// ============================================================
// Athena Mock Data — used when server is unavailable / demo mode
// ============================================================

export const MOCK_KPIS_LIST = [
  {
    "id": "kpi_001",
    "kpi_name": "Data Centralization Rate",
    "kpi_description": "Measures the percentage of claims and policy data successfully centralized in the data foundation, ensuring a single source of truth for analytics and compliance.",
    "ai_confidence_score": 0.9,
    "derivation_type": "implicit",
    "source_requirement_ref": "business_objective",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "id": "kpi_002",
    "kpi_name": "Data Traceability Coverage",
    "kpi_description": "Assesses the proportion of claims and policy records with complete traceability from source to centralized repository, supporting compliance and audit requirements.",
    "ai_confidence_score": 0.85,
    "derivation_type": "implicit",
    "source_requirement_ref": "business_objective",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "id": "kpi_003",
    "kpi_name": "Identifier Consistency Rate",
    "kpi_description": "Measures the percentage of records with consistent identifiers across all integrated systems, ensuring reliable data linkage and reducing duplication.",
    "ai_confidence_score": 0.95,
    "derivation_type": "explicit",
    "source_requirement_ref": "constraints",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "id": "kpi_004",
    "kpi_name": "Bronze Layer Transformation Incidents",
    "kpi_description": "Counts the number of unauthorized data transformations detected at the bronze layer, ensuring raw data integrity as per architectural constraints.",
    "ai_confidence_score": 0.9,
    "derivation_type": "explicit",
    "source_requirement_ref": "constraints",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "id": "kpi_005",
    "kpi_name": "Regulatory Reporting Readiness",
    "kpi_description": "Measures the percentage of regulatory, actuarial, underwriting, and claims management reports that can be generated on an ad hoc basis from the centralized data foundation.",
    "ai_confidence_score": 0.8,
    "derivation_type": "implicit",
    "source_requirement_ref": "reporting_frequency",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "id": "kpi_006",
    "kpi_name": "Data Domain Coverage",
    "kpi_description": "Assesses the extent to which all relevant claims and policy data domains are included in the centralized data foundation.",
    "ai_confidence_score": 0.8,
    "derivation_type": "implicit",
    "source_requirement_ref": "data_domains",
    "grounding_status": "GROUNDING_WEAK"
  }
]

export const MOCK_RUNS = [
  {
    id: 'run_a3f8c2',
    brd_filename: 'Insurance_BRD_v3.txt',
    status: 'HITL_WAIT',
    provider: 'azure_openai',
    deployment: 'gpt-4o-athena',
    started_at: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
    completed_at: null,
    cache_hit: 'L2_FUZZY',
    cache_score: 0.947,
    extraction_path: 'CACHED_L2',
    total_tokens: 48320,
    total_cost: 1.24,
    stages: [
      {
        id: 'stage_01',
        name: 'BRD Ingest',
        icon: 'FileText',
        status: 'PENDING',
        tokens: 4820,
        cost: 0.048,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 8).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 7.5).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0, max_tokens: 2048 },
        jobId: 1106200200113657
      },
      {
        id: 'stage_02',
        name: 'Memory Check',
        icon: 'ClipboardList',
        status: 'PENDING',
        tokens: 18430,
        cost: 0.46,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 7.5).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 6).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0.1, max_tokens: 4096 },
        jobId:1069281733285393
      },
      {
        id: 'stage_03',
        name: 'Requirement Extraction',
        icon: 'ClipboardList',
        status: 'PENDING',
        tokens: 18430,
        cost: 0.46,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 7.5).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 6).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0.1, max_tokens: 4096 },
        jobId: 699232417060802

      },
      {
        id: 'stage_04',
        name: 'KPI Extraction',
        icon: 'BarChart2',
        status: 'PENDING',
        tokens: 22100,
        cost: 0.61,
        attempts: 2,
        started_at: new Date(Date.now() - 1000 * 60 * 6).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 4).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0.2, max_tokens: 8192 },
        jobId: 682268475558864
      },
      {
        id: 'stage_hitl',
        name: 'KPI Review',
        icon: 'Users',
        status:  'PENDING',//'HITL_WAIT',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: new Date(Date.now() - 1000 * 60 * 4).toISOString(),
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 59144764005965
      },
      {
        id: 'stage_06',
        name: 'Table Nomination',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 987084467974814
      },
      {
        id: 'stage_07',
        name: 'Metadata Discovery',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 508520645629402
      },
      {
        id: 'stage_08',
        name: 'Column Profiling',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 338568496331713
      },
      {
        id: 'stage_09',
        name: 'Semantic Enrichment',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 367893927842326
      },
      {
        id: 'stage_10',
        name: 'Bronze Code Generation',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 504860645935481
      },
      {
        id: 'stage_11',
        name: 'Silver Code Generation',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 692626552193576
      },
      {
        id: 'stage_12',
        name: 'Gold Code Generation',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 46541173073199
      },
      {
        id: 'stage_13',
        name: 'Lakehouse Creation',
        icon: 'Database',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null,
        jobId: 1116350299661967
      }
    ],
    requirements: {
      objective: 'Build a comprehensive insurance analytics dashboard to track policy performance, claims processing efficiency, and customer retention metrics.',
      data_domains: ['Policy Management', 'Claims Processing', 'Customer Data', 'Financial Data', 'Actuarial Data'],
      reporting_frequency: 'Daily with monthly aggregations',
      target_audience: 'Insurance Operations Team, Actuarial Department, C-Suite',
      constraints: [
        'GDPR compliance for EU customer data',
        'Data retention max 7 years',
        'Real-time claims under 5s SLA',
        'SOX compliance for financial metrics'
      ],
      faithfulness_score: 0.94,
      retry_count: 0
    },
    kpis: [
      {
    "kpi_name": "Data Centralization Rate",
    "kpi_description": "Measures the percentage of claims and policy data successfully centralized in the data foundation, ensuring a single source of truth for analytics and compliance.",
    "ai_confidence_score": 0.9,
    "derivation_type": "implicit",
    "source_requirement_ref": "business_objective",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "kpi_name": "Data Traceability Coverage",
    "kpi_description": "Assesses the proportion of claims and policy records with complete traceability from source to centralized repository, supporting compliance and audit requirements.",
    "ai_confidence_score": 0.85,
    "derivation_type": "implicit",
    "source_requirement_ref": "business_objective",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "kpi_name": "Identifier Consistency Rate",
    "kpi_description": "Measures the percentage of records with consistent identifiers across all integrated systems, ensuring reliable data linkage and reducing duplication.",
    "ai_confidence_score": 0.95,
    "derivation_type": "explicit",
    "source_requirement_ref": "constraints",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "kpi_name": "Bronze Layer Transformation Incidents",
    "kpi_description": "Counts the number of unauthorized data transformations detected at the bronze layer, ensuring raw data integrity as per architectural constraints.",
    "ai_confidence_score": 0.9,
    "derivation_type": "explicit",
    "source_requirement_ref": "constraints",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "kpi_name": "Regulatory Reporting Readiness",
    "kpi_description": "Measures the percentage of regulatory, actuarial, underwriting, and claims management reports that can be generated on an ad hoc basis from the centralized data foundation.",
    "ai_confidence_score": 0.8,
    "derivation_type": "implicit",
    "source_requirement_ref": "reporting_frequency",
    "grounding_status": "GROUNDING_WEAK"
  },
  {
    "kpi_name": "Data Domain Coverage",
    "kpi_description": "Assesses the extent to which all relevant claims and policy data domains are included in the centralized data foundation.",
    "ai_confidence_score": 0.8,
    "derivation_type": "implicit",
    "source_requirement_ref": "data_domains",
    "grounding_status": "GROUNDING_WEAK"
  }
    ]
  },
  {
    id: 'run_b7e1d3',
    brd_filename: 'Sales_Dashboard_BRD.txt',
    status: 'COMPLETED',
    provider: 'azure_openai',
    deployment: 'gpt-4o-athena',
    started_at: new Date(Date.now() - 1000 * 60 * 45).toISOString(),
    completed_at: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
    cache_hit: 'L1_EXACT',
    cache_score: 1.0,
    extraction_path: 'CACHED_L1',
    total_tokens: 31200,
    total_cost: 0.78,
    stages: [
      {
        id: 'stage_01',
        name: 'Stage 01 — BRD Ingest',
        icon: 'FileText',
        status: 'COMPLETED',
        tokens: 3200,
        cost: 0.032,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 45).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 44).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0, max_tokens: 2048 }
      },
      {
        id: 'stage_02',
        name: 'Stage 02 — Requirements',
        icon: 'ClipboardList',
        status: 'COMPLETED',
        tokens: 12800,
        cost: 0.32,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 44).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 42).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0.1, max_tokens: 4096 }
      },
      {
        id: 'stage_03',
        name: 'Stage 03 — KPI Extraction',
        icon: 'BarChart2',
        status: 'COMPLETED',
        tokens: 14800,
        cost: 0.37,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 42).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 38).toISOString(),
        error: null,
        prompt_metadata: { model: 'gpt-4o', temperature: 0.2, max_tokens: 8192 }
      },
      {
        id: 'stage_hitl',
        name: 'HITL Gate — KPI Review',
        icon: 'Users',
        status: 'COMPLETED',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: new Date(Date.now() - 1000 * 60 * 38).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 35).toISOString(),
        error: null,
        prompt_metadata: null
      },
      {
        id: 'stage_final',
        name: 'Stage 04 — Finalize',
        icon: 'CheckCircle',
        status: 'COMPLETED',
        tokens: 0,
        cost: 0.058,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 35).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
        error: null,
        prompt_metadata: null
      }
    ],
    requirements: {
      objective: 'Create a real-time sales performance dashboard for the enterprise sales team with pipeline tracking and forecasting.',
      data_domains: ['CRM Data', 'Opportunity Pipeline', 'Revenue Data', 'Activity Tracking'],
      reporting_frequency: 'Real-time with weekly summaries',
      target_audience: 'Sales Managers, Account Executives, VP of Sales',
      constraints: [
        'Salesforce CRM integration required',
        'Mobile-first design',
        'Sub-2s load time requirement'
      ],
      faithfulness_score: 0.98,
      retry_count: 0
    },
    kpis: [
      {
        id: 'kpi_101',
        name: 'Pipeline Coverage Ratio',
        definition: 'Ratio of total pipeline value to quota, indicating whether sufficient opportunities exist to achieve targets.',
        category: 'Sales',
        domain: 'Opportunity Pipeline',
        confidence: 0.95,
        status: 'APPROVED',
        grounded: true,
        evidence: 'BRD Section 2: "Track pipeline coverage at 3x quota minimum..."',
        explicit: true,
        decision: 'APPROVED',
        reviewer: 'analyst_01',
        reviewed_at: new Date(Date.now() - 1000 * 60 * 36).toISOString()
      },
      {
        id: 'kpi_102',
        name: 'Win Rate',
        definition: 'Percentage of opportunities that result in closed-won deals, measured over rolling 90-day period.',
        category: 'Sales',
        domain: 'CRM Data',
        confidence: 0.93,
        status: 'APPROVED',
        grounded: true,
        evidence: 'BRD Section 2: "Win rate benchmarks are critical success metrics..."',
        explicit: true,
        decision: 'APPROVED',
        reviewer: 'analyst_01',
        reviewed_at: new Date(Date.now() - 1000 * 60 * 36).toISOString()
      },
      {
        id: 'kpi_103',
        name: 'Average Deal Size',
        definition: 'Mean value of closed-won opportunities in USD, tracked monthly with YoY comparison.',
        category: 'Revenue',
        domain: 'Revenue Data',
        confidence: 0.89,
        status: 'EDITED',
        grounded: true,
        evidence: 'BRD Section 3.1: "Revenue metrics including average contract value..."',
        explicit: false,
        decision: 'EDITED',
        reviewer: 'analyst_02',
        reviewed_at: new Date(Date.now() - 1000 * 60 * 35).toISOString()
      }
    ]
  },
  {
    id: 'run_c9f2a1',
    brd_filename: 'Logistics_Operations_BRD.docx',
    status: 'FAILED',
    provider: 'anthropic',
    deployment: null,
    started_at: new Date(Date.now() - 1000 * 60 * 120).toISOString(),
    completed_at: new Date(Date.now() - 1000 * 60 * 115).toISOString(),
    cache_hit: 'NONE',
    cache_score: 0,
    extraction_path: 'FULL_EXTRACTION',
    total_tokens: 8400,
    total_cost: 0.21,
    stages: [
      {
        id: 'stage_01',
        name: 'Stage 01 — BRD Ingest',
        icon: 'FileText',
        status: 'COMPLETED',
        tokens: 4200,
        cost: 0.105,
        attempts: 1,
        started_at: new Date(Date.now() - 1000 * 60 * 120).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 119).toISOString(),
        error: null,
        prompt_metadata: { model: 'claude-3-5-sonnet-20241022', temperature: 0, max_tokens: 2048 }
      },
      {
        id: 'stage_02',
        name: 'Stage 02 — Requirements',
        icon: 'ClipboardList',
        status: 'FAILED',
        tokens: 4200,
        cost: 0.105,
        attempts: 3,
        started_at: new Date(Date.now() - 1000 * 60 * 119).toISOString(),
        completed_at: new Date(Date.now() - 1000 * 60 * 115).toISOString(),
        error: 'API rate limit exceeded after 3 attempts. Last error: 429 Too Many Requests. Retry budget exhausted.',
        prompt_metadata: { model: 'claude-3-5-sonnet-20241022', temperature: 0.1, max_tokens: 4096 }
      },
      {
        id: 'stage_03',
        name: 'Stage 03 — KPI Extraction',
        icon: 'BarChart2',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null
      },
      {
        id: 'stage_hitl',
        name: 'HITL Gate — KPI Review',
        icon: 'Users',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null
      },
      {
        id: 'stage_final',
        name: 'Stage 04 — Finalize',
        icon: 'CheckCircle',
        status: 'PENDING',
        tokens: 0,
        cost: 0,
        attempts: 0,
        started_at: null,
        completed_at: null,
        error: null,
        prompt_metadata: null
      }
    ],
    requirements: null,
    kpis: []
  }
]

export const MOCK_HITL_QUEUE = MOCK_KPIS_LIST;//MOCK_RUNS[0].kpis

export const MOCK_KPI_LIBRARY = [
  ...MOCK_RUNS[0].kpis.map(k => ({ ...k, run_id: 'run_a3f8c2', brd_filename: 'Insurance_BRD_v3.txt', recorded_at: new Date(Date.now() - 1000 * 60 * 4).toISOString() })),
  ...MOCK_RUNS[1].kpis.map(k => ({ ...k, run_id: 'run_b7e1d3', brd_filename: 'Sales_Dashboard_BRD.txt', recorded_at: new Date(Date.now() - 1000 * 60 * 30).toISOString() })),
  {
    id: 'kpi_201',
    name: 'Days Sales Outstanding',
    definition: 'Average number of days to collect payment after a sale, measured over 30-day rolling window.',
    category: 'Financial',
    domain: 'Revenue Data',
    confidence: 0.82,
    status: 'APPROVED',
    grounded: true,
    evidence: 'Derived from financial BRD requirements section 4.3',
    explicit: false,
    decision: 'APPROVED',
    reviewer: 'analyst_03',
    reviewed_at: new Date(Date.now() - 1000 * 60 * 60 * 2).toISOString(),
    run_id: 'run_b7e1d3',
    brd_filename: 'Sales_Dashboard_BRD.txt',
    recorded_at: new Date(Date.now() - 1000 * 60 * 60 * 2).toISOString()
  },
  {
    id: 'kpi_202',
    name: 'Customer Churn Rate',
    definition: 'Monthly percentage of customers who cancel or do not renew their subscription/policy.',
    category: 'Customer',
    domain: 'Customer Data',
    confidence: 0.78,
    status: 'REJECTED',
    grounded: false,
    evidence: 'Inferred from retention goals in BRD section 1',
    explicit: false,
    decision: 'REJECTED',
    reviewer: 'analyst_01',
    reviewed_at: new Date(Date.now() - 1000 * 60 * 60 * 3).toISOString(),
    rejection_reason: 'Duplicate of Policy Renewal Rate — measures same concept differently',
    run_id: 'run_a3f8c2',
    brd_filename: 'Insurance_BRD_v3.txt',
    recorded_at: new Date(Date.now() - 1000 * 60 * 60 * 3).toISOString()
  },
  {
    id: 'kpi_203',
    name: 'Inventory Turnover Rate',
    definition: 'Number of times inventory is sold or used in a given time period, typically annually.',
    category: 'Operational',
    domain: 'Supply Chain',
    confidence: 0.91,
    status: 'AUTO_SUPPRESSED',
    grounded: true,
    evidence: 'Logistics BRD section 7: inventory management metrics',
    explicit: true,
    decision: 'AUTO_SUPPRESSED',
    reviewer: 'system',
    reviewed_at: new Date(Date.now() - 1000 * 60 * 60 * 5).toISOString(),
    run_id: 'run_c9f2a1',
    brd_filename: 'Logistics_Operations_BRD.docx',
    recorded_at: new Date(Date.now() - 1000 * 60 * 60 * 5).toISOString()
  }
]

// Generate 30 days of cost data
const generateCostData = () => {
  const data = []
  const now = new Date()
  for (let i = 29; i >= 0; i--) {
    const date = new Date(now)
    date.setDate(date.getDate() - i)
    const stage02Cost = parseFloat((Math.random() * 0.8 + 0.2).toFixed(3))
    const stage03Cost = parseFloat((Math.random() * 1.2 + 0.3).toFixed(3))
    const totalCost = parseFloat((stage02Cost + stage03Cost + Math.random() * 0.1).toFixed(3))
    const tokens = Math.floor(Math.random() * 40000 + 15000)
    data.push({
      date: date.toISOString().split('T')[0],
      stage02Cost,
      stage03Cost,
      totalCost,
      tokens
    })
  }
  return data
}

export const MOCK_COST_DATA = generateCostData()

export const MOCK_REQUIREMENTS = MOCK_RUNS[0].requirements

export const MOCK_SETTINGS = {
  provider: 'azure_openai',
  azure_endpoint: 'https://athena-openai.openai.azure.com/',
  azure_deployment: 'gpt-4o-athena',
  openai_model: 'gpt-4o',
  anthropic_model: 'claude-3-5-sonnet-20241022',
  budget: 5.0,
  maxKpis: 25,
  devMode: true,
  dataDir: '~/.athena',
  pythonCmd: 'python'
}
