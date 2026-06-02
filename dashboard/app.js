(function () {
  const e = React.createElement;
  const ReactFlowLib = window.ReactFlow || {};
  const Flow = ReactFlowLib.default || ReactFlowLib.ReactFlow || ReactFlowLib;
  const ReactFlowProvider = ReactFlowLib.ReactFlowProvider || React.Fragment;
  const Background = ReactFlowLib.Background || (() => null);
  const Controls = ReactFlowLib.Controls || (() => null);
  const MiniMap = ReactFlowLib.MiniMap || (() => null);
  const hasReactFlow = !!Flow && (typeof Flow === "function" || typeof Flow === "object");

  const TABS = [
    ["workflows", "Workflows"],
    ["metadata", "Metadata Map"],
    ["control", "Control Links"],
    ["flow", "Run Flow"],
    ["diagram", "Workflow Diagram"],
    ["process", "Process Model"],
    ["developer", "Developer Model"],
    ["rows", "Target Rows"],
    ["templates", "Templates"],
    ["docs", "Documentation"],
  ];

  // Known dashboard snapshots. Default stays the customer-demo file so the
  // existing demo view is unchanged. Use ?data=<file> or the header dropdown
  // to load another snapshot without hand-editing this file.
  const KNOWN_DATA_FILES = [
    ["demo-workflow.json", "Customer / Transaction demo"],
    ["policy-claims-workflow.json", "Insurance Policy / Claims"],
  ];
  const DEFAULT_DATA_FILE = KNOWN_DATA_FILES[0][0];

  function initialDataFile() {
    try {
      const requested = new URLSearchParams(window.location.search).get("data");
      if (requested) {
        // Basename only: defends against ../ path traversal in the query param.
        const safe = requested.split("/").pop();
        if (safe) return safe;
      }
    } catch (err) {
      // window/URLSearchParams unavailable (non-browser) — fall through.
    }
    return DEFAULT_DATA_FILE;
  }

  const TEMPLATES = [
    {
      id: "airflow-glue-raw-to-silver",
      title: "Airflow + Glue raw parquet to silver",
      stage: "canonicalization",
      summary: "Read one raw parquet file, validate its schema, transform it, write silver parquet to S3, and record the exact input and output metadata.",
      purpose: "Use this when an Airflow task launches Glue to turn one raw file into one reusable silver dataset. The control rows prove which physical file was read, which validation and transformation stages ran, and which silver object downstream jobs are allowed to consume.",
      commentary: [
        "The raw file identity should already exist before Glue starts. Glue receives source_file_id so lineage is anchored to cp.file_catalogue instead of an ambiguous S3 path.",
        "The run row is the logical task execution. The stage rows are the operational audit trail, so a restart can see whether validation, transformation, or publishing failed.",
        "The lineage link is created only after the silver write succeeds. That keeps downstream discovery from picking up an output that was only partially written.",
      ],
      inputs: [
        "cp.file_catalogue.file_id for the raw file",
        "raw S3 path and content hash",
        "workflow_run_id and task attempt from Airflow",
      ],
      writes: [
        "cp.run_log: one logical run for the raw-to-silver task",
        "cp.run_stage_log: validate_schema, transform, write_silver",
        "Output link (cp.output_link): one output for the silver S3 object",
        "Input edge (cp.input_edge): source_file_id points back to the raw file",
      ],
      code: `# Airflow task
# Purpose: pass Airflow identity and the registered raw file_id into Glue.
# These values let Glue write deterministic control rows instead of inventing
# lineage from paths after the fact.
glue_raw_customer_to_silver = GlueJobOperator(
    task_id="customer_raw_to_silver",
    job_name="ods-raw-to-silver",
    script_args={
        "--workflow_run_id": "{{ dag_run.run_id }}",
        "--pipeline_type": "canonicalization",
        "--domain": "sales",
        "--dataset": "customer",
        "--business_date": "{{ ds }}",
        "--source_file_id": "{{ ti.xcom_pull(task_ids='register_customer_raw', key='file_id') }}",
        "--raw_path": "{{ ti.xcom_pull(task_ids='register_customer_raw', key='raw_path') }}",
        "--target_path": "s3://ods/silver/customer/business_date={{ ds }}/",
        "--attempt": "{{ ti.try_number }}",
    },
)

# Glue job skeleton
# start_or_resume_run creates the cp.run_log row or reuses it on retry.
# task_key is the stable business task identity; attempt is the Airflow try.
run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key="customer_raw_to_silver",
    pipeline_type=args["pipeline_type"],
    domain=args["domain"],
    dataset=args["dataset"],
    business_date=args["business_date"],
    attempt=args["attempt"],
)

# cp.run_stage_log: validation starts here. If this fails, restart/debugging
# can see the file was read but not accepted as schema-valid.
control.start_stage(run.run_id, "validate_schema")
raw_df = spark.read.parquet(args["raw_path"])
validate_schema(raw_df, expected_customer_schema)
control.finish_stage(run.run_id, "validate_schema", record_count_in=raw_df.count(), record_count_out=raw_df.count())

# cp.run_stage_log: transformation is tracked separately from validation so a
# rerun can distinguish bad input from bad business logic.
control.start_stage(run.run_id, "transform")
silver_df = canonicalize_customer(raw_df)
control.finish_stage(run.run_id, "transform", record_count_in=raw_df.count(), record_count_out=silver_df.count())

# cp.run_stage_log: publish starts only when the dataframe is ready to write.
control.start_stage(run.run_id, "write_silver")
silver_df.write.mode("overwrite").parquet(args["target_path"])

# cp.lineage_link: this is the silver output that downstream jobs discover.
# cp.lineage_edge: source_file_id anchors this output to the immutable raw file.
link_id = control.create_lineage_link(
    consumer_run_id=run.run_id,
    edge_type="raw_to_silver",
    target_ref={"kind": "s3", "path": args["target_path"], "format": "parquet"},
    record_count=silver_df.count(),
    transform_version="customer_raw_to_silver:v1",
    edges=[{"source_file_id": args["source_file_id"], "record_count": raw_df.count()}],
)
control.finish_stage(run.run_id, "write_silver", record_count_in=silver_df.count(), record_count_out=silver_df.count())

# cp.run_log: finish last, after the output link exists and the stage succeeded.
control.finish_run(run.run_id, record_count_in=raw_df.count(), record_count_out=silver_df.count())`,
    },
    {
      id: "raw-file-registration",
      title: "Raw file registration",
      stage: "ingestion",
      summary: "Create the file identity before processing. This is the anchor used by every downstream lineage edge.",
      purpose: "Use this at the boundary where data first arrives. It gives every physical file a stable file_id, making refeed, duplicate detection, replay, and source-level lineage possible even if paths are reused or filenames look similar.",
      commentary: [
        "This step should be small and deterministic: inspect the object, calculate or read its hash, insert the catalogue row, and pass file_id forward.",
        "A refeed should create a new file_id and optionally point back to the file it replaces. Do not overwrite the old file row; history is the point.",
        "Visibility can mark which file is active for business processing, but the file catalogue itself should remain immutable.",
      ],
      inputs: [
        "S3 raw object path",
        "file size, md5/content hash, arrival timestamp",
        "business date inferred from object key or manifest",
      ],
      writes: [
        "cp.file_catalogue: one immutable row per physical file",
        "cp.target_visibility or active-slice table: marks whether this file is currently active for business use",
        "Airflow XCom: file_id and raw_path for the Glue task",
      ],
      code: `# Airflow Python task
# Purpose: create a durable file_id before any transformation starts.
# Every later lineage edge should point to this row instead of only an S3 path.
def register_raw_file(**context):
    raw_path = context["dag_run"].conf["raw_customer_path"]
    stats = s3_object_stats(raw_path)

    # cp.file_catalogue: immutable identity for this physical raw object.
    # refeed_of_file_id links a replacement file to the older business input.
    file_id = control.register_file(
        domain="sales",
        dataset="customer",
        business_date=context["ds"],
        s3_raw_path=raw_path,
        file_md5=stats.md5,
        file_size_bytes=stats.size,
        received_at=stats.last_modified,
        refeed_of_file_id=context["dag_run"].conf.get("refeed_of_file_id"),
    )

    # cp.target_visibility or active-slice table: marks which raw file is the
    # current business input for this dataset/date without deleting history.
    control.set_file_visibility(
        file_id=file_id,
        business_date=context["ds"],
        active_flag="Y",
        reason="new raw file accepted",
    )

    # Airflow XCom: passes the stable file_id to Glue so downstream lineage
    # does not need to rediscover or recalculate file identity.
    context["ti"].xcom_push(key="file_id", value=file_id)
    context["ti"].xcom_push(key="raw_path", value=raw_path)

register_customer_raw = PythonOperator(
    task_id="register_customer_raw",
    python_callable=register_raw_file,
)`,
    },
    {
      id: "silver-merge-to-postgres",
      title: "Silver customer + transaction merge to Postgres",
      stage: "merge and sink",
      summary: "Read two upstream silver outputs, join them, upsert target rows, and stamp every row with the sink lineage link.",
      purpose: "Use this when a target table is built from multiple upstream datasets. It shows how one Postgres row can still point back to the exact customer silver slice and transaction silver slice that produced it.",
      commentary: [
        "The job should discover active upstream lineage links from the control plane, not from guessed S3 folders. That is what makes refeed behavior predictable.",
        "The output link represents the written target slice. Every target row should carry that link id so row-level trace-back can jump from Postgres back into the lineage graph.",
        "If merge and sink are separate tasks, create separate runs and links. If they are one atomic Glue job, one run with clear stages is acceptable.",
      ],
      inputs: [
        "customer silver output_link_id",
        "transaction silver output_link_id",
        "business_date and workflow_run_id",
      ],
      writes: [
        "cp.run_log: merge run and sink run, or one run with two stages if the job is intentionally atomic",
        "Input edges (cp.input_edge): two upstream_output_link_id values",
        "Output link (cp.output_link): one canonical_to_sink output",
        "ods.customer_transaction: _ods_output_link_id, _ods_workflow_run_id, _ods_active_flag",
      ],
      code: `# Glue job skeleton
# Purpose: read two active silver outputs and publish one business target.
# The important part is that inputs are discovered through lineage links,
# not guessed from folder names.

# cp.target_visibility / cp.lineage_link lookup: pick the active upstream
# customer and transaction slices for this business date.
customer_link = control.get_active_link(dataset="customer", edge_type="raw_to_silver", business_date=args["business_date"])
transaction_link = control.get_active_link(dataset="transaction", edge_type="raw_to_silver", business_date=args["business_date"])

# cp.run_log: one restartable logical run for this sink task.
run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key="customer_transaction_sink",
    pipeline_type="sink",
    domain="sales",
    dataset="customer_transaction",
    business_date=args["business_date"],
    attempt=args["attempt"],
)

customer_df = spark.read.parquet(customer_link.target_ref["path"])
transaction_df = spark.read.parquet(transaction_link.target_ref["path"])
merged_df = build_customer_transaction(customer_df, transaction_df)

# cp.lineage_link: represents the Postgres target slice being written.
# cp.lineage_edge: records both upstream silver lineage_link_id values.
link_id = control.create_lineage_link(
    consumer_run_id=run.run_id,
    edge_type="canonical_to_sink",
    target_ref={"kind": "postgres", "schema": "ods", "table": "customer_transaction"},
    record_count=merged_df.count(),
    transform_version="customer_transaction_sink:v1",
    edges=[
        {"upstream_lineage_link_id": customer_link.lineage_link_id, "record_count": customer_df.count()},
        {"upstream_lineage_link_id": transaction_link.lineage_link_id, "record_count": transaction_df.count()},
    ],
)

# Target table metadata columns:
# _ods_lineage_link_id lets a clicked Postgres row jump back to this output.
# _ods_workflow_run_id groups rows by the Airflow/business execution.
# _ods_active_flag lets business views filter to the current active slice.
rows = merged_df.withColumn("_ods_lineage_link_id", lit(link_id)) \\
    .withColumn("_ods_workflow_run_id", lit(args["workflow_run_id"])) \\
    .withColumn("_ods_active_flag", lit("Y"))

postgres.upsert(
    table="ods.customer_transaction",
    dataframe=rows,
    keys=["business_date", "transaction_id"],
)

# cp.run_log: finish after the target write has succeeded.
control.finish_run(run.run_id, record_count_out=merged_df.count())`,
    },
    {
      id: "aggregate-to-postgres",
      title: "Aggregate detail rows to Postgres",
      stage: "aggregation",
      summary: "Read the active detail slice for one business date, aggregate it, and preserve traceability back to the detail output link.",
      purpose: "Use this when a lower-granularity table feeds a higher-granularity table. The aggregate should not just say it came from a table; it should say which active detail slice was used.",
      commentary: [
        "The aggregate reads through the active-slice control table so a refeed for one business date naturally changes the aggregate input for that date.",
        "The aggregate output gets its own output_link_id. That output points to the detail output as an upstream input edge, preserving the chain from daily totals back to raw files.",
        "For restartability, publish the aggregate and flip visibility in one transaction where possible, so business readers do not see half-refreshed totals.",
      ],
      inputs: [
        "active customer_transaction output_link_id",
        "ods.customer_transaction rows for a business date",
        "target visibility table says which slice is active",
      ],
      writes: [
        "cp.run_log and cp.run_stage_log for aggregate task",
        "Output link (cp.output_link): detail_to_aggregate output",
        "Input edge (cp.input_edge): upstream_output_link_id points to the detail sink output",
        "ods.customer_transaction_daily rows stamped with _ods_output_link_id",
      ],
      code: `-- Purpose: aggregate only the current business-visible detail slice.
-- This prevents a refeed from leaving old detail rows mixed into new totals.

-- cp.target_visibility: discover the active detail lineage link for this date.
-- The aggregate should depend on this exact link, not just the table name.
select lineage_link_id
from cp.target_visibility
where target_schema = 'ods'
  and target_table = 'customer_transaction'
  and business_date = :business_date
  and active_flag = 'Y';

-- cp.lineage_link should be created before this insert for the aggregate output.
-- cp.lineage_edge should point from :aggregate_lineage_link_id back to the
-- active detail lineage_link_id selected above.

-- Target table write: stamp every aggregate row with the aggregate output link.
-- A row click can then trace daily totals back to the detail slice and raw files.
insert into ods.customer_transaction_daily (
    business_date,
    customer_id,
    transaction_count,
    total_amount,
    _ods_lineage_link_id,
    _ods_workflow_run_id,
    _ods_active_flag
)
select
    business_date,
    customer_id,
    count(*) as transaction_count,
    sum(amount) as total_amount,
    :aggregate_lineage_link_id,
    :workflow_run_id,
    'Y'
from ods.customer_transaction
where business_date = :business_date
  and _ods_active_flag = 'Y'
group by business_date, customer_id;`,
    },
    {
      id: "refeed-restart-template",
      title: "Refeed and restartable task",
      stage: "restartability",
      summary: "Let Airflow retry safely and let a business refeed supersede an old slice without losing lineage history.",
      purpose: "Use this as the standard wrapper around every real processing task. It separates retry mechanics from business supersession, so failed attempts remain auditable and successful refeeds become the new active slice intentionally.",
      commentary: [
        "Retries should either resume the same logical task run or create attempts under that run. They should not create competing active business outputs.",
        "A refeed is different from a retry: it is a new accepted input or output for an existing business date, and it should supersede the old active slice only after success.",
        "The active flag flip should be the last step. If processing fails halfway through, old business-visible data remains active and the failed run is still visible for diagnosis.",
      ],
      inputs: [
        "logical task_key per dataset/date/stage",
        "attempt number from Airflow",
        "optional refeed_of_file_id or supersedes_lineage_link_id",
      ],
      writes: [
        "cp.run_log keeps one logical task run, with attempts recorded separately or as run_attempt",
        "failed attempts stay visible and are not treated as active outputs",
        "target visibility flips old business slice to N and new slice to Y only after successful publish",
      ],
      code: `# Restart safe pattern
# Purpose: make retries safe and make refeeds explicit.
# A retry continues the same logical task; a refeed creates a new successful
# business slice that supersedes the old active slice only after publishing.

# cp.run_log: start_or_resume_run prevents duplicate competing runs when
# Airflow retries the same task attempt after a worker failure.
run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key=f"{args['pipeline_type']}:{args['dataset']}:{args['business_date']}",
    pipeline_type=args["pipeline_type"],
    domain=args["domain"],
    dataset=args["dataset"],
    business_date=args["business_date"],
    attempt=args["attempt"],
)

try:
    output = process()

    # cp.lineage_link: the new output exists as history even if it supersedes
    # an older output for the same business date.
    link_id = control.create_lineage_link(..., supersedes_lineage_link_id=args.get("supersedes_lineage_link_id"))

    # Publish transaction: flip active visibility only after processing and
    # target writes have succeeded. If anything fails before this, old data
    # remains active for business users.
    with control.transaction():
        control.deactivate_target_slice(
            target_schema="ods",
            target_table=args["target_table"],
            business_date=args["business_date"],
            reason="superseded by refeed",
        )
        control.activate_target_slice(
            lineage_link_id=link_id,
            target_schema="ods",
            target_table=args["target_table"],
            business_date=args["business_date"],
        )
        control.finish_run(run.run_id, record_count_out=output.record_count)
except Exception as exc:
    # cp.run_log: failed attempts stay auditable but do not become active output.
    control.fail_run(run.run_id, error_message=str(exc))
    raise`,
    },
  ];

  const TEMPLATE_WORKFLOWS = {
    "airflow-glue-raw-to-silver": [
      {
        step: "1",
        title: "Airflow starts Glue",
        meta: "workflow_run_id + attempt",
        note: "Airflow passes stable task identity and retry context.",
      },
      {
        step: "2",
        title: "Read raw file",
        meta: "cp.file_catalogue.file_id",
        note: "Glue reads the raw parquet anchored by file_id.",
      },
      {
        step: "3",
        title: "Validate schema",
        meta: "cp.run_stage_log",
        note: "Validation is recorded as its own restartable stage.",
      },
      {
        step: "4",
        title: "Transform",
        meta: "cp.run_stage_log",
        note: "Canonical business logic creates the silver dataframe.",
      },
      {
        step: "5",
        title: "Write silver",
        meta: "s3://ods/silver/...",
        note: "The parquet output is written before publishing lineage.",
      },
      {
        step: "6",
        title: "Publish output link",
        meta: "Output link + input edge (cp.output_link + cp.input_edge)",
        note: "Downstream jobs consume this exact silver output.",
      },
    ],
    "raw-file-registration": [
      {
        step: "1",
        title: "Raw object arrives",
        meta: "s3 raw path",
        note: "The ingestion boundary receives a physical file.",
      },
      {
        step: "2",
        title: "Inspect object",
        meta: "md5 + size + timestamp",
        note: "Object facts are collected before any transformation.",
      },
      {
        step: "3",
        title: "Register file",
        meta: "cp.file_catalogue",
        note: "A stable file_id is created for lineage and replay.",
      },
      {
        step: "4",
        title: "Set active input",
        meta: "cp.target_visibility",
        note: "Business processing knows which raw file is current.",
      },
      {
        step: "5",
        title: "Pass to Glue",
        meta: "Airflow XCom file_id",
        note: "Downstream tasks receive file_id instead of rediscovering it.",
      },
    ],
    "silver-merge-to-postgres": [
      {
        step: "1",
        title: "Find active customer",
        meta: "customer output_link_id",
        note: "The control plane chooses the current customer silver slice.",
      },
      {
        step: "2",
        title: "Find active transactions",
        meta: "transaction output_link_id",
        note: "The transaction silver slice is selected the same way.",
      },
      {
        step: "3",
        title: "Join silver data",
        meta: "Input edges (cp.input_edge)",
        note: "Both upstream outputs become explicit input edges.",
      },
      {
        step: "4",
        title: "Upsert detail rows",
        meta: "ods.customer_transaction",
        note: "Postgres receives the detail business output.",
      },
      {
        step: "5",
        title: "Stamp rows",
        meta: "_ods_output_link_id",
        note: "Each row can point back to the exact sink output.",
      },
      {
        step: "6",
        title: "Publish target slice",
        meta: "Output link (cp.output_link)",
        note: "Downstream aggregate tasks consume this target output.",
      },
    ],
    "aggregate-to-postgres": [
      {
        step: "1",
        title: "Find active detail",
        meta: "cp.target_visibility",
        note: "The aggregate reads only the active detail slice.",
      },
      {
        step: "2",
        title: "Read detail rows",
        meta: "ods.customer_transaction",
        note: "Rows are filtered by date and active business slice.",
      },
      {
        step: "3",
        title: "Aggregate",
        meta: "group by customer/date",
        note: "Lower-granularity rows become daily totals.",
      },
      {
        step: "4",
        title: "Create output link",
        meta: "detail_to_aggregate",
        note: "The aggregate has its own lineage identity.",
      },
      {
        step: "5",
        title: "Write daily rows",
        meta: "ods.customer_transaction_daily",
        note: "Daily rows are stamped with the aggregate output link.",
      },
    ],
    "refeed-restart-template": [
      {
        step: "1",
        title: "Start or resume",
        meta: "task_key + attempt",
        note: "Retries attach to the logical task identity.",
      },
      {
        step: "2",
        title: "Process safely",
        meta: "cp.run_stage_log",
        note: "Progress shows where a stopped run can restart or diagnose.",
      },
      {
        step: "3",
        title: "Create new output",
        meta: "Output link (cp.output_link)",
        note: "A refeed creates history instead of overwriting history.",
      },
      {
        step: "4",
        title: "Deactivate old slice",
        meta: "active_flag = N",
        note: "The older business-visible output is superseded.",
      },
      {
        step: "5",
        title: "Activate new slice",
        meta: "active_flag = Y",
        note: "The new slice becomes visible only after success.",
      },
      {
        step: "6",
        title: "Fail without publish",
        meta: "cp.run_log failed",
        note: "If processing fails, old data remains active.",
      },
    ],
  };

  const SIMPLE_TEMPLATES = [
    {
      id: "simple-transaction-file",
      title: "Simple transaction file",
      stage: "single file",
      summary: "Read one transaction raw parquet file, validate it, transform it, write the transaction target, and record enough metadata for lineage and restart.",
      purpose: "Use this for the common case where one raw file produces one target dataset. It is the smallest useful product pattern: identify the file, start the run, validate and write the output, create lineage, stamp target rows, then mark the slice active.",
      commentary: [
        "This is the baseline template. If a team can implement this correctly, they can use the product for file-level lineage, row trace-back, and safe reprocessing.",
        "The minimum is not just start_run and finish_run. The minimum useful lineage contract also needs the input file_id, output_link_id, target row stamping, and active-slice publish.",
        "Use the existing edge types for the production relationship. Today that means raw_to_curated for file-to-S3 outputs and canonical_to_sink for business target writes.",
        "target_ref must carry path, content_hash, and version. You can add layer/schema/table/kind fields, but those are descriptive extras.",
      ],
      inputs: [
        "transaction raw parquet path",
        "cp.file_catalogue.file_id for that raw file",
        "workflow_run_id, business_date, and Airflow attempt",
      ],
      writes: [
        "cp.file_catalogue: raw transaction file identity",
        "cp.run_log: one logical run for the transaction task",
        "cp.run_stage_log: validate, transform, write_target",
        "Output link + input edge (cp.output_link + cp.input_edge): target output linked to the raw file with layer metadata",
        "ods.transaction rows stamped with _ods_output_link_id and _ods_workflow_run_id",
        "cp.target_visibility: active_flag Y only after the write succeeds",
      ],
      dataModel: {
        drawioPath: "docs/reference/control-plane-cookbook/simple-transaction-file-entity-relationship.drawio",
        summary: "Entity relationship model for one transaction file. It shows run_log producing output_link rows, output_link owning input_edge rows, and input_edge pointing to either file_catalogue or a previous output_link.",
        rows: [
          {
            table: "cp.file_catalogue",
            role: "raw input object",
            fields: [
              ["file_id", "F_TX_20260530"],
              ["dataset", "transaction"],
              ["s3_raw_path", "s3://raw/sales/transaction/2026-05-30.parquet"],
            ],
          },
          {
            table: "cp.run_log",
            role: "silver process",
            fields: [
              ["run_id", "R_TX_SILVER"],
              ["pipeline_type", "canonicalization"],
              ["dataset", "transaction"],
            ],
          },
          {
            table: "cp.output_link",
            role: "silver output",
            fields: [
              ["output_link_id", "O_TX_SILVER"],
              ["consumer_run_id", "R_TX_SILVER"],
              ["target_ref.path", "s3://silver/transaction/2026-05-30.parquet"],
            ],
          },
          {
            table: "cp.input_edge",
            role: "raw file used by silver output",
            fields: [
              ["input_edge_id", "E_TX_RAW"],
              ["output_link_id", "O_TX_SILVER"],
              ["input_reference_type", "raw_file"],
              ["input_reference_id", "F_TX_20260530"],
            ],
          },
          {
            table: "cp.output_link",
            role: "Postgres target output",
            fields: [
              ["output_link_id", "O_TX_TARGET"],
              ["consumer_run_id", "R_TX_SINK"],
              ["target_ref.path", "postgres://ods/transaction"],
            ],
          },
          {
            table: "cp.input_edge",
            role: "silver output used by target output",
            fields: [
              ["input_edge_id", "E_TX_SILVER"],
              ["output_link_id", "O_TX_TARGET"],
              ["input_reference_type", "output_link"],
              ["input_reference_id", "O_TX_SILVER"],
            ],
          },
          {
            table: "ods.transaction",
            role: "business row",
            fields: [
              ["transaction_id", "T100"],
              ["business_date", "2026-05-30"],
              ["_ods_output_link_id", "O_TX_TARGET"],
            ],
          },
        ],
        lookupSql: `select
    run_id,
    output_link_id,
    output_path,
    input_edge_id,
    input_reference_type,
    input_reference_id,
    input_path
from cp.v_run_io
where run_id = 'R_TX_SILVER'
order by output_created_at, input_slot nulls last;`,
      },
      code: `# Simple transaction file template
# Purpose: one raw transaction parquet file becomes a silver S3 output, and
# optionally a business-visible Postgres target.

# Product terms:
# - pipeline_type describes the role of the run: ingestion, canonicalization,
#   merge, aggregation, sink. It changes how humans and discovery code classify
#   the run; it does not choose the storage destination by itself.
# - sink_type chooses the sink destination for canonical_to_sink links:
#   postgres, s3, kafka, etc.
# - target_ref must include path, content_hash, and version. Extra fields such
#   as layer/schema/table/format are useful but not the core contract.

file_id = control.register_file(
    domain="sales",
    dataset="transaction",
    business_date=args["business_date"],
    s3_raw_path=args["raw_path"],
    file_md5=args["file_md5"],
)

# This task creates the reusable silver output.
silver_run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key="transaction_raw_to_silver",
    pipeline_type="canonicalization",
    domain="sales",
    dataset="transaction",
    business_date=args["business_date"],
    attempt=args["attempt"],  # usually Airflow ti.try_number
)

control.start_stage(silver_run.run_id, "validate")
raw_df = spark.read.parquet(args["raw_path"])
validate_schema(raw_df, expected_transaction_schema)
control.finish_stage(silver_run.run_id, "validate", record_count_in=raw_df.count(), record_count_out=raw_df.count())

control.start_stage(silver_run.run_id, "transform")
silver_df = canonicalize_transaction(raw_df)
control.finish_stage(silver_run.run_id, "transform", record_count_in=raw_df.count(), record_count_out=silver_df.count())

control.start_stage(silver_run.run_id, "write_silver")
silver_df.write.mode("overwrite").parquet(args["silver_path"])

# For a file target, compute content_hash after the file exists.
# Then create the output link that names the completed output.
silver_content_hash = compute_s3_content_hash(args["silver_path"])
silver_output_link_id = control.write_output_link(
    consumer_run_id=silver_run.run_id,
    edge_type="raw_to_curated",
    target_ref={
        "path": args["silver_path"],
        "content_hash": silver_content_hash,
        "version": 1,
        "layer": "silver",
        "format": "parquet",
    },
    record_count=silver_df.count(),
    transform_version="transaction_raw_to_silver:v1",
    inputs=[{
        "source_file_id": file_id,
        "edge_type": "raw_to_curated",
        "record_count": raw_df.count(),
    }],
)
control.finish_stage(silver_run.run_id, "write_silver", record_count_in=silver_df.count(), record_count_out=silver_df.count())
control.finish_run(silver_run.run_id, record_count_out=silver_df.count())

# If you do NOT sync this dataset to a business target, stop here.
# You have file + run + silver output lineage, but no active target slice.
if args["sync_to_postgres"]:
    sink_run = control.start_or_resume_run(
        workflow_run_id=args["workflow_run_id"],
        task_key="transaction_silver_to_postgres",
        pipeline_type="sink",
        domain="sales",
        dataset="transaction",
        business_date=args["business_date"],
        attempt=args["attempt"],
    )

    # For a database target, compute a deterministic hash of the business rows
    # before stamping output-link metadata. This hash identifies the output
    # content; the output_link_id itself is metadata about the write event.
    postgres_content_hash = compute_rowset_hash(
        silver_df,
        keys=["business_date", "transaction_id"],
    )
    sink_output_link_id = control.write_output_link(
        consumer_run_id=sink_run.run_id,
        edge_type="canonical_to_sink",
        sink_type="postgres",
        target_ref={
            "path": "postgres://ods/transaction",
            "content_hash": postgres_content_hash,
            "version": 1,
            "kind": "postgres",
            "schema": "ods",
            "table": "transaction",
            "layer": "target",
        },
        record_count=silver_df.count(),
        transform_version="transaction_postgres_upsert:v1",
        inputs=[{
            "upstream_output_link_id": silver_output_link_id,
            "edge_type": "canonical_to_sink",
            "record_count": silver_df.count(),
        }],
    )

    # postgres.upsert is a placeholder adapter. In Glue this might be a
    # dataframe JDBC write, COPY into staging + SQL MERGE, or a Python DB call.
    # Whichever implementation you choose must write sink_output_link_id onto the rows.
    rows = silver_df.withColumn("_ods_output_link_id", lit(sink_output_link_id)) \\
        .withColumn("_ods_workflow_run_id", lit(args["workflow_run_id"]))
    postgres.upsert(table="ods.transaction", dataframe=rows, keys=["business_date", "transaction_id"])

    control.reconcile_sink_link(lineage_link_id=sink_output_link_id, source_count=silver_df.count())
    control.finish_run(sink_run.run_id, record_count_out=silver_df.count())

    # activate_target_slice writes the active-control row: for ods.transaction
    # and this business_date, sink_output_link_id is now the business-visible version.
    control.activate_target_slice(
        output_link_id=sink_output_link_id,
        target_schema="ods",
        target_table="transaction",
        business_date=args["business_date"],
    )`,
    },
    {
      id: "customer-transaction-merge",
      title: "Customer + transaction merge",
      stage: "merge",
      summary: "Use the current pattern: read customer and transaction silver outputs, merge them, sink customer_transaction, and stamp each target row with the output link.",
      purpose: "Use this when the target is created from two upstream datasets. The product value here is proving which customer silver slice and which transaction silver slice produced the merged target rows.",
      commentary: [
        "This template assumes customer and transaction already have silver output links from earlier processing.",
        "The merge output first creates a merge_to_canonical output link. The Postgres write then creates a canonical_to_sink output link that points to the merge output.",
        "The active slice should flip only after the target upsert, reconciliation, and succeeded run status. That protects business users during retries and refeeds.",
        "The control API records metadata. It does not automatically read data or merge rows; read_target_slice and build_customer_transaction are application code.",
      ],
      inputs: [
        "customer silver output_link_id",
        "transaction silver output_link_id",
        "workflow_run_id, business_date, and Airflow attempt",
      ],
      writes: [
        "cp.run_log: one logical merge/sink run",
        "cp.run_stage_log: read_inputs, merge, write_target",
        "cp.input_edge: one merge edge for customer and one merge edge for transaction",
        "cp.output_link: one merge_to_canonical link and one canonical_to_sink link",
        "ods.customer_transaction rows stamped with _ods_output_link_id",
        "cp.target_visibility: active customer_transaction slice after successful write",
      ],
      dataModel: {
        drawioPath: "docs/reference/control-plane-cookbook/customer-transaction-merge-entity-relationship.drawio",
        summary: "Entity relationship model for the merge. Two upstream output_link rows are referenced by two input_edge rows that belong to the merged output_link; the sink output then consumes that merged output.",
        rows: [
          {
            table: "cp.output_link",
            role: "customer input output",
            fields: [
              ["output_link_id", "O_CUSTOMER_SILVER"],
              ["dataset", "customer"],
              ["target_ref.path", "s3://silver/customer/2026-05-30.parquet"],
            ],
          },
          {
            table: "cp.output_link",
            role: "transaction input output",
            fields: [
              ["output_link_id", "O_TRANSACTION_SILVER"],
              ["dataset", "transaction"],
              ["target_ref.path", "s3://silver/transaction/2026-05-30.parquet"],
            ],
          },
          {
            table: "cp.run_log",
            role: "merge process",
            fields: [
              ["run_id", "R_MERGE"],
              ["pipeline_type", "merge"],
              ["dataset", "customer_transaction"],
            ],
          },
          {
            table: "cp.output_link",
            role: "merged silver output",
            fields: [
              ["output_link_id", "O_MERGED_SILVER"],
              ["consumer_run_id", "R_MERGE"],
              ["target_ref.path", "s3://silver/customer_transaction/2026-05-30.parquet"],
            ],
          },
          {
            table: "cp.input_edge",
            role: "customer input relationship",
            fields: [
              ["input_edge_id", "E_MERGE_CUSTOMER"],
              ["output_link_id", "O_MERGED_SILVER"],
              ["input_role", "customer"],
              ["input_reference_id", "O_CUSTOMER_SILVER"],
            ],
          },
          {
            table: "cp.input_edge",
            role: "transaction input relationship",
            fields: [
              ["input_edge_id", "E_MERGE_TRANSACTION"],
              ["output_link_id", "O_MERGED_SILVER"],
              ["input_role", "transaction"],
              ["input_reference_id", "O_TRANSACTION_SILVER"],
            ],
          },
          {
            table: "cp.output_link",
            role: "Postgres target output",
            fields: [
              ["output_link_id", "O_CT_TARGET"],
              ["consumer_run_id", "R_CT_SINK"],
              ["target_ref.path", "postgres://ods/customer_transaction"],
            ],
          },
          {
            table: "cp.input_edge",
            role: "merged output used by sink",
            fields: [
              ["input_edge_id", "E_SINK_MERGED"],
              ["output_link_id", "O_CT_TARGET"],
              ["input_reference_type", "output_link"],
              ["input_reference_id", "O_MERGED_SILVER"],
            ],
          },
          {
            table: "ods.customer_transaction",
            role: "business row",
            fields: [
              ["transaction_id", "T100"],
              ["customer_id", "C001"],
              ["_ods_output_link_id", "O_CT_TARGET"],
            ],
          },
        ],
        lookupSql: `select
    run_id,
    output_link_id,
    output_path,
    input_role,
    input_reference_type,
    input_reference_id,
    input_path
from cp.v_run_io
where run_id = 'R_MERGE'
order by input_slot nulls last;`,
      },
      code: `# Customer + transaction merge template
# Purpose: two silver upstream datasets become one merged target table.

# 1. Discover exact SILVER inputs from the control plane.
# This identifies curated_to_canonical links, not guessed S3 paths and not
# the final Postgres sink.
customer_run_id = control.latest_succeeded_run(
    domain="sales",
    dataset="customer",
    business_date=args["business_date"],
    pipeline_type="canonicalization",
)
customer_link = control.run_output_link(
    run_id=customer_run_id,
    edge_type="curated_to_canonical",
)

transaction_run_id = control.latest_succeeded_run(
    domain="sales",
    dataset="transaction",
    business_date=args["business_date"],
    pipeline_type="canonicalization",
)
transaction_link = control.run_output_link(
    run_id=transaction_run_id,
    edge_type="curated_to_canonical",
)

# 2. Start or resume the logical merge run.
# This is not merge-specific. Every processing task should start/resume a run.
# attempt is usually Airflow ti.try_number, used to make retries auditable.
merge_run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key="customer_transaction_merge",
    pipeline_type="merge",
    domain="sales",
    dataset="customer_transaction",
    business_date=args["business_date"],
    attempt=args["attempt"],
)

# 3. Stage logging is not automatic and does not do the merge.
# start_stage/finish_stage only writes cp.run_stage_log rows.
# spark.read and build_customer_transaction are your application code.
control.start_stage(merge_run.run_id, "read_inputs")
customer_df = spark.read.parquet(customer_link.target_ref["path"])
transaction_df = spark.read.parquet(transaction_link.target_ref["path"])
control.finish_stage(merge_run.run_id, "read_inputs", record_count_in=0, record_count_out=customer_df.count() + transaction_df.count())

control.start_stage(merge_run.run_id, "merge")
merged_df = build_customer_transaction(customer_df, transaction_df)
control.finish_stage(merge_run.run_id, "merge", record_count_in=customer_df.count() + transaction_df.count(), record_count_out=merged_df.count())

# 4. The merge creates a canonical output. It is not the Postgres sink yet.
merged_df.write.mode("overwrite").parquet(args["merged_silver_path"])
merged_content_hash = compute_s3_content_hash(args["merged_silver_path"])
merge_output_link_id = control.write_output_link(
    consumer_run_id=merge_run.run_id,
    edge_type="merge_to_canonical",
    target_ref={
        "path": args["merged_silver_path"],
        "content_hash": merged_content_hash,
        "version": 1,
        "layer": "silver",
        "format": "parquet",
    },
    record_count=merged_df.count(),
    transform_version="customer_transaction_merge:v1",
    inputs=[
        {"upstream_output_link_id": customer_link.output_link_id, "edge_type": "merge_to_canonical", "record_count": customer_df.count()},
        {"upstream_output_link_id": transaction_link.output_link_id, "edge_type": "merge_to_canonical", "record_count": transaction_df.count()},
    ],
)
control.finish_run(merge_run.run_id, record_count_out=merged_df.count())

# 5. The sink run writes the merged output to Postgres.
# pipeline_type='sink' means this run publishes to a sink. The destination is
# sink_type='postgres' plus target_ref.path='postgres://ods/customer_transaction'.
sink_run = control.start_or_resume_run(
    workflow_run_id=args["workflow_run_id"],
    task_key="customer_transaction_postgres_sink",
    pipeline_type="sink",
    domain="sales",
    dataset="customer_transaction",
    business_date=args["business_date"],
    attempt=args["attempt"],
)

postgres_content_hash = compute_rowset_hash(
    merged_df,
    keys=["business_date", "transaction_id"],
)
sink_output_link_id = control.write_output_link(
    consumer_run_id=sink_run.run_id,
    edge_type="canonical_to_sink",
    sink_type="postgres",
    target_ref={
        "path": "postgres://ods/customer_transaction",
        "content_hash": postgres_content_hash,
        "version": 1,
        "kind": "postgres",
        "schema": "ods",
        "table": "customer_transaction",
        "layer": "target",
    },
    record_count=merged_df.count(),
    transform_version="customer_transaction_postgres_upsert:v1",
    inputs=[{
        "upstream_output_link_id": merge_output_link_id,
        "edge_type": "canonical_to_sink",
        "record_count": merged_df.count(),
    }],
)

# postgres.upsert is an adapter placeholder. In real Glue it might be dataframe
# JDBC, COPY-to-staging plus SQL MERGE, or a stored procedure call. The product
# requires every implementation to carry sink_output_link_id into the target rows.
rows = merged_df.withColumn("_ods_output_link_id", lit(sink_output_link_id)) \\
    .withColumn("_ods_workflow_run_id", lit(args["workflow_run_id"]))
postgres.upsert(table="ods.customer_transaction", dataframe=rows, keys=["business_date", "transaction_id"])

control.reconcile_sink_link(lineage_link_id=sink_output_link_id, source_count=merged_df.count())
control.finish_run(sink_run.run_id, record_count_out=merged_df.count())

# 6. activate_target_slice updates the business-active control table.
# It means: for ods.customer_transaction and this business_date, sink_output_link_id is
# now the Y/current version. The previous active slice, if any, becomes N.
control.activate_target_slice(
    output_link_id=sink_output_link_id,
    target_schema="ods",
    target_table="customer_transaction",
    business_date=args["business_date"],
)`,
    },
  ];

  const SIMPLE_TEMPLATE_WORKFLOWS = {
    "simple-transaction-file": [
      {
        step: "1",
        title: "Raw transaction file",
        meta: "s3 parquet",
        note: "One physical input file arrives for a business date.",
      },
      {
        step: "2",
        title: "Register file",
        meta: "cp.file_catalogue.file_id",
        note: "The file gets a stable identity for lineage.",
      },
      {
        step: "3",
        title: "Run and stages",
        meta: "cp.run_log + cp.run_stage_log",
        note: "Airflow/Glue progress is restartable and auditable.",
      },
      {
        step: "4",
        title: "Create output link",
        meta: "raw_to_curated + source_file_id",
        note: "The silver output points back to the raw file.",
      },
      {
        step: "5",
        title: "Optional sink",
        meta: "canonical_to_sink",
        note: "If synced, target rows are stamped with _ods_output_link_id.",
      },
      {
        step: "6",
        title: "Activate slice",
        meta: "active_flag = Y",
        note: "Business use starts only after success.",
      },
    ],
    "customer-transaction-merge": [
      {
        step: "1",
        title: "Customer silver input",
        meta: "curated_to_canonical",
        note: "The control plane chooses the exact customer silver output.",
      },
      {
        step: "2",
        title: "Transaction silver input",
        meta: "curated_to_canonical",
        note: "The control plane chooses the exact transaction silver output.",
      },
      {
        step: "3",
        title: "Merge",
        meta: "cp.run_stage_log",
        note: "The job joins both active inputs.",
      },
      {
        step: "4",
        title: "Create merged link",
        meta: "merge_to_canonical + two edges",
        note: "The merged silver output records both upstream inputs.",
      },
      {
        step: "5",
        title: "Upsert target rows",
        meta: "ods.customer_transaction",
        note: "Rows are stamped with the merged output link.",
      },
      {
        step: "6",
        title: "Activate merged slice",
        meta: "active_flag = Y",
        note: "The new merged result becomes business-visible.",
      },
    ],
  };

  const DOCS_FAQ = [
    {
      topic: "Minimum useful contract",
      question: "What is the minimum I need to use the product properly?",
      answer: [
        "For run tracking only, start_or_resume_run and finish_run are enough.",
        "For lineage, also create output links and input edges for every produced output.",
        "For row trace-back, stamp target rows with _ods_output_link_id and _ods_workflow_run_id.",
        "For business current-state, activate the target slice only after the write, reconciliation, and succeeded run status.",
      ],
      code: `run = control.start_or_resume_run(...)
output_link_id = control.write_output_link(..., inputs=[...])
rows = rows.withColumn("_ods_output_link_id", lit(output_link_id))
postgres.upsert(...)
control.reconcile_sink_link(lineage_link_id=output_link_id, source_count=row_count)
control.finish_run(run.run_id, record_count_out=row_count)
control.activate_target_slice(...)`,
    },
    {
      topic: "Pipeline type",
      question: "What does pipeline_type='sink' do? Can a sink be S3 or Postgres?",
      answer: [
        "pipeline_type classifies the run in cp.run_log. It helps humans, discovery logic, dashboards, and restart decisions understand the role of the task.",
        "pipeline_type='sink' means the run publishes to a sink or serving target. It does not choose the storage destination by itself.",
        "The destination is described by the output link: edge_type='canonical_to_sink', sink_type='postgres' or 's3' or another sink, and target_ref.path.",
      ],
      code: `sink_run = control.start_or_resume_run(
    pipeline_type="sink",
    dataset="transaction",
    ...
)

sink_output_link_id = control.write_output_link(
    edge_type="canonical_to_sink",
    sink_type="postgres",
    target_ref={"path": "postgres://ods/transaction", "content_hash": "...", "version": 1},
    ...
)`,
    },
    {
      topic: "Target reference",
      question: "What is a valid target_ref?",
      answer: [
        "Today target_ref must include path, content_hash, and version. The client and database both expect that contract.",
        "path is the stable output location or target identity. content_hash distinguishes changed content, including refeeds at the same path. version is the output identity version.",
        "For a file output, content_hash usually comes after writing the file. For a database output, compute a deterministic hash of the business rowset before stamping output-link metadata, or compute it from a staging table before the final merge.",
        "Extra fields such as kind, layer, schema, table, and format are useful descriptive metadata, but they do not replace the required three fields.",
      ],
      code: `# S3 silver output: write first, hash second, then create the link.
silver_df.write.mode("overwrite").parquet(silver_path)
silver_hash = compute_s3_content_hash(silver_path)

target_ref = {
    "path": "s3://ods/silver/transaction/business_date=2026-05-30/",
    "content_hash": silver_hash,
    "version": 1,
    "layer": "silver",
    "format": "parquet",
}

# Postgres target output: hash the deterministic business rowset.
postgres_hash = compute_rowset_hash(rows, keys=["business_date", "transaction_id"])

target_ref = {
    "path": "postgres://ods/transaction",
    "content_hash": postgres_hash,
    "version": 1,
    "kind": "postgres",
    "schema": "ods",
    "table": "transaction",
    "layer": "target",
}`,
    },
    {
      topic: "Application code",
      question: "Does control.start_stage(...) read or merge data automatically?",
      answer: [
        "No. start_stage and finish_stage only write cp.run_stage_log metadata.",
        "Your application code still reads files, validates schemas, merges dataframes, writes Postgres, or calls stored procedures.",
        "The stage log is there so restart, audit, and dashboards can show where the work stopped or succeeded.",
      ],
      code: `control.start_stage(run.run_id, "merge")

# This is your application code, not automatic control-plane work.
merged_df = build_customer_transaction(customer_df, transaction_df)

control.finish_stage(
    run.run_id,
    "merge",
    record_count_in=customer_df.count() + transaction_df.count(),
    record_count_out=merged_df.count(),
)`,
    },
    {
      topic: "Postgres writes",
      question: "Does postgres.upsert(...) have to be a dataframe write?",
      answer: [
        "No. postgres.upsert is only a placeholder adapter in the template.",
        "In Glue it could be a Spark JDBC dataframe write, COPY into staging plus SQL MERGE, a stored procedure, or a Python DB call.",
        "The product requirement is that every write path carries sink_output_link_id into the target rows. If an example does not pass or stamp the output id, it is incomplete.",
      ],
      code: `# Any of these can be valid implementation choices:
rows = rows.withColumn("_ods_output_link_id", lit(sink_output_link_id))
postgres.upsert(table="ods.transaction", dataframe=rows, keys=[...])

copy_to_staging_then_merge(
    rows,
    target_table="ods.transaction",
    output_link_id=sink_output_link_id,
)

call_stored_procedure(
    "ods.load_transaction",
    output_link_id=sink_output_link_id,
)

# Product invariant:
# written target rows must carry _ods_output_link_id = sink_output_link_id`,
    },
    {
      topic: "Activation",
      question: "What does activate_target_slice(...) do?",
      answer: [
        "It writes the business-active control row. In plain English: for this target and business date, this output_link_id is now the current version.",
        "On a refeed, the old slice becomes inactive and the new slice becomes active. Lineage history remains immutable.",
        "It should run last, after the target write, reconciliation, and run success. If processing fails before activation, business users keep seeing the previous active slice.",
      ],
      code: `control.activate_target_slice(
    output_link_id=sink_output_link_id,
    target_schema="ods",
    target_table="transaction",
    business_date=args["business_date"],
)`,
    },
    {
      topic: "Naming",
      question: "Can I rename silver_run to integration_run?",
      answer: [
        "Yes. Local variable names in your DAG, Glue job, or Python module do not affect the control tables.",
        "What matters is passing a valid run_id into stage/link calls, and using stable metadata values such as task_key, pipeline_type, domain, dataset, and business_date.",
      ],
      code: `integration_run = control.start_or_resume_run(
    task_key="transaction_raw_to_silver",
    pipeline_type="canonicalization",
    ...
)

control.start_stage(integration_run.run_id, "transform")`,
    },
    {
      topic: "Sync optionality",
      question: "Do I have to create a sink and active slice if I do not sync to Postgres?",
      answer: [
        "No. If the job only creates an internal S3 silver output, create the file/run/stage/output-link metadata for that output and stop there.",
        "Only create canonical_to_sink and activate_target_slice when you publish something business-visible or serving-facing.",
      ],
      code: `silver_output_link_id = control.write_output_link(
    edge_type="raw_to_curated",
    target_ref={"path": silver_path, "content_hash": silver_hash, "version": 1},
    ...
)

if sync_to_postgres:
    sink_output_link_id = control.write_output_link(edge_type="canonical_to_sink", ...)
    control.activate_target_slice(...)`,
    },
  ];

  function App() {
    const [data, setData] = React.useState(null);
    const [error, setError] = React.useState(null);
    const [tab, setTab] = React.useState("workflows");
    const [selectedExecutionId, setSelectedExecutionId] = React.useState("all");
    const [selectedRunId, setSelectedRunId] = React.useState("");
    const [focusedLinkId, setFocusedLinkId] = React.useState("");
    const [selectedTable, setSelectedTable] = React.useState("");
    const [selectedRowsWorkflowId, setSelectedRowsWorkflowId] = React.useState("all");
    const [selectedDate, setSelectedDate] = React.useState("all");
    const [targetRowSort, setTargetRowSort] = React.useState("row_id:asc");
    const [flowScope, setFlowScope] = React.useState("overview");
    const [jsonWorkflowId, setJsonWorkflowId] = React.useState("");
    const [olWorkflowId, setOlWorkflowId] = React.useState("");
    const [dataFile, setDataFile] = React.useState(initialDataFile());

    React.useEffect(() => {
      setData(null);
      setError(null);
      fetch(`./data/${dataFile}`, { cache: "no-store" })
        .then((res) => {
          if (!res.ok) throw new Error("snapshot not found");
          return res.json();
        })
        .then((snapshot) => {
          const firstExecution = snapshot.executions?.[0]?.workflow_run_id || "all";
          setData(normalizeSnapshot(snapshot));
          setSelectedExecutionId(firstExecution);
          setSelectedTable(Object.keys(snapshot.tables || {})[0] || "");
          setSelectedDate("all");
        })
        .catch((err) => setError(err.message));
    }, [dataFile]);

    React.useEffect(() => {
      if (!data) return;
      const runs = filteredRuns(data, selectedExecutionId);
      if (!runs.find((run) => run.run_id === selectedRunId)) {
        setSelectedRunId(runs[0]?.run_id || "");
      }
    }, [data, selectedExecutionId]);

    if (error) {
      return e("main", { className: "content" },
        e("div", { className: "error" },
          e("strong", null, "No dashboard snapshot found."),
          e("div", { className: "mono", style: { marginTop: 8 } },
            "python -m harness.customer_transaction_workflow --out dashboard/data/demo-workflow.json"),
          e("div", { className: "mono", style: { marginTop: 6 } },
            "python -m http.server 8099 --directory dashboard")
        )
      );
    }
    if (!data) {
      return e("main", { className: "content" },
        e("div", { className: "empty" }, "Loading dashboard data")
      );
    }

    const selectedExecution = data.executionByWorkflow[selectedExecutionId] || null;
    const selectedRun = data.runById[selectedRunId] || filteredRuns(data, selectedExecutionId)[0] || null;

    const focusLink = (linkId) => {
      const link = data.linkById[linkId];
      if (!link) return;
      setFocusedLinkId(linkId);
      setSelectedExecutionId(link.workflow_run_id || data.runById[link.consumer_run_id]?.workflow_run_id || "all");
      setSelectedRunId(link.consumer_run_id);
      setTab("control");
    };

    return e("div", { className: "app" },
      e(Header, { data, tab, setTab, dataFile, setDataFile }),
      e("main", { className: "content" },
        tab === "workflows" && e(WorkflowsTab, {
          data,
          selectedExecutionId,
          jsonWorkflowId,
          setJsonWorkflowId,
          olWorkflowId,
          setOlWorkflowId,
          openDetails: (workflowRunId) => {
            setSelectedExecutionId(workflowRunId);
            setTab("diagram");
          },
          openProcess: (workflowRunId) => {
            setSelectedExecutionId(workflowRunId);
            setTab("process");
          },
          openDeveloper: (workflowRunId) => {
            setSelectedExecutionId(workflowRunId);
            setTab("developer");
          },
        }),
        tab === "metadata" && e(MetadataTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
          focusLink,
          focusRun: (runId) => {
            setSelectedRunId(runId);
            setTab("control");
          },
        }),
        tab === "control" && e(ControlTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
          selectedExecution,
          selectedRun,
          selectedRunId,
          setSelectedRunId,
          focusedLinkId,
          setFocusedLinkId,
        }),
        tab === "flow" && e(FlowTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
          flowScope,
          setFlowScope,
          focusRun: (runId) => {
            setSelectedRunId(runId);
            setTab("control");
          },
        }),
        tab === "diagram" && e(WorkflowDiagramTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
          focusLink,
        }),
        tab === "process" && e(ProcessModelTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
          focusLink,
        }),
        tab === "developer" && e(DeveloperProcessModelTab, {
          data,
          selectedExecutionId,
          setSelectedExecutionId,
        }),
        tab === "rows" && e(RowsTab, {
          data,
          selectedTable,
          setSelectedTable,
          selectedRowsWorkflowId,
          setSelectedRowsWorkflowId,
          selectedDate,
          setSelectedDate,
          targetRowSort,
          setTargetRowSort,
          openWorkflow: (workflowRunId) => {
            setSelectedExecutionId(workflowRunId);
            setTab("process");
          },
          focusLink,
        }),
        tab === "templates" && e(TemplatesTab),
        tab === "docs" && e(DocumentationTab)
      )
    );
  }

  function normalizeSnapshot(snapshot) {
    const executionByWorkflow = Object.fromEntries(
      (snapshot.executions || []).map((execution) => [execution.workflow_run_id, execution])
    );
    const runById = Object.fromEntries((snapshot.runs || []).map((run) => [run.run_id, run]));
    const linkById = Object.fromEntries((snapshot.links || []).map((link) => [outputLinkId(link), link]));
    const fileById = Object.fromEntries((snapshot.files || []).map((file) => [file.file_id, file]));
    const linksByRun = {};
    const targetRowsByLink = {};
    const consumersByLink = {};
    (snapshot.links || []).forEach((link) => {
      if (!linksByRun[link.consumer_run_id]) linksByRun[link.consumer_run_id] = [];
      linksByRun[link.consumer_run_id].push(link);
      (link.edges || []).forEach((edge) => {
        const upstreamId = upstreamOutputLinkId(edge);
        if (!upstreamId) return;
        if (!consumersByLink[upstreamId]) consumersByLink[upstreamId] = [];
        consumersByLink[upstreamId].push({
          edge_type: link.edge_type,
          consumer_run_id: link.consumer_run_id,
          consumer_link_id: outputLinkId(link),
          record_count: edge.record_count,
        });
      });
    });
    Object.entries(snapshot.tables || {}).forEach(([tableName, rows]) => {
      rows.forEach((row) => {
        const linkId = rowOutputLinkId(row);
        if (!linkId) return;
        if (!targetRowsByLink[linkId]) targetRowsByLink[linkId] = [];
        targetRowsByLink[linkId].push({ tableName, ...row });
      });
    });
    return {
      ...snapshot,
      executionByWorkflow,
      runById,
      linkById,
      fileById,
      linksByRun,
      targetRowsByLink,
      consumersByLink,
    };
  }

  function outputLinkId(link) {
    return link?.output_link_id || link?.lineage_link_id || "";
  }

  function inputEdgeId(edge) {
    return edge?.input_edge_id || edge?.lineage_edge_id || "";
  }

  function upstreamOutputLinkId(edge) {
    return edge?.upstream_output_link_id || edge?.upstream_lineage_link_id || "";
  }

  function rowOutputLinkId(row) {
    return row?._ods_output_link_id || row?._ods_lineage_link_id || "";
  }

  function Header({ data, tab, setTab, dataFile, setDataFile }) {
    const counts = {
      executions: data.executions?.length || 0,
      runs: data.runs?.length || 0,
      links: data.links?.length || 0,
    };
    return e("header", { className: "topbar" },
      e("div", { className: "brand" },
        e("h1", null, "ODS Lineage Dashboard"),
        e("span", null,
          `${counts.executions} executions · ${counts.runs} runs · ${counts.links} output links`
        )
      ),
      setDataFile && e("div", { className: "dataset-picker" },
        e("label", { htmlFor: "dataset-select" }, "snapshot"),
        e("select", {
          id: "dataset-select",
          value: dataFile,
          onChange: (event) => {
            const next = event.target.value;
            setDataFile(next);
            const url = new URL(window.location.href);
            url.searchParams.set("data", next);
            window.history.replaceState(null, "", url.toString());
          },
        },
          KNOWN_DATA_FILES.map(([file, label]) =>
            e("option", { key: file, value: file }, label)
          )
        )
      ),
      e("nav", { className: "tabs" },
        TABS.map(([id, label]) =>
          e("button", {
            key: id,
            className: `tab ${tab === id ? "active" : ""}`,
            onClick: () => setTab(id),
          }, label)
        )
      )
    );
  }

  function WorkflowsTab({ data, selectedExecutionId, jsonWorkflowId, setJsonWorkflowId, olWorkflowId, setOlWorkflowId, openDetails, openProcess, openDeveloper }) {
    const summaries = workflowSummaries(data);
    const selectedJson = jsonWorkflowId
      ? buildWorkflowJson(data, jsonWorkflowId)
      : null;
    const selectedOl = olWorkflowId
      ? buildOpenLineageEvents(data, olWorkflowId)
      : null;

    return e("div", { className: "workflows-page" },
      e("section", { className: "workflow-overview-grid" },
        e("div", { className: "panel workflow-overview-card" },
          e("span", null, "workflows"),
          e("strong", null, summaries.length),
          e("small", null, "Grouped by workflow_run_id")
        ),
        e("div", { className: "panel workflow-overview-card" },
          e("span", null, "runs"),
          e("strong", null, data.runs.length),
          e("small", null, "Scheduler/task executions")
        ),
        e("div", { className: "panel workflow-overview-card" },
          e("span", null, "outputs"),
          e("strong", null, data.links.length),
          e("small", null, "cp.output_link rows")
        ),
        e("div", { className: "panel workflow-overview-card" },
          e("span", null, "target rows"),
          e("strong", null, Object.values(data.tables || {}).reduce((sum, rows) => sum + rows.length, 0)),
          e("small", null, "Rows stamped by workflow")
        )
      ),
      e("section", { className: "panel" },
        e("div", { className: "panel-head" },
          e("h2", null, "Workflow Runs"),
          e("span", { className: "pill" }, `${summaries.length}`)
        ),
        e("div", { className: "workflow-list" },
          summaries.map((summary) => e(WorkflowSummaryCard, {
            key: summary.workflow_run_id,
            summary,
            selected: selectedExecutionId === summary.workflow_run_id,
            jsonOpen: jsonWorkflowId === summary.workflow_run_id,
            olOpen: olWorkflowId === summary.workflow_run_id,
            onDetails: () => openDetails(summary.workflow_run_id),
            onProcess: () => openProcess(summary.workflow_run_id),
            onDeveloper: () => openDeveloper(summary.workflow_run_id),
            onJson: () => {
              setOlWorkflowId("");
              setJsonWorkflowId(jsonWorkflowId === summary.workflow_run_id ? "" : summary.workflow_run_id);
            },
            onOpenLineage: () => {
              setJsonWorkflowId("");
              setOlWorkflowId(olWorkflowId === summary.workflow_run_id ? "" : summary.workflow_run_id);
            },
          }))
        )
      ),
      selectedJson && e("section", { className: "panel workflow-json-panel" },
        e("div", { className: "panel-head" },
          e("div", null,
            e("h2", null, "Workflow JSON"),
            e("p", null, "Generated from the dashboard snapshot. Inputs use a generic input.type + input.id shape.")
          ),
          e("button", { type: "button", className: "small-action", onClick: () => setJsonWorkflowId("") }, "close")
        ),
        e("pre", { className: "template-code workflow-json-code" },
          e("code", null, JSON.stringify(selectedJson, null, 2))
        )
      ),
      selectedOl && e("section", { className: "panel workflow-json-panel" },
        e("div", { className: "panel-head" },
          e("div", null,
            e("h2", null, "OpenLineage Events"),
            e("p", null, "Generated OpenLineage-style COMPLETE events from the selected workflow. ODS identifiers are carried in custom facets.")
          ),
          e("button", { type: "button", className: "small-action", onClick: () => setOlWorkflowId("") }, "close")
        ),
        e("pre", { className: "template-code workflow-json-code" },
          e("code", null, JSON.stringify(selectedOl, null, 2))
        )
      )
    );
  }

  function WorkflowSummaryCard({ summary, selected, jsonOpen, olOpen, onDetails, onProcess, onDeveloper, onJson, onOpenLineage }) {
    const statusClass = summary.execution_type === "refeed" ? "amber" : "green";
    return e("article", { className: `workflow-summary-card ${selected ? "selected" : ""}` },
      e("div", { className: "workflow-summary-main" },
        e("div", { className: "workflow-summary-title" },
          e("span", { className: `pill ${statusClass}` }, summary.execution_type),
          e("strong", null, summary.business_date),
          e("code", null, summary.workflow_run_id)
        ),
        e("p", null, summary.description || "Workflow grouped from run_log rows"),
        summary.refeed_of_workflow_run_id && e("div", { className: "workflow-refeed" },
          e("span", null, "refeed of"),
          e("code", null, shortId(summary.refeed_of_workflow_run_id))
        )
      ),
      e("div", { className: "workflow-metrics" },
        e(WorkflowMetric, { label: "runs", value: summary.runCount }),
        e(WorkflowMetric, { label: "stages", value: summary.stageCount }),
        e(WorkflowMetric, { label: "outputs", value: summary.outputCount }),
        e(WorkflowMetric, { label: "inputs", value: summary.inputCount }),
        e(WorkflowMetric, { label: "files", value: summary.fileCount }),
        e(WorkflowMetric, { label: "target rows", value: summary.targetRowCount }),
        e(WorkflowMetric, { label: "succeeded", value: summary.succeededCount }),
        e(WorkflowMetric, { label: "failed", value: summary.failedCount })
      ),
      e("div", { className: "workflow-tags" },
        summary.datasets.map((dataset) => e("span", {
          className: `workflow-tag ${summary.datasetRoles?.[dataset] || ""}`,
          key: dataset,
        }, dataset)),
        summary.pipelineTypes.map((type) => e("span", { className: "workflow-tag muted", key: type }, type))
      ),
      e("div", { className: "workflow-actions" },
        e("button", { type: "button", className: "small-action", onClick: onDetails }, "Details"),
        e("button", { type: "button", className: "small-action", onClick: onProcess }, "Process Diagram"),
        e("button", { type: "button", className: "small-action", onClick: onDeveloper }, "Developer Model"),
        e("button", { type: "button", className: `small-action ${olOpen ? "active" : ""}`, onClick: onOpenLineage },
          olOpen ? "Hide OL" : "OL"
        ),
        e("button", { type: "button", className: `small-action ${jsonOpen ? "active" : ""}`, onClick: onJson },
          jsonOpen ? "Hide JSON" : "JSON"
        )
      )
    );
  }

  function WorkflowMetric({ label, value }) {
    return e("div", { className: "workflow-metric" },
      e("strong", null, value),
      e("span", null, label)
    );
  }

  function workflowSummaries(data) {
    return (data.executions || []).map((execution) => {
      const runs = filteredRuns(data, execution.workflow_run_id);
      const runIds = new Set(runs.map((run) => run.run_id));
      const links = (data.links || []).filter((link) => runIds.has(link.consumer_run_id));
      const inputEdges = links.flatMap((link) => link.edges || []);
      const sourceFileIds = new Set();
      runs.forEach((run) => run.file_id && sourceFileIds.add(run.file_id));
      inputEdges.forEach((edge) => edge.source_file_id && sourceFileIds.add(edge.source_file_id));
      const targetRows = workflowTargetRows(data, execution.workflow_run_id);
      const statusCounts = runs.reduce((counts, run) => {
        counts[run.status] = (counts[run.status] || 0) + 1;
        return counts;
      }, {});
      return {
        ...execution,
        runCount: runs.length,
        stageCount: runs.reduce((sum, run) => sum + (run.stages?.length || 0), 0),
        outputCount: links.length,
        inputCount: inputEdges.length,
        fileCount: sourceFileIds.size,
        targetRowCount: targetRows.length,
        succeededCount: statusCounts.succeeded || 0,
        failedCount: statusCounts.failed || 0,
        runningCount: statusCounts.running || 0,
        datasets: unique(runs.map((run) => run.dataset)).sort(),
        datasetRoles: workflowDatasetRoles(execution, runs),
        pipelineTypes: unique(runs.map((run) => run.pipeline_type)).sort(),
      };
    });
  }

  function workflowDatasetRoles(execution, runs) {
    if (execution.execution_type !== "refeed") return {};
    const changed = new Set(
      runs
        .filter((run) => ["ingestion", "canonicalization"].includes(run.pipeline_type))
        .map((run) => run.dataset)
    );
    return Object.fromEntries(
      unique(runs.map((run) => run.dataset)).map((dataset) => [
        dataset,
        changed.has(dataset) ? "changed" : "impacted",
      ])
    );
  }

  function workflowTargetRows(data, workflowRunId) {
    return Object.entries(data.tables || {}).flatMap(([tableName, rows]) =>
      rows
        .filter((row) => row._ods_workflow_run_id === workflowRunId)
        .map((row) => ({ tableName, ...row }))
    );
  }

  function buildWorkflowJson(data, workflowRunId) {
    const execution = data.executionByWorkflow[workflowRunId] || { workflow_run_id: workflowRunId };
    const runs = filteredRuns(data, workflowRunId).slice().sort(compareWorkflowRuns);
    const runIds = new Set(runs.map((run) => run.run_id));
    const links = (data.links || []).filter((link) => runIds.has(link.consumer_run_id));
    const sourceFileIds = new Set();
    runs.forEach((run) => run.file_id && sourceFileIds.add(run.file_id));
    links.forEach((link) => (link.edges || []).forEach((edge) => {
      if (edge.source_file_id) sourceFileIds.add(edge.source_file_id);
    }));

    return {
      workflow_run_id: workflowRunId,
      business_date: execution.business_date || null,
      execution_type: execution.execution_type || null,
      refeed_of_workflow_run_id: execution.refeed_of_workflow_run_id || null,
      description: execution.description || null,
      metrics: workflowSummaries(data).find((item) => item.workflow_run_id === workflowRunId) || null,
      runs: runs.map((run) => ({
        run_id: run.run_id,
        pipeline_type: run.pipeline_type,
        domain: run.domain,
        dataset: run.dataset,
        business_date: run.business_date,
        trigger_type: run.trigger_type,
        status: run.status,
        record_count_in: run.record_count_in,
        record_count_out: run.record_count_out,
        stages: run.stages || [],
        outputs: (data.linksByRun[run.run_id] || []).map((link) => ({
          output_link_id: outputLinkId(link),
          edge_type: link.edge_type,
          sink_type: link.sink_type || null,
          target_ref: link.target_ref,
          transform_version: link.transform_version || null,
          record_count: link.record_count,
          inputs: (link.edges || []).map((edge) => workflowJsonInput(edge)),
        })),
      })),
      files: Array.from(sourceFileIds)
        .map((fileId) => data.fileById[fileId])
        .filter(Boolean),
      target_rows: workflowTargetRows(data, workflowRunId).map((row) => ({
        table_name: row.tableName,
        row_id: row.row_id,
        output_link_id: rowOutputLinkId(row),
        workflow_run_id: row._ods_workflow_run_id,
        payload: row.payload,
      })),
    };
  }

  function workflowJsonInput(edge) {
    const sourceFileId = edge.source_file_id || "";
    const upstreamId = upstreamOutputLinkId(edge);
    const input = sourceFileId
      ? { type: "raw_file", id: sourceFileId }
      : { type: "output_link", id: upstreamId || null };
    return {
      input_edge_id: inputEdgeId(edge),
      input,
      input_slot: edge.input_slot ?? null,
      edge_type: edge.edge_type,
      source_ref: edge.source_ref || null,
      record_count: edge.record_count,
    };
  }

  function buildOpenLineageEvents(data, workflowRunId) {
    const execution = data.executionByWorkflow[workflowRunId] || { workflow_run_id: workflowRunId };
    return filteredRuns(data, workflowRunId)
      .slice()
      .sort(compareWorkflowRuns)
      .map((run) => {
        const links = data.linksByRun[run.run_id] || [];
        const inputEdges = links.flatMap((link) => link.edges || []);
        const runFacets = {
          ods_workflow: openLineageFacet({
            workflow_run_id: run.workflow_run_id,
            business_date: run.business_date,
            execution_type: execution.execution_type || null,
            refeed_of_workflow_run_id: execution.refeed_of_workflow_run_id || null,
          }),
          ods_run: openLineageFacet({
            pipeline_type: run.pipeline_type,
            domain: run.domain,
            dataset: run.dataset,
            trigger_type: run.trigger_type,
            record_count_in: run.record_count_in,
            record_count_out: run.record_count_out,
          }),
        };
        const orchestratorFacet = openLineageOrchestratorFacet(run);
        if (orchestratorFacet) runFacets.ods_orchestrator = orchestratorFacet;
        return {
          eventType: run.status === "failed" ? "FAIL" : "COMPLETE",
          eventTime: openLineageEventTime(run.finished_at || run.started_at),
          run: {
            runId: run.run_id,
            facets: runFacets,
          },
          job: {
            namespace: "ods-control-plane",
            name: `${run.domain}.${run.pipeline_type}.${run.dataset}`,
            facets: {
              ods_job: openLineageFacet({
                domain: run.domain,
                pipeline_type: run.pipeline_type,
                dataset: run.dataset,
              }),
            },
          },
          inputs: inputEdges.map((edge) => openLineageInputDataset(data, edge)),
          outputs: links.map((link) => openLineageOutputDataset(link)),
          producer: "ods-control-plane-dashboard",
          schemaURL: "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        };
      });
  }

  function openLineageInputDataset(data, edge) {
    const sourceFileId = edge.source_file_id || "";
    const upstreamId = upstreamOutputLinkId(edge);
    const upstream = upstreamId ? data.linkById[upstreamId] : null;
    const file = sourceFileId ? data.fileById[sourceFileId] : null;
    const path = sourceFileId
      ? file?.s3_raw_path || edge.source_ref?.path || `file:${sourceFileId}`
      : upstream?.target_ref?.path || `output_link:${upstreamId}`;
    const dataset = openLineageDatasetRef(path);
    return {
      ...dataset,
      facets: {
        ods_inputEdge: openLineageFacet({
          input_edge_id: inputEdgeId(edge),
          source_file_id: sourceFileId || null,
          upstream_output_link_id: upstreamId || null,
          edge_type: edge.edge_type,
          input_slot: edge.input_slot ?? null,
          record_count: edge.record_count,
        }),
      },
    };
  }

  function openLineageOutputDataset(link) {
    const path = link.target_ref?.path || `output_link:${outputLinkId(link)}`;
    const dataset = openLineageDatasetRef(path);
    return {
      ...dataset,
      facets: {
        ods_outputLink: openLineageFacet({
          output_link_id: outputLinkId(link),
          consumer_run_id: link.consumer_run_id,
          edge_type: link.edge_type,
          sink_type: link.sink_type || null,
          target_ref: link.target_ref,
          record_count: link.record_count,
          transform_version: link.transform_version || null,
        }),
      },
    };
  }

  function openLineageDatasetRef(path) {
    const text = String(path || "-");
    if (text.startsWith("s3://")) {
      const withoutScheme = text.slice("s3://".length);
      const slash = withoutScheme.indexOf("/");
      return {
        namespace: slash >= 0 ? `s3://${withoutScheme.slice(0, slash)}` : "s3",
        name: slash >= 0 ? withoutScheme.slice(slash + 1) : withoutScheme,
      };
    }
    if (text.startsWith("postgres://")) {
      return {
        namespace: "postgres",
        name: text.slice("postgres://".length),
      };
    }
    return {
      namespace: "ods-control-plane",
      name: text,
    };
  }

  function openLineageFacet(values) {
    return {
      _producer: "ods-control-plane-dashboard",
      _schemaURL: "https://example.com/ods-control-plane/facets/1-0-0/OdsControlFacet.json",
      ...values,
    };
  }

  // OpenLineage run facet carrying the Airflow orchestrator identity recorded on
  // cp.run_log (migration 020). Returned only when the run actually has an
  // orchestrator_type, so non-orchestrated runs stay unchanged.
  function openLineageOrchestratorFacet(run) {
    if (!run || !run.orchestrator_type) return null;
    return {
      _producer: "ods-control-plane-dashboard",
      _schemaURL: "https://example.com/ods-control-plane/facets/1-0-0/OdsOrchestratorFacet.json",
      type: run.orchestrator_type,
      dag_id: run.orchestrator_dag_id ?? null,
      run_id: run.orchestrator_run_id ?? null,
      task_id: run.orchestrator_task_id ?? null,
      try_number: run.orchestrator_try_number ?? null,
      map_index: run.orchestrator_map_index ?? null,
      url: run.orchestrator_url ?? null,
    };
  }

  function openLineageEventTime(value) {
    if (!value) return new Date().toISOString();
    const text = String(value).replace(" ", "T").replace(/\+00$/, "Z");
    const parsed = new Date(text);
    return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
  }

  function ControlTab(props) {
    const {
      data,
      selectedExecutionId,
      setSelectedExecutionId,
      selectedExecution,
      selectedRun,
      selectedRunId,
      setSelectedRunId,
      focusedLinkId,
      setFocusedLinkId,
    } = props;
    const runs = filteredRuns(data, selectedExecutionId);
    const runLinks = selectedRun ? data.linksByRun[selectedRun.run_id] || [] : [];
    const focusedTrace = focusedLinkId ? data.traces[focusedLinkId] || [] : [];

    return e(React.Fragment, null,
      e(ExecutionStrip, {
        data,
        selectedExecutionId,
        setSelectedExecutionId,
      }),
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Execution",
          value: selectedExecutionId,
          onChange: (value) => {
            setSelectedExecutionId(value);
            setFocusedLinkId("");
          },
          options: [
            ["all", "All executions"],
            ...data.executions.map((execution) => [
              execution.workflow_run_id,
              `${execution.business_date} · ${execution.execution_type} · ${shortId(execution.workflow_run_id)}`,
            ]),
          ],
        }),
        e(SelectField, {
          label: "Run",
          value: selectedRunId,
          onChange: (value) => {
            setSelectedRunId(value);
            setFocusedLinkId("");
          },
          options: runs.map((run) => [
            run.run_id,
            `${run.business_date} · ${run.pipeline_type} / ${run.dataset} · ${shortId(run.run_id)}`,
          ]),
        })
      ),
      e("section", { className: "summary-grid" },
        e(SummaryPanel, {
          title: "Execution",
          item: selectedExecution || scenarioSummary(data),
          keys: selectedExecution
            ? ["business_date", "execution_type", "description", "workflow_run_id"]
            : ["business_dates", "refeed_business_date"],
        }),
        e(SummaryPanel, {
          title: "Run",
          item: selectedRun || {},
          keys: ["pipeline_type", "dataset", "status", "record_count_in", "record_count_out", "run_id"],
        }),
        e("div", { className: "panel metric-panel" },
          e("div", { className: "metric" }, e("strong", null, runs.length), e("span", null, "runs in scope")),
          e("div", { className: "metric" }, e("strong", null, runLinks.length), e("span", null, "links on selected run")),
          e("div", { className: "metric" }, e("strong", null, focusedTrace.length), e("span", null, "trace hops focused"))
        )
      ),
      e("section", { className: "grid" },
        e("div", { className: "panel" },
          e("div", { className: "panel-head" }, e("h2", null, "Stages")),
          e("div", { className: "panel-body" },
            selectedRun?.stages?.length
              ? e("div", { className: "stage-list" },
                  selectedRun.stages.map((stage) => e(StageCard, { key: `${stage.stage}-${stage.attempt}`, stage }))
                )
              : e("div", { className: "empty" }, "No stages in scope")
          )
        ),
        e("div", { className: "panel" },
          e("div", { className: "panel-head" },
            e("h2", null, "Output Links"),
            e("span", { className: "pill" }, `${runLinks.length}`)
          ),
          e("div", { className: "panel-body diagram" },
            runLinks.length
              ? runLinks.map((link) => e(LinkCard, {
                  key: outputLinkId(link),
                  data,
                  link,
                  focused: focusedLinkId === outputLinkId(link),
                  onFocus: () => setFocusedLinkId(outputLinkId(link)),
                }))
              : e("div", { className: "empty" }, "No links for this run")
          )
        )
      ),
      focusedLinkId && e(TracePanel, {
        data,
        focusedLinkId,
        trace: focusedTrace,
      })
    );
  }

  function ExecutionStrip({ data, selectedExecutionId, setSelectedExecutionId }) {
    return e("section", { className: "execution-strip" },
      data.executions.map((execution) => e("button", {
        key: execution.workflow_run_id,
        type: "button",
        className: `execution-card ${selectedExecutionId === execution.workflow_run_id ? "active" : ""} ${execution.execution_type}`,
        onClick: () => setSelectedExecutionId(execution.workflow_run_id),
      },
        e("span", { className: `pill ${execution.execution_type === "refeed" ? "amber" : "green"}` },
          execution.execution_type
        ),
        e("strong", null, execution.business_date),
        e("small", null, execution.description),
        e("code", null, shortId(execution.workflow_run_id))
      ))
    );
  }

  function MetadataTab({ data, selectedExecutionId, setSelectedExecutionId, focusLink, focusRun }) {
    const executionId = selectedExecutionId === "all"
      ? data.executions[0]?.workflow_run_id
      : selectedExecutionId;
    const execution = data.executionByWorkflow[executionId] || data.executions[0];
    const runs = filteredRuns(data, execution?.workflow_run_id || "all")
      .slice()
      .sort(compareWorkflowRuns);

    return e(React.Fragment, null,
      e(ExecutionStrip, {
        data,
        selectedExecutionId: execution?.workflow_run_id,
        setSelectedExecutionId,
      }),
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Execution To Explain",
          value: execution?.workflow_run_id || "",
          onChange: setSelectedExecutionId,
          options: data.executions.map((item) => [
            item.workflow_run_id,
            `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`,
          ]),
        })
      ),
      e("section", { className: "metadata-intro panel" },
        e("div", { className: "panel-head" },
          e("h2", null, "How To Read This Execution"),
          e("span", { className: `pill ${execution?.execution_type === "refeed" ? "amber" : "green"}` },
            execution?.execution_type || "execution"
          )
        ),
        e("div", { className: "panel-body" },
          e("p", null,
            "Each row below is one control-plane run. It shows the run_log entry, stage_log entries, output_link rows it created, input_edge rows it read, and any target rows stamped with the output link."
          ),
          e("div", { className: "facts compact" },
            e("div", { className: "fact" }, e("span", null, "business_date"), e("code", null, execution?.business_date || "-")),
            e("div", { className: "fact" }, e("span", null, "workflow_run_id"), e("code", null, execution?.workflow_run_id || "-")),
            e("div", { className: "fact" }, e("span", null, "description"), e("code", null, execution?.description || "-"))
          )
        )
      ),
      e("section", { className: "metadata-flow" },
        runs.map((run, index) => e(MetadataStep, {
          key: run.run_id,
          data,
          run,
          stepNumber: index + 1,
          focusLink,
          focusRun,
        }))
      )
    );
  }

  function MetadataStep({ data, run, stepNumber, focusLink, focusRun }) {
    const links = data.linksByRun[run.run_id] || [];
    const inputEdges = links.flatMap((link) =>
      (link.edges || []).map((edge) => ({ link, edge }))
    );
    const stage = run.stages?.[0];

    return e("article", { className: "metadata-step panel" },
      e("div", { className: "step-rail" },
        e("span", null, stepNumber)
      ),
      e("div", { className: "step-main" },
        e("div", { className: "panel-head step-head" },
          e("div", null,
            e("h2", null, `${run.pipeline_type} / ${run.dataset}`),
            e("p", null, explainRun(run))
          ),
          e("button", { className: "small-action", type: "button", onClick: () => focusRun(run.run_id) },
            "open run"
          )
        ),
        e("div", { className: "step-grid" },
          e("div", { className: "step-section" },
            e("h3", null, "Control Rows Created"),
            e(MetadataFact, { label: "cp.run_log", value: `${run.status}; out=${run.record_count_out ?? "-"}` }),
            e(MetadataFact, { label: "run_id", value: run.run_id }),
            e(MetadataFact, { label: "business_date", value: run.business_date }),
            stage && e(MetadataFact, {
              label: "cp.run_stage_log",
              value: `${stage.stage}; ${stage.record_count_in ?? "-"} -> ${stage.record_count_out ?? "-"}`
            }),
            e(MetadataFact, { label: "output links", value: String(links.length) })
          ),
          e("div", { className: "step-section" },
            e("h3", null, "Inputs Read"),
            inputEdges.length
              ? inputEdges.map(({ link, edge }) => e(InputMetadataCard, {
                  key: `${outputLinkId(link)}-${inputEdgeId(edge)}`,
                  data,
                  edge,
                }))
              : e("div", { className: "empty small" }, "No upstream input edges")
          ),
          e("div", { className: "step-section" },
            e("h3", null, "Outputs Produced"),
            links.length
              ? links.map((link) => e(OutputMetadataCard, {
                  key: outputLinkId(link),
                  data,
                  link,
                  focusLink,
                }))
              : e("div", { className: "empty small" }, "No output links")
          )
        )
      )
    );
  }

  function InputMetadataCard({ data, edge }) {
    if (edge.source_file_id) {
      const file = data.fileById[edge.source_file_id];
      return e("div", { className: "metadata-card input" },
        e("strong", null, "Raw file input"),
        e("span", { className: "pill mini" }, edge.edge_type),
        e(MetadataFact, { label: "cp.file_catalogue.file_id", value: edge.source_file_id }),
        e(MetadataFact, { label: "raw path", value: file?.s3_raw_path || edge.source_ref?.path || "-" }),
        e(MetadataFact, { label: "file_md5", value: file?.file_md5 || "-" }),
        e(MetadataFact, { label: "records", value: String(edge.record_count) })
      );
    }

    const upstreamId = upstreamOutputLinkId(edge);
    const upstreamLink = data.linkById[upstreamId];
    const upstreamRun = upstreamLink ? data.runById[upstreamLink.consumer_run_id] : data.runById[edge.upstream_run_id];
    return e("div", { className: "metadata-card input" },
      e("strong", null, "Upstream output input"),
      e("span", { className: "pill mini" }, edge.edge_type),
      e(MetadataFact, {
        label: "upstream run",
        value: upstreamRun ? `${upstreamRun.pipeline_type} / ${upstreamRun.dataset}` : edge.upstream_run_id || "-"
      }),
      e(MetadataFact, { label: "upstream_output_link_id", value: upstreamId || "-" }),
      e(MetadataFact, { label: "upstream target", value: upstreamLink?.target_ref?.path || "-" }),
      e(MetadataFact, { label: "records consumed", value: String(edge.record_count) })
    );
  }

  function OutputMetadataCard({ data, link, focusLink }) {
    const linkId = outputLinkId(link);
    const targetRows = data.targetRowsByLink[linkId] || [];
    const consumers = data.consumersByLink[linkId] || [];
    return e("button", {
      type: "button",
      className: "metadata-card output",
      onClick: () => focusLink(linkId),
    },
      e("strong", null, link.edge_type),
      e("span", { className: "pill mini green" }, `${link.record_count} rows`),
      e(MetadataFact, { label: "cp.output_link.output_link_id", value: linkId }),
      e(MetadataFact, { label: "target_ref.path", value: link.target_ref?.path || "-" }),
      e(MetadataFact, { label: "content_hash", value: link.target_ref?.content_hash || "-" }),
      e(MetadataFact, {
        label: "target rows",
        value: targetRows.length
          ? `${targetRows.length} in ${unique(targetRows.map((row) => `ods.${row.tableName}`)).join(", ")}`
          : "none"
      }),
      e(MetadataFact, {
        label: "used by next step",
        value: consumers.length
          ? consumers.map((consumer) => {
              const run = data.runById[consumer.consumer_run_id];
              return `${run?.pipeline_type || "run"} / ${run?.dataset || shortId(consumer.consumer_run_id)}`;
            }).join(", ")
          : "not consumed"
      })
    );
  }

  function MetadataFact({ label, value }) {
    return e("div", { className: "metadata-fact" },
      e("span", null, label),
      e("code", null, value == null ? "-" : String(value))
    );
  }

  function SummaryPanel({ title, item, keys }) {
    return e("div", { className: "panel" },
      e("div", { className: "panel-head" }, e("h2", null, title)),
      e("div", { className: "panel-body" },
        e("div", { className: "facts" },
          keys.map((key) => e("div", { className: "fact", key },
            e("span", null, key),
            e("code", null, renderValue(item?.[key]))
          ))
        )
      )
    );
  }

  function StageCard({ stage }) {
    return e("div", { className: "stage-card" },
      e("div", { className: "link-title" },
        e("strong", null, stage.stage),
        e("span", { className: "pill green" }, stage.status)
      ),
      e("div", { className: "edge" }, e("span", null, "records"), e("code", null, `${stage.record_count_in ?? "-"} -> ${stage.record_count_out ?? "-"}`)),
      stage.metrics && e("div", { className: "edge" }, e("span", null, "metrics"), e("code", null, JSON.stringify(stage.metrics)))
    );
  }

  function LinkCard({ data, link, focused, onFocus }) {
    const linkId = outputLinkId(link);
    const run = data.runById[link.consumer_run_id];
    const execution = data.executionByWorkflow[link.workflow_run_id || run?.workflow_run_id];
    const trace = data.traces[linkId] || [];
    const rawPaths = unique(trace.map((hop) => hop.raw_s3_path).filter(Boolean));
    return e("button", {
      type: "button",
      className: `link-card ${focused ? "focused" : ""}`,
      onClick: onFocus,
    },
      e("div", { className: "link-title" },
        e("strong", null, link.edge_type),
        e("span", { className: `pill ${execution?.execution_type === "refeed" ? "amber" : ""}` },
          execution?.execution_type || "link"
        )
      ),
      e("div", { className: "edge" }, e("span", null, "output_link_id"), e("code", null, linkId)),
      e("div", { className: "edge" }, e("span", null, "target"), e("code", null, link.target_ref?.path || "-")),
      e("div", { className: "edge" }, e("span", null, "hash"), e("code", null, link.target_ref?.content_hash || "-")),
      e("div", { className: "edge" }, e("span", null, "records"), e("code", null, link.record_count)),
      e("div", { className: "edge-list" },
        link.edges.map((edge) => e("div", { className: "edge", key: inputEdgeId(edge) },
          e("span", null, edge.edge_type),
          e("code", null, upstreamOutputLinkId(edge) || edge.source_file_id || "-")
        ))
      ),
      rawPaths.length > 0 && e("div", { className: "raw-list" },
        rawPaths.map((path) => e("span", { className: "raw-chip", key: path }, path))
      )
    );
  }

  function TracePanel({ data, focusedLinkId, trace }) {
    const rawPaths = unique(trace.map((hop) => hop.raw_s3_path).filter(Boolean));
    return e("section", { className: "panel trace-panel" },
      e("div", { className: "panel-head" },
        e("h2", null, "Focused Trace"),
        e("span", { className: "pill green" }, shortId(focusedLinkId))
      ),
      e("div", { className: "panel-body" },
        rawPaths.length > 0 && e("div", { className: "raw-summary" },
          rawPaths.map((path) => e("span", { className: "raw-chip", key: path }, path))
        ),
        e("div", { className: "trace-grid" },
          trace.length
            ? trace.map((hop, index) => e("div", { className: "trace-card", key: `${hop.hop}-${hop.edge_type}-${index}` },
                e("div", { className: "link-title" },
                  e("strong", null, `Hop ${hop.hop}: ${hop.edge_type}`),
                  hop.raw_s3_path && e("span", { className: "pill amber" }, "raw")
                ),
                e("div", { className: "edge" }, e("span", null, "consumer"), e("code", null, hop.consumer_run_id)),
                e("div", { className: "edge" }, e("span", null, "upstream"), e("code", null, hop.upstream_run_id || "-")),
                e("div", { className: "edge" }, e("span", null, "file"), e("code", null, hop.source_file_id || "-")),
                e("div", { className: "edge" }, e("span", null, "raw path"), e("code", null, hop.raw_s3_path || "-"))
              ))
            : e("div", { className: "empty" }, "No trace rows for this link")
        )
      )
    );
  }

  function FlowTab({ data, selectedExecutionId, setSelectedExecutionId, flowScope, setFlowScope, focusRun }) {
    const scopeId = flowScope === "selected" ? selectedExecutionId : "all";
    const graph = buildFlow(data, scopeId);
    const flowKey = `${scopeId}-${graph.nodes.length}-${graph.edges.length}`;
    return e(React.Fragment, null,
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Graph Scope",
          value: flowScope,
          onChange: setFlowScope,
          options: [["overview", "Scenario overview"], ["selected", "Selected execution"]],
        }),
        e(SelectField, {
          label: "Execution",
          value: selectedExecutionId,
          onChange: setSelectedExecutionId,
          options: data.executions.map((execution) => [
            execution.workflow_run_id,
            `${execution.business_date} · ${execution.execution_type}`,
          ]),
        })
      ),
      e("section", { className: "flow-layout" },
        e("div", { className: "flow-wrap" },
          hasReactFlow
            ? e(ReactFlowProvider, null,
                e(Flow, {
                  key: flowKey,
                  defaultNodes: graph.nodes,
                  defaultEdges: graph.edges,
                  fitView: true,
                  fitViewOptions: { padding: 0.25 },
                  nodesDraggable: true,
                  nodesConnectable: false,
                  elementsSelectable: true,
                  panOnDrag: true,
                  panOnScroll: true,
                  zoomOnScroll: true,
                  zoomOnPinch: true,
                  onNodeDoubleClick: (_event, node) => focusRun(node.id),
                  minZoom: 0.2,
                  maxZoom: 1.8,
                },
                  e(Background, { gap: 18, size: 1 }),
                  e(MiniMap, { pannable: true, zoomable: true }),
                  e(Controls, null)
                )
              )
            : e("div", { className: "error" }, "React Flow did not load")
        ),
        e("aside", { className: "flow-side panel" },
          e("div", { className: "panel-head" },
            e("h2", null, "Edges"),
            e("span", { className: "pill" }, graph.edgeDetails.length)
          ),
          e("div", { className: "panel-body edge-detail-list" },
            graph.edgeDetails.map((edge) => e("button", {
              key: edge.id,
              type: "button",
              className: "edge-detail",
              onClick: () => focusRun(edge.targetId),
            },
              e("span", { className: `pill mini ${edge.executionType === "refeed" ? "amber" : "green"}` }, edge.businessDate),
              e("strong", null, edge.label),
              e("code", null, `${edge.source} -> ${edge.target}`),
              e("small", null, `${edge.edgeType} · ${edge.recordCount} rows`)
            ))
          )
        )
      )
    );
  }

  function buildFlow(data, selectedExecutionId) {
    const runs = filteredRuns(data, selectedExecutionId);
    const runSet = new Set(runs.map((run) => run.run_id));
    const order = { ingestion: 0, canonicalization: 1, merge: 2, sink: 3, aggregation: 4 };
    const datasetOrder = {
      customer: 0,
      transaction: 1,
      customer_transaction: 2,
      customer_transaction_daily: 3,
    };
    const executionIndex = Object.fromEntries(data.executions.map((execution, i) => [execution.workflow_run_id, i]));

    const nodes = runs.map((run) => {
      const execution = data.executionByWorkflow[run.workflow_run_id];
      const x = (order[run.pipeline_type] ?? 0) * 320;
      const y = selectedExecutionId === "all"
        ? (executionIndex[run.workflow_run_id] || 0) * 360 + (datasetOrder[run.dataset] || 0) * 72
        : (datasetOrder[run.dataset] || 0) * 145;
      return {
        id: run.run_id,
        position: { x, y },
        data: {
          label: e("div", { className: "flow-node" },
            e("strong", null, `${run.pipeline_type} / ${run.dataset}`),
            e("span", { className: `pill mini ${execution?.execution_type === "refeed" ? "amber" : "green"}` },
              `${run.business_date} ${execution?.execution_type || ""}`
            ),
            e("div", { className: "meta" },
              e("span", null, `records ${run.record_count_in ?? "-"} -> ${run.record_count_out ?? "-"}`),
              ...(run.stages || []).map((stage) =>
                e("span", { key: stage.stage }, `${stage.stage}: ${stage.record_count_in ?? "-"} -> ${stage.record_count_out ?? "-"}`)
              ),
              e("code", null, shortId(run.run_id))
            )
          ),
        },
        style: {
          border: execution?.execution_type === "refeed" ? "2px solid #a46113" : "1px solid #d9dee7",
          borderRadius: 8,
          padding: 10,
          width: 270,
          background: "#ffffff",
        },
      };
    });

    const edges = [];
    const edgeDetails = [];
    data.links.forEach((link) => {
      if (!runSet.has(link.consumer_run_id)) return;
      const targetRun = data.runById[link.consumer_run_id];
      const execution = data.executionByWorkflow[link.workflow_run_id || targetRun?.workflow_run_id];
      link.edges.forEach((edge) => {
        if (!edge.upstream_run_id || !runSet.has(edge.upstream_run_id)) return;
        const sourceRun = data.runById[edge.upstream_run_id];
        edges.push({
          id: inputEdgeId(edge),
          source: edge.upstream_run_id,
          target: link.consumer_run_id,
          label: "",
          animated: data.executionByWorkflow[link.workflow_run_id]?.execution_type === "refeed",
          style: {
            stroke: link.edge_type === "canonical_to_sink" ? "#2f7d59" : "#2364aa",
            strokeWidth: execution?.execution_type === "refeed" ? 3 : 2,
          },
          type: "smoothstep",
        });
        edgeDetails.push({
          id: inputEdgeId(edge),
          targetId: link.consumer_run_id,
          businessDate: targetRun?.business_date || execution?.business_date || "-",
          executionType: execution?.execution_type || "normal",
          source: `${sourceRun?.pipeline_type || "upstream"} / ${sourceRun?.dataset || shortId(edge.upstream_run_id)}`,
          target: `${targetRun?.pipeline_type || "target"} / ${targetRun?.dataset || shortId(link.consumer_run_id)}`,
          label: `${sourceRun?.dataset || "upstream"} -> ${targetRun?.dataset || "target"}`,
          edgeType: link.edge_type,
          recordCount: edge.record_count,
        });
      });
    });
    return { nodes, edges, edgeDetails };
  }

  function WorkflowDiagramTab({ data, selectedExecutionId, setSelectedExecutionId, focusLink }) {
    const execution = data.executionByWorkflow[selectedExecutionId] || data.executions[0];
    const runs = filteredRuns(data, execution.workflow_run_id)
      .slice()
      .sort(compareWorkflowRuns);
    const groups = groupDiagramRuns(runs);

    return e(React.Fragment, null,
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Execution",
          value: execution.workflow_run_id,
          onChange: setSelectedExecutionId,
          options: data.executions.map((item) => [
            item.workflow_run_id,
            `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`,
          ]),
        }),
        e("div", { className: "diagram-note" },
          e("strong", null, "Diagram rule"),
          e("span", null, "Runs do work. Output links describe produced outputs. Later runs consume upstream_output_link_id; only raw-file starts show file_id.")
        )
      ),
      e("section", { className: "lineage-board panel" },
        e("div", { className: "panel-head" },
          e("h2", null, `Workflow diagram: ${execution.business_date} ${execution.execution_type}`),
          e("span", { className: `pill ${execution.execution_type === "refeed" ? "amber" : "green"}` },
            execution.execution_type
          )
        ),
        e("div", { className: "lineage-board-scroll" },
          e("div", { className: "lineage-diagram-row" },
            groups.map((group, index) => e(React.Fragment, { key: group.key },
              e("section", { className: "diagram-level" },
                e("div", { className: "diagram-level-label" },
                  e("strong", null, group.label),
                  e("span", null, group.hint)
                ),
                e("div", { className: "diagram-level-runs" },
                  group.runs.map((run) =>
                    e(DiagramRunColumn, { key: run.run_id, data, run, focusLink })
                  )
                )
              ),
              index < groups.length - 1 && e("div", { className: "diagram-connector connected" },
                e("span", null, "next layer consumes output_link_id where applicable")
              )
            ))
          )
        )
      )
    );
  }

  function groupDiagramRuns(runs) {
    const buckets = {};
    runs.forEach((run) => {
      let key = run.pipeline_type;
      if (run.pipeline_type === "sink") {
        key = run.dataset === "customer_transaction_daily" ? "sink_aggregate" : "sink_detail";
      }
      if (!buckets[key]) buckets[key] = [];
      buckets[key].push(run);
    });
    const orderedKeys = ["ingestion", "canonicalization", "merge", "sink_detail", "aggregation", "sink_aggregate"];
    const labels = {
      ingestion: ["Raw file ingestion", "Customer and transaction can arrive independently."],
      canonicalization: ["Canonicalization / silver", "Each file is validated and transformed independently."],
      merge: ["Merge", "Parallel inputs converge into customer_transaction."],
      sink_detail: ["Detail target write", "The merged detail rows are written and stamped."],
      aggregation: ["Aggregation", "The detail output is summarized."],
      sink_aggregate: ["Aggregate target write", "The aggregate rows are written and stamped."],
    };
    return orderedKeys
      .filter((key) => buckets[key]?.length)
      .map((key) => ({
        key,
        label: labels[key][0],
        hint: labels[key][1],
        runs: buckets[key].slice().sort(compareWorkflowRuns),
      }));
  }

  function DiagramConnector({ data, fromRun, toRun }) {
    const fromLinks = data.linksByRun[fromRun.run_id] || [];
    const fromLinkIds = new Set(fromLinks.map((link) => outputLinkId(link)));
    const toLinks = data.linksByRun[toRun.run_id] || [];
    const consumesPrevious = toLinks.some((link) =>
      (link.edges || []).some((edge) => fromLinkIds.has(upstreamOutputLinkId(edge)))
    );
    return e("div", { className: `diagram-connector ${consumesPrevious ? "connected" : "parallel"}` },
      e("span", null, consumesPrevious ? "consumed as upstream_output_link_id" : "parallel or later branch")
    );
  }

  function DiagramRunColumn({ data, run, focusLink }) {
    const links = data.linksByRun[run.run_id] || [];
    const inputEdges = links.flatMap((link) => link.edges || []);
    const stages = run.stages || [];

    return e("article", { className: "diagram-column" },
      e("div", { className: "diagram-card run-card" },
        e("div", { className: "diagram-card-title" },
          e("span", { className: "pill mini" }, run.pipeline_type),
          e("strong", null, run.dataset)
        ),
        e("div", { className: "diagram-facts" },
          e(DiagramFact, { label: "run_id", value: shortId(run.run_id) }),
          e(DiagramFact, { label: "records", value: `${run.record_count_in ?? "-"} -> ${run.record_count_out ?? "-"}` }),
          e(DiagramFact, {
            label: "stages",
            value: stages.length ? stages.map((stage) => stage.stage).join(", ") : "-",
          })
        ),
        e("div", { className: "diagram-inputs" },
          e("strong", null, "Inputs"),
          inputEdges.length
            ? inputEdges.map((edge) => e(DiagramInput, { key: inputEdgeId(edge), data, edge }))
            : e("span", { className: "muted-text" }, "No input edge recorded")
        )
      ),
      e("div", { className: "diagram-down-arrow" }, "writes"),
      e("div", { className: "diagram-link-stack" },
        links.length
          ? links.map((link) => e(DiagramLinkCard, {
              key: outputLinkId(link),
              link,
              onClick: () => focusLink(outputLinkId(link)),
            }))
          : e("div", { className: "diagram-card link-card-diagram muted" },
              "No output link"
            )
      )
    );
  }

  function DiagramInput({ data, edge }) {
    const edgeId = inputEdgeId(edge);
    if (edge.source_file_id) {
      const file = data.fileById[edge.source_file_id];
      return e("div", { className: "diagram-input raw" },
        e("span", null, "input_edge -> raw file"),
        e("code", null, `input_edge_id ${shortId(edgeId)}`),
        e("code", null, `file_id ${shortId(edge.source_file_id)}`),
        e("small", null, file?.s3_raw_path || edge.source_ref?.path || "")
      );
    }
    const upstreamId = upstreamOutputLinkId(edge);
    const upstream = data.linkById[upstreamId];
    return e("div", { className: "diagram-input upstream" },
      e("span", null, "input_edge -> upstream output"),
      e("code", null, `input_edge_id ${shortId(edgeId)}`),
      e("code", null, `upstream_output_link_id ${shortId(upstreamId)}`),
      e("small", null, upstream?.edge_type || edge.edge_type || "")
    );
  }

  function DiagramLinkCard({ link, onClick }) {
    const linkId = outputLinkId(link);
    return e("button", { className: "diagram-card link-card-diagram", type: "button", onClick },
      e("div", { className: "diagram-card-title" },
        e("span", { className: "pill mini" }, link.edge_type),
        e("strong", null, "output_link")
      ),
      e("div", { className: "diagram-facts" },
        e(DiagramFact, { label: "output_link_id", value: shortId(linkId) }),
        e(DiagramFact, { label: "sink_type", value: link.sink_type || "-" }),
        e(DiagramFact, { label: "target", value: link.target_ref?.path || "-" }),
        e(DiagramFact, { label: "content_hash", value: shortId(link.target_ref?.content_hash) })
      )
    );
  }

  function DiagramFact({ label, value, stored }) {
    return e("div", { className: `diagram-fact ${stored ? "stored" : ""}` },
      e("span", null, label),
      e("code", null, value == null ? "-" : String(value))
    );
  }

  function ProcessModelTab({ data, selectedExecutionId, setSelectedExecutionId, focusLink }) {
    const [showGuide, setShowGuide] = React.useState(true);
    const execution = data.executionByWorkflow[selectedExecutionId] || data.executions[0];
    const runs = filteredRuns(data, execution.workflow_run_id)
      .slice()
      .sort(compareWorkflowRuns);
    const groups = groupDiagramRuns(runs);

    return e(React.Fragment, null,
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Execution",
          value: execution.workflow_run_id,
          onChange: setSelectedExecutionId,
          options: data.executions.map((item) => [
            item.workflow_run_id,
            `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`,
          ]),
        }),
        e("div", { className: "diagram-note" },
          e("strong", null, "Process model rule"),
          e("span", null, "Inside each process, the task records 1. input_edge rows for what it consumed, then 2. output_link rows for what it produced.")
        ),
        e("button", {
          type: "button",
          className: `small-action ${showGuide ? "active" : ""}`,
          onClick: () => setShowGuide(!showGuide),
        },
          showGuide ? "Hide Guide" : "Show Guide"
        )
      ),
      showGuide && e(ProcessModelGuide),
      e("section", { className: "process-board panel" },
        e("div", { className: "panel-head" },
          e("h2", null, `Process model: ${execution.business_date} ${execution.execution_type}`),
          e("span", { className: `pill ${execution.execution_type === "refeed" ? "amber" : "green"}` },
            execution.execution_type
          )
        ),
        e("div", { className: "process-board-scroll" },
          groups.map((group, index) => e(React.Fragment, { key: group.key },
            e(ProcessStageSection, { data, group, focusLink, showGuide }),
            index < groups.length - 1 && e("div", { className: "process-stage-arrow" },
              "next process consumes prior output_link through its own input_edge"
            )
          ))
        )
      )
    );
  }

  function ProcessModelGuide() {
    return e("section", { className: "process-guide panel" },
      e("div", { className: "process-guide-card task" },
        e("strong", null, "task / run_log"),
        e("span", null, "What executed. Owns stages and creates metadata rows.")
      ),
      e("div", { className: "process-guide-card input" },
        e("strong", null, "input_edge"),
        e("span", null, "What this task consumed. Points to a raw file or upstream output_link.")
      ),
      e("div", { className: "process-guide-card output" },
        e("strong", null, "output_link"),
        e("span", null, "What this task produced. Later tasks consume this ID.")
      ),
      e("div", { className: "process-guide-card stage" },
        e("strong", null, "run_stage_log"),
        e("span", null, "Internal checkpoints inside the task.")
      ),
      e("div", { className: "process-guide-card stored" },
        e("strong", null, "bold values"),
        e("span", null, "Stored directly in the named table/column.")
      )
    );
  }

  function ProcessStageSection({ data, group, focusLink, showGuide }) {
    return e("section", { className: `process-stage process-stage-${group.key}` },
      e("div", { className: "process-stage-head" },
        e("strong", null, group.label),
        e("span", null, group.hint)
      ),
      e("div", {
        className: `process-stage-lanes ${group.runs.length > 1 ? "multi" : "single"}`,
      },
        group.runs.map((run) => e(ProcessRunBox, {
          key: run.run_id,
          data,
          run,
          focusLink,
          showGuide,
        }))
      )
    );
  }

  function ProcessRunBox({ data, run, focusLink, showGuide }) {
    const links = data.linksByRun[run.run_id] || [];
    const inputItems = links.flatMap((link) =>
      (link.edges || []).map((edge) => ({ edge, link }))
    );
    const stages = run.stages || [];
    const inputCount = inputItems.length;
    const outputCount = links.length;

    return e("article", { className: "process-box" },
      e("div", { className: "process-task-card" },
        e("span", { className: "pill mini" }, "task"),
        e("strong", null, `${run.pipeline_type} / ${run.dataset}`),
        e("code", null, `run_log ${shortId(run.run_id)}`),
        e("small", null, `${run.record_count_in ?? "-"} -> ${run.record_count_out ?? "-"} records`)
      ),
      showGuide && e("div", { className: "process-ownership-strip" },
        e("span", null, "run_log owns run_stage_log"),
        e("span", null, "1 creates input_edge"),
        e("span", null, "2 creates output_link")
      ),
      e("div", { className: "process-action-grid" },
        e("div", { className: "process-action-column" },
          e("div", { className: "process-action-title" }, "1. task creates input_edge"),
          inputItems.length
            ? inputItems.map((item, index) => e(ProcessInputCard, {
                key: `${outputLinkId(item.link)}-${inputEdgeId(item.edge)}`,
                data,
                run,
                edge: item.edge,
                link: item.link,
                label: processStepLabel(1, index, inputCount),
              }))
            : e("div", { className: "process-empty-card" }, "No input_edge rows")
        ),
        e("div", { className: "process-action-column" },
          e("div", { className: "process-action-title" }, "2. task creates output_link"),
          links.length
            ? links.map((link, index) => e(ProcessOutputCard, {
                key: outputLinkId(link),
                run,
                link,
                label: processStepLabel(2, index, outputCount),
                onClick: () => focusLink(outputLinkId(link)),
              }))
            : e("div", { className: "process-empty-card" }, "No output_link rows")
        )
      ),
      e("div", { className: "process-stage-log-section" },
        e("div", { className: "process-action-title" }, "task stages"),
        stages.length
          ? e("div", { className: "process-stage-log-grid" },
              stages.map((stage, index) => e(ProcessStageLogCard, {
                key: `${run.run_id}-${stage.stage || index}-${stage.attempt || 0}`,
                run,
                stage,
                label: processStepLabel("stage", index, stages.length),
              }))
            )
          : e("div", { className: "process-empty-card" }, "No run_stage_log rows")
      )
    );
  }

  function ProcessInputCard({ data, run, edge, label }) {
    const edgeId = inputEdgeId(edge);
    const sourceFileId = edge.source_file_id || "";
    const upstreamId = upstreamOutputLinkId(edge);
    const upstream = upstreamId ? data.linkById[upstreamId] : null;
    const upstreamRun = upstream ? data.runById[upstream.consumer_run_id] : null;
    const file = sourceFileId ? data.fileById[sourceFileId] : null;
    const inputKind = sourceFileId ? "raw file" : "output_link";
    const inputId = sourceFileId || upstreamId;
    const inputName = sourceFileId
      ? file?.source_name || file?.file_name || "raw file"
      : `${upstreamRun?.pipeline_type || "upstream"} / ${upstreamRun?.dataset || "output"}`;
    const inputPath = sourceFileId
      ? file?.s3_raw_path || edge.source_ref?.path || "-"
      : upstream?.target_ref?.path || "-";

    return e("div", { className: "process-meta-card process-input-card" },
      e("div", { className: "process-card-title" },
        e("span", null, `${label} input_edge`),
        e("code", { className: "stored-value" }, shortId(edgeId))
      ),
      e("p", null, `created by task ${shortId(run.run_id)}`),
      e("div", { className: "process-table-name" }, "table: input_edge"),
      e("div", { className: "process-column-head" },
        e("span", null, "column"),
        e("span", null, "value")
      ),
      e("div", { className: "process-facts" },
        e(DiagramFact, { label: "input_edge_id", value: shortId(edgeId), stored: true }),
        e(DiagramFact, { label: sourceFileId ? "source_file_id" : "upstream_output_link_id", value: shortId(inputId), stored: true }),
        e(DiagramFact, { label: "reads", value: inputName }),
        e(DiagramFact, { label: "path", value: inputPath }),
        e(DiagramFact, { label: "input kind", value: inputKind })
      )
    );
  }

  function ProcessOutputCard({ run, link, label, onClick }) {
    const linkId = outputLinkId(link);
    return e("button", { className: "process-meta-card process-output-card", type: "button", onClick },
      e("div", { className: "process-card-title" },
        e("span", null, `${label} output_link`),
        e("code", { className: "stored-value" }, shortId(linkId))
      ),
      e("p", null, `created by task ${shortId(run.run_id)}`),
      e("div", { className: "process-table-name" }, "table: output_link"),
      e("div", { className: "process-column-head" },
        e("span", null, "column"),
        e("span", null, "value")
      ),
      e("div", { className: "process-facts" },
        e(DiagramFact, { label: "output_link_id", value: shortId(linkId), stored: true }),
        e(DiagramFact, { label: "consumer_run_id", value: shortId(run.run_id), stored: true }),
        e(DiagramFact, { label: "edge_type", value: link.edge_type || "-", stored: true }),
        e(DiagramFact, { label: "target_ref.path", value: link.target_ref?.path || "-", stored: true }),
        e(DiagramFact, { label: "record_count", value: link.record_count ?? "-", stored: true }),
        e(DiagramFact, { label: "target_ref.content_hash", value: shortId(link.target_ref?.content_hash), stored: true })
      )
    );
  }

  function ProcessStageLogCard({ run, stage, label }) {
    return e("div", { className: "process-meta-card process-stage-log-card" },
      e("div", { className: "process-card-title" },
        e("span", null, label),
        e("code", { className: "stored-value" }, stage.stage || "-")
      ),
      e("p", null, `stage recorded under task ${shortId(run.run_id)}`),
      e("div", { className: "process-table-name" }, "table: run_stage_log"),
      e("div", { className: "process-column-head" },
        e("span", null, "column"),
        e("span", null, "value")
      ),
      e("div", { className: "process-facts" },
        e(DiagramFact, { label: "run_id", value: shortId(run.run_id), stored: true }),
        e(DiagramFact, { label: "stage", value: stage.stage || "-", stored: true }),
        e(DiagramFact, { label: "attempt", value: stage.attempt ?? "-", stored: true }),
        e(DiagramFact, { label: "status", value: stage.status || "-", stored: true }),
        e(DiagramFact, { label: "record_count_in", value: stage.record_count_in ?? "-", stored: true }),
        e(DiagramFact, { label: "record_count_out", value: stage.record_count_out ?? "-", stored: true }),
        e(DiagramFact, { label: "started_at", value: shortTimestamp(stage.started_at), stored: true }),
        e(DiagramFact, { label: "finished_at", value: shortTimestamp(stage.finished_at), stored: true }),
        e(DiagramFact, { label: "metrics", value: stage.metrics ? JSON.stringify(stage.metrics) : "-", stored: true })
      )
    );
  }

  function processStepLabel(step, index, count) {
    if (step === "stage") {
      if (count <= 1) return "stage";
      return `stage ${index + 1}`;
    }
    if (count <= 1) return `${step}.`;
    return `${step}${String.fromCharCode(97 + index)}.`;
  }

  function DeveloperProcessModelTab({ data, selectedExecutionId, setSelectedExecutionId }) {
    const execution = data.executionByWorkflow[selectedExecutionId] || data.executions[0];
    const runs = filteredRuns(data, execution.workflow_run_id)
      .slice()
      .sort(compareWorkflowRuns);
    const groups = groupDiagramRuns(runs);
    const firstRun = runs[0] || null;
    const [selectedItem, setSelectedItem] = React.useState(null);
    const activeItem = selectedItem && selectedItem.workflow_run_id === execution.workflow_run_id
      ? selectedItem
      : firstRun
        ? { kind: "task", workflow_run_id: execution.workflow_run_id, run_id: firstRun.run_id }
        : null;
    const command = activeItem ? buildDeveloperCommand(data, activeItem) : null;
    const activeKey = activeItem ? developerItemKey(activeItem) : "";

    return e(React.Fragment, null,
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Execution",
          value: execution.workflow_run_id,
          onChange: setSelectedExecutionId,
          options: data.executions.map((item) => [
            item.workflow_run_id,
            `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`,
          ]),
        }),
        e("div", { className: "diagram-note" },
          e("strong", null, "Developer model"),
          e("span", null, "Click a task, input, output, or stage to see the wrapper call that creates the control-plane rows.")
        )
      ),
      e("section", { className: "developer-model-layout" },
        e("div", { className: "developer-model-board panel" },
          e("div", { className: "panel-head" },
            e("h2", null, `Developer model: ${execution.business_date} ${execution.execution_type}`),
            e("span", { className: `pill ${execution.execution_type === "refeed" ? "amber" : "green"}` },
              execution.execution_type
            )
          ),
          e("div", { className: "developer-board-scroll" },
            groups.map((group, index) => e(React.Fragment, { key: group.key },
              e(DeveloperStageSection, {
                data,
                group,
                activeKey,
                selectItem: setSelectedItem,
                workflowRunId: execution.workflow_run_id,
              }),
              index < groups.length - 1 && e("div", { className: "process-stage-arrow" },
                "next process consumes prior output_link through inputs=[...]"
              )
            ))
          )
        ),
        e("aside", { className: "developer-command-panel panel" },
          command
            ? e(DeveloperCommandView, { command })
            : e("div", { className: "empty" }, "Select a card")
        )
      )
    );
  }

  function DeveloperStageSection({ data, group, activeKey, selectItem, workflowRunId }) {
    return e("section", { className: `process-stage process-stage-${group.key}` },
      e("div", { className: "process-stage-head" },
        e("strong", null, group.label),
        e("span", null, group.hint)
      ),
      e("div", {
        className: `process-stage-lanes ${group.runs.length > 1 ? "multi" : "single"}`,
      },
        group.runs.map((run) => e(DeveloperRunBox, {
          key: run.run_id,
          data,
          run,
          activeKey,
          selectItem,
          workflowRunId,
        }))
      )
    );
  }

  function DeveloperRunBox({ data, run, activeKey, selectItem, workflowRunId }) {
    const links = data.linksByRun[run.run_id] || [];
    const inputItems = links.flatMap((link) =>
      (link.edges || []).map((edge) => ({ edge, link }))
    );
    const stages = run.stages || [];
    const pick = (item) => selectItem({ workflow_run_id: workflowRunId, ...item });

    return e("article", { className: "developer-run-box" },
      e(DeveloperClickCard, {
        kind: "task",
        title: `${run.pipeline_type} / ${run.dataset}`,
        subtitle: `run_log ${shortId(run.run_id)}`,
        active: activeKey === developerItemKey({ kind: "task", run_id: run.run_id }),
        onClick: () => pick({ kind: "task", run_id: run.run_id }),
      }),
      e("div", { className: "developer-card-grid" },
        e("div", { className: "developer-card-column" },
          e("div", { className: "process-action-title" }, "inputs"),
          inputItems.length
            ? inputItems.map(({ edge, link }, index) => e(DeveloperClickCard, {
                key: `${outputLinkId(link)}-${inputEdgeId(edge)}`,
                kind: "input",
                title: `${processStepLabel(1, index, inputItems.length)} input_edge`,
                subtitle: `${shortId(inputEdgeId(edge))} -> ${shortId(edge.source_file_id || upstreamOutputLinkId(edge))}`,
                active: activeKey === developerItemKey({ kind: "input", link_id: outputLinkId(link), edge_id: inputEdgeId(edge) }),
                onClick: () => pick({ kind: "input", run_id: run.run_id, link_id: outputLinkId(link), edge_id: inputEdgeId(edge) }),
              }))
            : e("div", { className: "process-empty-card" }, "No input")
        ),
        e("div", { className: "developer-card-column" },
          e("div", { className: "process-action-title" }, "outputs"),
          links.length
            ? links.map((link, index) => e(DeveloperClickCard, {
                key: outputLinkId(link),
                kind: "output",
                title: `${processStepLabel(2, index, links.length)} output_link`,
                subtitle: `${shortId(outputLinkId(link))} ${link.edge_type}`,
                active: activeKey === developerItemKey({ kind: "output", link_id: outputLinkId(link) }),
                onClick: () => pick({ kind: "output", run_id: run.run_id, link_id: outputLinkId(link) }),
              }))
            : e("div", { className: "process-empty-card" }, "No output")
        )
      ),
      e("div", { className: "developer-stage-clicks" },
        e("div", { className: "process-action-title" }, "stages"),
        stages.length
          ? stages.map((stage, index) => e(DeveloperClickCard, {
              key: `${run.run_id}-${stage.stage}-${stage.attempt || 0}-${index}`,
              kind: "stage",
              title: stages.length > 1 ? `stage ${index + 1}` : "stage",
              subtitle: `${stage.stage} ${stage.status}`,
              active: activeKey === developerItemKey({ kind: "stage", run_id: run.run_id, stage_name: stage.stage, attempt: stage.attempt || 1 }),
              onClick: () => pick({ kind: "stage", run_id: run.run_id, stage_name: stage.stage, attempt: stage.attempt || 1 }),
            }))
          : e("div", { className: "process-empty-card" }, "No stage")
      )
    );
  }

  function DeveloperClickCard({ kind, title, subtitle, active, onClick }) {
    return e("button", {
      type: "button",
      className: `developer-click-card ${kind} ${active ? "active" : ""}`,
      onClick,
    },
      e("strong", null, title),
      e("span", null, subtitle)
    );
  }

  function DeveloperCommandView({ command }) {
    return e(React.Fragment, null,
      e("div", { className: "panel-head" },
        e("div", null,
          e("h2", null, command.title),
          e("p", null, command.description)
        ),
        e("span", { className: `pill ${command.kind}` }, command.kind)
      ),
      e("pre", { className: "template-code developer-command-code" },
        e("code", null, command.code)
      )
    );
  }

  function buildDeveloperCommand(data, item) {
    if (item.kind === "task") return taskCommand(data.runById[item.run_id]);
    if (item.kind === "stage") {
      const run = data.runById[item.run_id];
      const stage = (run?.stages || []).find((candidate) =>
        candidate.stage === item.stage_name && (candidate.attempt || 1) === (item.attempt || 1)
      );
      return stageCommand(run, stage);
    }
    if (item.kind === "output") {
      const link = data.linkById[item.link_id];
      const run = data.runById[link?.consumer_run_id];
      return outputCommand(data, run, link);
    }
    if (item.kind === "input") {
      const link = data.linkById[item.link_id];
      const run = data.runById[link?.consumer_run_id];
      const edge = (link?.edges || []).find((candidate) => inputEdgeId(candidate) === item.edge_id);
      return inputCommand(data, run, link, edge);
    }
    return {
      kind: "unknown",
      title: "Unknown command",
      description: "No command available.",
      code: "",
    };
  }

  function taskCommand(run) {
    if (!run) return emptyCommand("task");
    const fileArg = run.file_id ? `,\n    file_id=${py(run.file_id)}` : "";
    // Airflow-driven runs carry orchestrator identity (migration 020). Surface
    // it as orchestrator={...} in the runs.start snippet when present.
    const orchestrator = runOrchestrator(run);
    const orchestratorArg = orchestrator
      ? `,\n    orchestrator=${py(orchestrator)}`
      : "";
    return {
      kind: "task",
      title: `Create task ${run.pipeline_type} / ${run.dataset}`,
      description: "Creates or identifies the run_log row for one restartable task execution.",
      code: `from control import runs\n\nrun_id = runs.start(\n    conn,\n    workflow_run_id=${py(run.workflow_run_id)},\n    pipeline_type=${py(run.pipeline_type)},\n    domain=${py(run.domain)},\n    dataset=${py(run.dataset)},\n    business_date=${py(run.business_date)},\n    trigger_type=${py(run.trigger_type || "manual")}${fileArg}${orchestratorArg},\n    commit=False,\n)\n\n# after the task succeeds\nruns.finalise(\n    conn,\n    run_id,\n    status=\"succeeded\",\n    record_count_out=${py(run.record_count_out)},\n    commit=False,\n)`,
    };
  }

  // The orchestrator={...} dict for a run, mirroring the keys runs.start records
  // on cp.run_log. Returns null for non-orchestrated runs so their snippet is
  // unchanged.
  function runOrchestrator(run) {
    if (!run || !run.orchestrator_type) return null;
    return {
      type: run.orchestrator_type,
      dag_id: run.orchestrator_dag_id ?? null,
      run_id: run.orchestrator_run_id ?? null,
      task_id: run.orchestrator_task_id ?? null,
      try_number: run.orchestrator_try_number ?? null,
      map_index: run.orchestrator_map_index ?? null,
      url: run.orchestrator_url ?? null,
    };
  }

  function stageCommand(run, stage) {
    if (!run || !stage) return emptyCommand("stage");
    return {
      kind: "stage",
      title: `Create stage ${stage.stage}`,
      description: "The stage context manager inserts run_stage_log at start and finishes it on success or failure.",
      code: `from control import stages\n\nwith stages.stage_scope(\n    conn,\n    run_id=${py(run.run_id)},\n    stage=${py(stage.stage)},\n    attempt=${py(stage.attempt || 1)},\n    commit=False,\n) as stage_log:\n    # do the stage work here\n    stage_log.record_in = ${py(stage.record_count_in)}\n    stage_log.record_out = ${py(stage.record_count_out)}\n    stage_log.metrics = ${py(stage.metrics || {})}\n\n# stage_scope calls cp.finish_stage(..., status=\"succeeded\") on clean exit`,
    };
  }

  function outputCommand(data, run, link) {
    if (!run || !link) return emptyCommand("output");
    const inputs = (link.edges || []).map((edge) => edgeInputObject(edge));
    const targetRows = data.targetRowsByLink[outputLinkId(link)] || [];
    const writer = targetRows.length ? "lineage.write_output_then_rows" : "lineage.write_output_link";
    const rowArg = targetRows.length ? `,\n    rows=target_rows  # ${targetRows.length} row(s) stamped with produced_output_link_id` : "";
    const sinkArg = link.sink_type ? `,\n    sink_type=${py(link.sink_type)}` : "";
    return {
      kind: "output",
      title: `Create output_link ${shortId(outputLinkId(link))}`,
      description: "Creates one output_link row and all of its input_edge rows from inputs=[...].",
      code: `from control import lineage\n\nproduced_output_link_id = ${writer}(\n    conn,\n    consumer_run_id=${py(run.run_id)},\n    edge_type=${py(link.edge_type)},\n    target_ref=${py(link.target_ref)},\n    record_count=${py(link.record_count)},\n    inputs=${py(inputs)}${rowArg}${sinkArg},\n    transform_version=${py(link.transform_version)},\n    commit=False,\n)\n\n# returns produced_output_link_id ${shortId(outputLinkId(link))}\n# inputs=[...] creates the input_edge row(s) for this output`,
    };
  }

  function inputCommand(data, run, link, edge) {
    if (!run || !link || !edge) return emptyCommand("input");
    const source = edge.source_file_id
      ? `raw file ${shortId(edge.source_file_id)}`
      : `upstream output_link ${shortId(upstreamOutputLinkId(edge))}`;
    return {
      kind: "input",
      title: `Create input_edge ${shortId(inputEdgeId(edge))}`,
      description: `This shows only the input payload for ${source}. The payload becomes an input_edge row when the parent output_link is written.`,
      code: `# input_edge is not created by a standalone API call.\n# This payload is passed inside inputs=[...] on the parent output_link write.\n\ninput_edge_payload = ${py(edgeInputObject(edge))}\n\n# creates input_edge ${shortId(inputEdgeId(edge))}\n# belongs to output_link ${shortId(outputLinkId(link))}\n# parent task/run_log ${shortId(run.run_id)}`,
    };
  }

  function edgeInputObject(edge) {
    const input = {
      edge_type: edge.edge_type,
      input_slot: edge.input_slot ?? 0,
      record_count: edge.record_count,
    };
    if (edge.source_file_id) {
      input.source_file_id = edge.source_file_id;
    }
    if (upstreamOutputLinkId(edge)) {
      input.upstream_output_link_id = upstreamOutputLinkId(edge);
    }
    if (edge.upstream_run_id) {
      input.upstream_run_id = edge.upstream_run_id;
    }
    if (edge.source_ref) {
      input.source_ref = edge.source_ref;
    }
    return input;
  }

  function emptyCommand(kind) {
    return {
      kind,
      title: `No ${kind} selected`,
      description: "Select another item.",
      code: "",
    };
  }

  function developerItemKey(item) {
    if (!item) return "";
    if (item.kind === "task") return `task:${item.run_id}`;
    if (item.kind === "stage") return `stage:${item.run_id}:${item.stage_name}:${item.attempt || 1}`;
    if (item.kind === "output") return `output:${item.link_id}`;
    if (item.kind === "input") return `input:${item.link_id}:${item.edge_id}`;
    return item.kind;
  }

  function py(value) {
    if (value === undefined || value === null) return "None";
    if (typeof value === "string") return JSON.stringify(value);
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return JSON.stringify(value, null, 4)
      .replace(/\n/g, "\n    ")
      .replace(/\btrue\b/g, "True")
      .replace(/\bfalse\b/g, "False")
      .replace(/\bnull\b/g, "None");
  }

  function RowsTab({
    data,
    selectedTable,
    setSelectedTable,
    selectedRowsWorkflowId,
    setSelectedRowsWorkflowId,
    selectedDate,
    setSelectedDate,
    targetRowSort,
    setTargetRowSort,
    openWorkflow,
    focusLink,
  }) {
    const [selectedTargetRowKey, setSelectedTargetRowKey] = React.useState("");
    const tableNames = Object.keys(data.tables || {});
    const allTableRows = data.tables[selectedTable] || [];
    const sortOptions = targetRowSortOptions(allTableRows);
    const rowVersions = targetRowVersionMap(allTableRows);
    const rows = allTableRows.filter((row) => {
      if (selectedRowsWorkflowId !== "all" && row._ods_workflow_run_id !== selectedRowsWorkflowId) return false;
      if (selectedDate === "all") return true;
      return row.payload?.business_date === selectedDate;
    }).slice().sort((a, b) => compareTargetRows(a, b, targetRowSort));
    const selectedTargetRow = rows.find((row) => targetRowKey(selectedTable, row) === selectedTargetRowKey) || null;
    const workflowCounts = rows.reduce((counts, row) => {
      const workflowId = row._ods_workflow_run_id || "unknown";
      counts[workflowId] = (counts[workflowId] || 0) + 1;
      return counts;
    }, {});

    return e(React.Fragment, null,
      e("div", { className: "toolbar" },
        e(SelectField, {
          label: "Table",
          value: selectedTable,
          onChange: setSelectedTable,
          options: tableNames.map((name) => [name, `ods.${name}`]),
        }),
        e(SelectField, {
          label: "Producer Workflow",
          value: selectedRowsWorkflowId,
          onChange: setSelectedRowsWorkflowId,
          options: [
            ["all", "All workflows"],
            ...data.executions.map((item) => [
              item.workflow_run_id,
              `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`,
            ]),
          ],
        }),
        e(SelectField, {
          label: "Business Date",
          value: selectedDate,
          onChange: setSelectedDate,
          options: [["all", "All dates"], ...(data.scenario?.business_dates || []).map((date) => [date, date])],
        }),
        e(SelectField, {
          label: "Order By",
          value: targetRowSort,
          onChange: setTargetRowSort,
          options: sortOptions,
        })
      ),
      e("section", { className: "target-row-summary" },
        e("strong", null, `${rows.length} target rows`),
        e("span", null, `from ${Object.keys(workflowCounts).length} producer workflow(s)`),
        Object.entries(workflowCounts).map(([workflowId, count]) => {
          const execution = data.executionByWorkflow[workflowId];
          return e("span", { className: "pill mini", key: workflowId },
            `${execution?.execution_type || "unknown"} ${shortId(workflowId)}: ${count}`
          );
        })
      ),
      selectedTargetRow && e(TargetRowLineagePanel, {
        data,
        tableName: selectedTable,
        tableRows: allTableRows,
        rowVersions,
        row: selectedTargetRow,
        updateInfo: rowVersions[targetRowKey(selectedTable, selectedTargetRow)],
        openWorkflow,
        focusLink,
      }),
      e("div", { className: "table-wrap" },
        e("table", null,
          e("thead", null, e("tr", null,
            e("th", null, "row"),
            e("th", null, "update"),
            e("th", null, "original_output_link_id"),
            e("th", null, "new_output_link_id"),
            e("th", null, "business_date"),
            e("th", null, "producer workflow"),
            e("th", null, "payload")
          )),
          e("tbody", null,
            rows.map((row) => {
              const execution = data.executionByWorkflow[row._ods_workflow_run_id];
              const linkId = rowOutputLinkId(row);
              const updateInfo = rowVersions[targetRowKey(selectedTable, row)] || { status: "original", label: "original", outputLinkId: linkId };
              return e("tr", {
                key: `${selectedTable}-${row.row_id}-${linkId}`,
                className: selectedTargetRowKey === targetRowKey(selectedTable, row) ? "selected-row" : "",
                onClick: () => setSelectedTargetRowKey(targetRowKey(selectedTable, row)),
              },
                e("td", null, row.row_id),
                e("td", null,
                  e("span", { className: `pill mini update-${updateInfo.status}` }, updateInfo.label)
                ),
                e("td", null, e("code", null, shortId(targetRowOriginalOutputLinkId(updateInfo, linkId)))),
                e("td", null, e("code", null, shortId(targetRowNewOutputLinkId(updateInfo, linkId)) || "-")),
                e("td", null, row.payload?.business_date || "-"),
                e("td", null,
                  e("span", { className: `pill ${execution?.execution_type === "refeed" ? "amber" : "green"}` },
                    execution?.execution_type || "unknown"
                  ),
                  e("div", { className: "mono table-id" }, `workflow ${shortId(row._ods_workflow_run_id)}`),
                  e("button", {
                    type: "button",
                    className: "small-action row-action",
                    onClick: (event) => {
                      event.stopPropagation();
                      openWorkflow(row._ods_workflow_run_id);
                    },
                  }, "Open workflow")
                ),
                e("td", null, e(PayloadView, { payload: row.payload }))
              );
            })
          )
        )
      )
    );
  }

  function targetRowOriginalOutputLinkId(updateInfo, linkId) {
    if (updateInfo?.status === "latest" && updateInfo.previousOutputLinkId) {
      return updateInfo.previousOutputLinkId;
    }
    return updateInfo?.outputLinkId || linkId || "";
  }

  function targetRowNewOutputLinkId(updateInfo, linkId) {
    if (updateInfo?.status === "superseded") return updateInfo.nextOutputLinkId || "";
    if (updateInfo?.status === "latest") return updateInfo.outputLinkId || linkId || "";
    return "";
  }

  function PayloadView({ payload }) {
    const entries = Object.entries(payload || {});
    return e("div", { className: "payload-grid" },
      entries.map(([key, value]) => e("div", { className: "payload-item", key },
        e("span", null, key),
        e("strong", null, String(value))
      ))
    );
  }

  function TargetRowLineagePanel({ data, tableName, tableRows, rowVersions, row, updateInfo, openWorkflow, focusLink }) {
    const linkId = rowOutputLinkId(row);
    const link = data.linkById[linkId];
    const producerRun = link ? data.runById[link.consumer_run_id] : null;
    const execution = data.executionByWorkflow[row._ods_workflow_run_id];
    const trace = data.traces[linkId] || [];
    const rawPaths = unique(trace.map((hop) => hop.raw_s3_path).filter(Boolean));
    const directEdges = link?.edges || [];
    const historyRows = targetRowHistoryRows(tableRows, row);

    return e("section", { className: "panel target-lineage-panel" },
      e("div", { className: "panel-head" },
        e("div", null,
          e("h2", null, `Selected target row: ods.${tableName}`),
          e("p", null, `Row history is shown first for this business key, followed by the selected row lineage. ${historyRows.length} version(s) found.`)
        ),
        e("div", { className: "target-lineage-actions" },
          e("span", { className: `pill mini update-${updateInfo?.status || "original"}` }, updateInfo?.label || "original"),
          e("span", { className: "pill mini green" }, `output_link ${shortId(linkId)}`),
          e("span", { className: "pill mini" }, `workflow ${shortId(row._ods_workflow_run_id)}`),
          e("button", { type: "button", className: "small-action", onClick: () => openWorkflow(row._ods_workflow_run_id) }, "Open workflow"),
          e("button", { type: "button", className: "small-action", onClick: () => focusLink(linkId) }, "Open control links")
        )
      ),
      e("div", { className: "target-lineage-section target-history-section" },
        e("h3", null, `Row History (${historyRows.length})`),
        e("div", { className: "target-history-list" },
          historyRows.map((historyRow, index) => e(TargetRowHistoryCard, {
            key: targetRowKey(tableName, historyRow),
            data,
            row: historyRow,
            updateInfo: rowVersions[targetRowKey(tableName, historyRow)] || { status: "original", label: "original", outputLinkId: rowOutputLinkId(historyRow) },
            selected: targetRowKey(tableName, historyRow) === targetRowKey(tableName, row),
            versionNumber: index + 1,
            openWorkflow,
            focusLink,
          }))
        )
      ),
      e("div", { className: "target-lineage-grid" },
        e("article", { className: "metadata-card output" },
          e("strong", null, "target row"),
          e(MetadataFact, { label: "row_id", value: row.row_id }),
          e(MetadataFact, { label: "business key", value: targetRowBusinessKey(row) }),
          e(MetadataFact, { label: "updated by workflow", value: `${shortId(row._ods_workflow_run_id)} (${execution?.execution_type || "unknown"})` }),
          e(MetadataFact, { label: "_ods_workflow_run_id", value: shortId(row._ods_workflow_run_id) }),
          e(MetadataFact, { label: "_ods_output_link_id", value: shortId(linkId) }),
          e(MetadataFact, { label: "producer", value: `${execution?.execution_type || "unknown"} ${shortId(row._ods_workflow_run_id)}` })
        ),
        e("article", { className: "metadata-card output" },
          e("strong", null, "output_link"),
          e(MetadataFact, { label: "output_link_id", value: shortId(linkId) }),
          e(MetadataFact, { label: "run", value: producerRun ? `${producerRun.pipeline_type} / ${producerRun.dataset}` : "-" }),
          e(MetadataFact, { label: "edge_type", value: link?.edge_type || "-" }),
          e(MetadataFact, { label: "target", value: link?.target_ref?.path || "-" }),
          e(MetadataFact, { label: "records", value: link?.record_count ?? "-" })
        )
      ),
      e("div", { className: "target-lineage-section" },
        e("h3", null, "Direct Inputs"),
        directEdges.length
          ? e("div", { className: "target-lineage-inputs" },
              directEdges.map((edge) => e(TargetInputEdgeCard, {
                key: inputEdgeId(edge),
                data,
                edge,
              }))
            )
          : e("div", { className: "empty" }, "No input edges recorded for this output")
      ),
      e("div", { className: "target-lineage-section" },
        e("h3", null, "Raw Files In Trace"),
        rawPaths.length
          ? e("div", { className: "raw-summary" },
              rawPaths.map((path) => e("span", { className: "raw-chip", key: path }, path))
            )
          : e("div", { className: "empty" }, "No raw file paths found in trace")
      )
    );
  }

  function TargetRowHistoryCard({ data, row, updateInfo, selected, versionNumber, openWorkflow, focusLink }) {
    const linkId = rowOutputLinkId(row);
    const link = data.linkById[linkId];
    const run = link ? data.runById[link.consumer_run_id] : null;
    const execution = data.executionByWorkflow[row._ods_workflow_run_id];
    return e("article", { className: `target-history-card ${selected ? "selected" : ""}` },
      e("div", { className: "target-history-version" },
        e("span", null, versionNumber),
        e("strong", null, "version")
      ),
      e("div", { className: "target-history-main" },
        e("div", { className: "target-history-head" },
          e("span", { className: `pill mini update-${updateInfo.status || "original"}` }, updateInfo.label || "original"),
          e("span", { className: `pill mini ${execution?.execution_type === "refeed" ? "amber" : "green"}` },
            execution?.execution_type || "unknown"
          ),
          selected && e("span", { className: "pill mini" }, "selected row")
        ),
        e("div", { className: "target-history-facts" },
          e(MetadataFact, { label: "row_id", value: row.row_id }),
          e(MetadataFact, { label: "this_row_output_link_id", value: shortId(linkId) }),
          e(MetadataFact, { label: "workflow_run_id", value: shortId(row._ods_workflow_run_id) }),
          e(MetadataFact, { label: "task", value: run ? `${run.pipeline_type} / ${run.dataset}` : "-" }),
          updateInfo.status === "superseded" && e(MetadataFact, {
            label: "replaced_by_output_link_id",
            value: shortId(updateInfo.nextOutputLinkId),
          }),
          updateInfo.status === "latest" && e(MetadataFact, {
            label: "replaces_output_link_id",
            value: shortId(updateInfo.previousOutputLinkId),
          }),
          updateInfo.status === "original" && e(MetadataFact, {
            label: "replacement_output_link_id",
            value: "-",
          })
        ),
        e("div", { className: "target-history-actions" },
          e("button", { type: "button", className: "small-action", onClick: () => openWorkflow(row._ods_workflow_run_id) }, "Open workflow"),
          e("button", { type: "button", className: "small-action", onClick: () => focusLink(linkId) }, "Open output")
        ),
        e(PayloadView, { payload: row.payload })
      )
    );
  }

  function targetRowHistoryRows(tableRows, row) {
    const businessKey = targetRowBusinessKey(row);
    return (tableRows || [])
      .filter((candidate) => targetRowBusinessKey(candidate) === businessKey)
      .slice()
      .sort((a, b) => compareScalar(a.row_id, b.row_id));
  }

  function TargetInputEdgeCard({ data, edge }) {
    const edgeId = inputEdgeId(edge);
    const sourceFileId = edge.source_file_id || "";
    const upstreamId = upstreamOutputLinkId(edge);
    const upstream = upstreamId ? data.linkById[upstreamId] : null;
    const upstreamRun = upstream ? data.runById[upstream.consumer_run_id] : null;
    const file = sourceFileId ? data.fileById[sourceFileId] : null;
    return e("article", { className: "flow-mini-card input" },
      e("strong", null, "input_edge"),
      e(FlowFact, { label: "input_edge_id", value: shortId(edgeId) }),
      sourceFileId
        ? e(React.Fragment, null,
            e(FlowFact, { label: "source_file_id", value: shortId(sourceFileId) }),
            e(FlowFact, { label: "raw path", value: file?.s3_raw_path || edge.source_ref?.path || "-" })
          )
        : e(React.Fragment, null,
            e(FlowFact, { label: "upstream_output_link_id", value: shortId(upstreamId) }),
            e(FlowFact, { label: "upstream run", value: upstreamRun ? `${upstreamRun.pipeline_type} / ${upstreamRun.dataset}` : "-" }),
            e(FlowFact, { label: "upstream target", value: upstream?.target_ref?.path || "-" })
          ),
      e(FlowFact, { label: "record_count", value: edge.record_count ?? "-" })
    );
  }

  function FlowFact({ label, value }) {
    return e("div", { className: "flow-fact" },
      e("span", null, label),
      e("code", null, value == null ? "-" : String(value))
    );
  }

  function targetRowSortOptions(rows) {
    const payloadKeys = unique(
      rows.flatMap((row) => Object.keys(row.payload || {}))
    ).sort();
    return [
      ["row_id:asc", "row_id ascending"],
      ["row_id:desc", "row_id descending"],
      ["business_date:asc", "business_date ascending"],
      ["business_date:desc", "business_date descending"],
      ["workflow:asc", "producer workflow ascending"],
      ["workflow:desc", "producer workflow descending"],
      ["output_link:asc", "output_link_id ascending"],
      ["output_link:desc", "output_link_id descending"],
      ...payloadKeys.flatMap((key) => [
        [`payload.${key}:asc`, `${key} ascending`],
        [`payload.${key}:desc`, `${key} descending`],
      ]),
    ];
  }

  function compareTargetRows(a, b, sortSpec) {
    const [field, direction = "asc"] = String(sortSpec || "row_id:asc").split(":");
    const av = targetRowSortValue(a, field);
    const bv = targetRowSortValue(b, field);
    const result = compareScalar(av, bv);
    return direction === "desc" ? -result : result;
  }

  function targetRowSortValue(row, field) {
    if (field === "row_id") return row.row_id;
    if (field === "business_date") return row.payload?.business_date || "";
    if (field === "workflow") return row._ods_workflow_run_id || "";
    if (field === "output_link") return rowOutputLinkId(row);
    if (field.startsWith("payload.")) return row.payload?.[field.slice("payload.".length)];
    return row.row_id;
  }

  function compareScalar(a, b) {
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    const an = typeof a === "number" ? a : Number(a);
    const bn = typeof b === "number" ? b : Number(b);
    if (!Number.isNaN(an) && !Number.isNaN(bn)) return an - bn;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  }

  function targetRowVersionMap(rows) {
    const groups = {};
    rows.forEach((row) => {
      const key = targetRowBusinessKey(row);
      if (!groups[key]) groups[key] = [];
      groups[key].push(row);
    });
    const result = {};
    Object.values(groups).forEach((group) => {
      const ordered = group.slice().sort((a, b) => compareScalar(a.row_id, b.row_id));
      ordered.forEach((row, index) => {
        const key = targetRowKey("", row);
        const outputLinkId = rowOutputLinkId(row);
        const previousOutputLinkId = index > 0 ? rowOutputLinkId(ordered[index - 1]) : "";
        const nextOutputLinkId = index < ordered.length - 1 ? rowOutputLinkId(ordered[index + 1]) : "";
        if (ordered.length === 1) {
          result[key] = { status: "original", label: "original", outputLinkId };
        } else if (index === ordered.length - 1) {
          result[key] = { status: "latest", label: "latest update", outputLinkId, previousOutputLinkId };
        } else {
          result[key] = { status: "superseded", label: "superseded", outputLinkId, nextOutputLinkId };
        }
      });
    });
    return result;
  }

  function targetRowBusinessKey(row) {
    const payload = row.payload || {};
    const idKeys = Object.keys(payload)
      .filter((key) => /(^id$|_id$)/.test(key))
      .sort();
    const parts = [["business_date", payload.business_date || ""]];
    const selectedKeys = idKeys.length ? idKeys : Object.keys(payload).sort();
    selectedKeys.forEach((key) => parts.push([key, payload[key]]));
    return parts.map(([key, value]) => `${key}=${value ?? ""}`).join(" | ");
  }

  function targetRowKey(tableName, row) {
    return `${row.row_id}:${rowOutputLinkId(row)}`;
  }

  function TemplatesTab() {
    const [selectedId, setSelectedId] = React.useState(SIMPLE_TEMPLATES[0].id);
    const selected = SIMPLE_TEMPLATES.find((template) => template.id === selectedId) || SIMPLE_TEMPLATES[0];
    const workflow = SIMPLE_TEMPLATE_WORKFLOWS[selected.id] || [];

    return e(React.Fragment, null,
      e("section", { className: "template-workflow panel" },
        e("div", { className: "panel-head template-workflow-head" },
          e("div", null,
            e("h2", null, `Template diagram: ${selected.title}`),
            e("p", null, "Read left to right: this diagram shows the metadata handoff for the selected template.")
          )
        ),
        e("div", { className: "template-workflow-scroll" },
          e("div", { className: "template-workflow-row" },
            workflow.map((item, index) =>
              e("div", { className: "workflow-step-wrap", key: item.step },
                e("article", { className: "workflow-step-card" },
                  e("span", { className: "workflow-step-number" }, item.step),
                  e("strong", null, item.title),
                  e("code", null, item.meta),
                  e("p", null, item.note)
                ),
                index < workflow.length - 1 && e("div", { className: "workflow-step-arrow" }, "->")
              )
            )
          )
        )
      ),
      e("div", { className: "templates-layout" },
        e("aside", { className: "templates-nav panel" },
          e("div", { className: "panel-head" },
            e("h2", null, "Control templates")
          ),
          e("div", { className: "template-list" },
            SIMPLE_TEMPLATES.map((template) =>
              e("button", {
                key: template.id,
                type: "button",
                className: `template-choice ${selected.id === template.id ? "active" : ""}`,
                onClick: () => setSelectedId(template.id),
              },
                e("span", { className: "pill mini" }, template.stage),
                e("strong", null, template.title),
                e("small", null, template.summary)
              )
            )
          )
        ),
        e("section", { className: "template-detail panel" },
          e("div", { className: "panel-head template-heading" },
            e("div", null,
              e("h2", null, selected.title),
              e("p", null, selected.summary)
            ),
            e("span", { className: "pill" }, selected.stage)
          ),
          e("div", { className: "panel-body template-body" },
            e("div", { className: "template-intent" },
              e("div", null,
                e("h3", null, "Inputs"),
                e("ul", null, selected.inputs.map((item) => e("li", { key: item }, item)))
              ),
              e("div", null,
                e("h3", null, "Control-table writes"),
                e("ul", null, selected.writes.map((item) => e("li", { key: item }, item)))
              )
            ),
            e("div", { className: "template-checklist" },
              e("strong", null, "Minimum metadata contract"),
              e("p", null, selected.purpose),
              e("ul", null,
                e("li", null, "Create or reuse a logical run so Airflow retries can resume or attach attempts to the same business task."),
                e("li", null, "Record stage progress so restart logic can tell whether the task failed while validating, transforming, writing, or publishing."),
                e("li", null, "Create input edges for every input so each output can be traced to raw files or upstream output links."),
                e("li", null, "Create one output link per written target so downstream tasks consume an exact output, not a guessed table or path."),
                e("li", null, "Activate the business slice only after the target write succeeds, so failed or partial outputs never become business-visible.")
              ),
              selected.commentary.map((item) =>
                e("p", { key: item }, item)
              )
            ),
            selected.dataModel && e(TemplateDataModel, { model: selected.dataModel }),
            e("pre", { className: "template-code" },
              e("code", null, selected.code)
            )
          )
        )
      )
    );
  }

  function TemplateDataModel({ model }) {
    return e("section", { className: "template-data-model" },
      e("div", { className: "data-model-head" },
        e("div", null,
          e("strong", null, "Draw.io entity relationship model"),
          e("p", null, model.summary)
        ),
        e("code", null, model.drawioPath)
      ),
      e("div", { className: "data-model-grid" },
        model.rows.map((row) => e("article", { className: "data-model-card", key: `${row.table}-${row.role}` },
          e("div", { className: "data-model-card-head" },
            e("span", { className: "pill mini" }, row.table),
            e("strong", null, row.role)
          ),
          e("div", { className: "data-model-fields" },
            row.fields.map(([label, value]) => e("div", { className: "data-model-field", key: label },
              e("span", null, label),
              e("code", null, value)
            ))
          )
        ))
      ),
      e("pre", { className: "template-code data-model-sql" },
        e("code", null, model.lookupSql)
      )
    );
  }

  function DocumentationTab() {
    const [selectedTopic, setSelectedTopic] = React.useState(DOCS_FAQ[0].topic);
    const selected = DOCS_FAQ.find((item) => item.topic === selectedTopic) || DOCS_FAQ[0];

    return e("div", { className: "docs-layout" },
      e("aside", { className: "docs-nav panel" },
        e("div", { className: "panel-head" },
          e("h2", null, "Common questions")
        ),
        e("div", { className: "docs-topic-list" },
          DOCS_FAQ.map((item) =>
            e("button", {
              key: item.topic,
              type: "button",
              className: `docs-topic ${selected.topic === item.topic ? "active" : ""}`,
              onClick: () => setSelectedTopic(item.topic),
            },
              e("strong", null, item.topic),
              e("small", null, item.question)
            )
          )
        )
      ),
      e("section", { className: "docs-detail panel" },
        e("div", { className: "panel-head docs-heading" },
          e("div", null,
            e("h2", null, selected.question),
            e("p", null, selected.topic)
          )
        ),
        e("div", { className: "panel-body docs-body" },
          e("div", { className: "docs-answer" },
            selected.answer.map((paragraph) =>
              e("p", { key: paragraph }, paragraph)
            )
          ),
          e("pre", { className: "template-code docs-code" },
            e("code", null, selected.code)
          )
        )
      )
    );
  }

  function SelectField({ label, value, onChange, options }) {
    return e("div", { className: "field" },
      e("label", null, label),
      e("select", { value, onChange: (event) => onChange(event.target.value) },
        options.map(([optionValue, optionLabel]) =>
          e("option", { key: optionValue, value: optionValue }, optionLabel)
        )
      )
    );
  }

  function filteredRuns(data, selectedExecutionId) {
    if (selectedExecutionId === "all") return data.runs || [];
    return (data.runs || []).filter((run) => run.workflow_run_id === selectedExecutionId);
  }

  function compareWorkflowRuns(a, b) {
    const order = { ingestion: 0, canonicalization: 1, merge: 2, sink: 3, aggregation: 4 };
    const datasetOrder = {
      customer: 0,
      transaction: 1,
      customer_transaction: 2,
      customer_transaction_daily: 3,
    };
    const byPipeline = (order[a.pipeline_type] ?? 99) - (order[b.pipeline_type] ?? 99);
    if (byPipeline) return byPipeline;
    const byDataset = (datasetOrder[a.dataset] ?? 99) - (datasetOrder[b.dataset] ?? 99);
    if (byDataset) return byDataset;
    return String(a.started_at || "").localeCompare(String(b.started_at || ""));
  }

  function explainRun(run) {
    if (run.pipeline_type === "ingestion") {
      return "Registers the raw file, creates the run/stage rows, and writes a raw_to_curated link anchored to cp.file_catalogue.";
    }
    if (run.pipeline_type === "canonicalization") {
      return "Reads the exact raw_to_curated output from ingestion and writes the silver curated_to_canonical output.";
    }
    if (run.pipeline_type === "merge") {
      return "Reads customer and transaction silver outputs, joins them, and writes the merged customer_transaction output.";
    }
    if (run.pipeline_type === "aggregation") {
      return "Reads the detail output and creates the customer-level daily aggregate output.";
    }
    if (run.pipeline_type === "sink") {
      return "Writes the output rows to Postgres and stamps each row with _ods_output_link_id.";
    }
    return "Control-plane run with input edges and output links.";
  }

  function scenarioSummary(data) {
    return {
      business_dates: (data.scenario?.business_dates || []).join(", "),
      refeed_business_date: data.scenario?.refeed_business_date || "-",
    };
  }

  function unique(items) {
    return [...new Set(items)];
  }

  function renderValue(value) {
    if (value == null) return "-";
    if (Array.isArray(value)) return value.join(", ");
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  }

  function shortId(id) {
    if (!id) return "-";
    return String(id).slice(0, 8);
  }

  function shortTimestamp(value) {
    if (!value) return "-";
    return String(value).replace("T", " ").slice(0, 19);
  }

  ReactDOM.createRoot(document.getElementById("root")).render(e(App));
})();
