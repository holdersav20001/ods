(function () {
  const e = React.createElement;

  function App() {
    const [data, setData] = React.useState(null);
    const [error, setError] = React.useState(null);
    const [executionId, setExecutionId] = React.useState("");
    const [expanded, setExpanded] = React.useState({});

    React.useEffect(() => {
      fetch("./data/demo-workflow.json", { cache: "no-store" })
        .then((res) => {
          if (!res.ok) throw new Error("snapshot not found");
          return res.json();
        })
        .then((snapshot) => {
          const normalized = normalize(snapshot);
          setData(normalized);
          setExecutionId(normalized.executions[0]?.workflow_run_id || "");
        })
        .catch((err) => setError(err.message));
    }, []);

    if (error) {
      return e("main", { className: "content" },
        e("div", { className: "error" }, `Unable to load dashboard snapshot: ${error}`)
      );
    }
    if (!data) {
      return e("main", { className: "content" },
        e("div", { className: "empty" }, "Loading metadata flow")
      );
    }

    const execution = data.executionByWorkflow[executionId] || data.executions[0];
    const runs = data.runs
      .filter((run) => run.workflow_run_id === execution.workflow_run_id)
      .slice()
      .sort(compareRuns);

    return e("div", { className: "app" },
      e("header", { className: "topbar" },
        e("div", { className: "brand" },
          e("h1", null, "ODS Metadata Flow"),
          e("span", null, "Control table rows connected from input to output")
        ),
        e("nav", { className: "tabs" },
          e("a", { className: "tab active", href: "./metadata-flow.html" }, "Metadata Flow"),
          e("a", { className: "tab", href: "./index.html" }, "Main Dashboard")
        )
      ),
      e("main", { className: "content metadata-page" },
        e("section", { className: "flow-dashboard-toolbar" },
          e("div", { className: "field" },
            e("label", null, "Execution"),
            e("select", {
              value: execution.workflow_run_id,
              onChange: (event) => setExecutionId(event.target.value),
            },
              data.executions.map((item) => e("option", {
                key: item.workflow_run_id,
                value: item.workflow_run_id,
              }, `${item.business_date} - ${item.execution_type} - ${shortId(item.workflow_run_id)}`))
            )
          ),
          e("div", { className: "flow-execution-summary" },
            e("span", { className: `pill ${execution.execution_type === "refeed" ? "amber" : "green"}` },
              execution.execution_type
            ),
            e("strong", null, execution.business_date),
            e("code", null, execution.workflow_run_id),
            e("span", null, execution.description)
          )
        ),
        e("section", { className: "vertical-metadata-flow" },
          runs.map((run, index) => e(FlowStep, {
            key: run.run_id,
            data,
            run,
            index,
            isLast: index === runs.length - 1,
            expanded: !!expanded[run.run_id],
            onToggle: () => setExpanded((state) => ({ ...state, [run.run_id]: !state[run.run_id] })),
          }))
        )
      )
    );
  }

  function FlowStep({ data, run, index, isLast, expanded, onToggle }) {
    const links = data.linksByRun[run.run_id] || [];
    const inputEdges = links.flatMap((link) =>
      (link.edges || []).map((edge) => ({ link, edge }))
    );
    const stage = run.stages?.[0];
    const inputCount = inputEdges.length;
    const targetRows = links.flatMap((link) => data.targetRowsByLink[link.lineage_link_id] || []);

    return e(React.Fragment, null,
      e("article", { className: "vertical-step" },
        e("div", { className: "vertical-step-number" }, index + 1),
        e("div", { className: "vertical-step-card panel" },
          e("div", { className: "vertical-step-head" },
            e("div", null,
              e("h2", null, `${run.pipeline_type} / ${run.dataset}`),
              e("p", null, explain(run))
            ),
            e("button", { type: "button", className: "small-action", onClick: onToggle },
              expanded ? "less detail" : "more detail"
            )
          ),
          e("div", { className: "metadata-lane" },
            e(LaneBlock, {
              title: "Input metadata",
              badge: `${inputCount} edge${inputCount === 1 ? "" : "s"}`,
              children: inputEdges.length
                ? inputEdges.map(({ link, edge }) => e(InputBlock, {
                    key: edge.lineage_edge_id,
                    data,
                    edge,
                    outputLink: link,
                  }))
                : e("div", { className: "flow-mini-card muted" }, "No upstream input. This stage starts from a registered raw file or creates no input edge.")
            }),
            e(Arrow, { label: "creates / updates" }),
            e(LaneBlock, {
              title: "Control rows",
              badge: "run + stage",
              children: e(ControlRowsBlock, { run, stage, expanded })
            }),
            e(Arrow, { label: "writes" }),
            e(LaneBlock, {
              title: "Output metadata",
              badge: `${links.length} link${links.length === 1 ? "" : "s"}`,
              children: links.length
                ? links.map((link) => e(OutputBlock, {
                    key: link.lineage_link_id,
                    data,
                    link,
                    expanded,
                  }))
                : e("div", { className: "flow-mini-card muted" }, "No output link recorded for this run.")
            })
          ),
          targetRows.length > 0 && e("div", { className: "target-row-summary" },
            e("strong", null, "Target rows stamped"),
            e("span", null, `${targetRows.length} row${targetRows.length === 1 ? "" : "s"} across ${unique(targetRows.map((row) => `ods.${row.tableName}`)).join(", ")}`),
            e("code", null, unique(targetRows.map((row) => shortId(row._ods_lineage_link_id))).join(", "))
          )
        )
      ),
      !isLast && e("div", { className: "between-step-arrow" },
        e("span", null, "next step discovers or consumes the exact upstream output (upstream_output_link_id)")
      )
    );
  }

  function LaneBlock({ title, badge, children }) {
    return e("section", { className: "lane-block" },
      e("div", { className: "lane-head" },
        e("h3", null, title),
        e("span", { className: "pill mini" }, badge)
      ),
      e("div", { className: "lane-body" }, children)
    );
  }

  function Arrow({ label }) {
    return e("div", { className: "lane-arrow" },
      e("span", null, label)
    );
  }

  function InputBlock({ data, edge }) {
    if (edge.source_file_id) {
      const file = data.fileById[edge.source_file_id];
      return e("div", { className: "flow-mini-card input" },
        e("strong", null, "Input edge — raw file anchor"),
        e("span", { className: "card-table-ref" }, "cp.input_edge -> cp.file_catalogue"),
        e(Fact, { label: "source_file_id", value: edge.source_file_id }),
        e(Fact, { label: "raw path", value: file?.s3_raw_path || edge.source_ref?.path || "-" }),
        e(Fact, { label: "file_md5", value: file?.file_md5 || "-" }),
        e(Fact, { label: "record_count", value: edge.record_count })
      );
    }

    const upstreamLink = data.linkById[edge.upstream_lineage_link_id];
    const upstreamRun = upstreamLink ? data.runById[upstreamLink.consumer_run_id] : data.runById[edge.upstream_run_id];
    return e("div", { className: "flow-mini-card input" },
      e("strong", null, "Input edge — consumes upstream output"),
      e("span", { className: "card-table-ref" }, "cp.input_edge"),
      e(Fact, {
        label: "upstream run",
        value: upstreamRun ? `${upstreamRun.pipeline_type} / ${upstreamRun.dataset}` : edge.upstream_run_id || "-"
      }),
      e(Fact, { label: "upstream_output_link_id", value: edge.upstream_lineage_link_id || "-" }),
      e(Fact, { label: "upstream target", value: upstreamLink?.target_ref?.path || "-" }),
      e(Fact, { label: "record_count", value: edge.record_count })
    );
  }

  function ControlRowsBlock({ run, stage, expanded }) {
    return e("div", { className: "flow-mini-card control" },
      e("strong", null, "cp.run_log"),
      e(Fact, { label: "run_id", value: run.run_id }),
      e(Fact, { label: "pipeline_type", value: run.pipeline_type }),
      e(Fact, { label: "dataset", value: run.dataset }),
      e(Fact, { label: "status", value: run.status }),
      e(Fact, { label: "record_count_out", value: run.record_count_out ?? "-" }),
      stage && e(React.Fragment, null,
        e("strong", { className: "subhead" }, "cp.run_stage_log"),
        e(Fact, { label: "stage", value: stage.stage }),
        e(Fact, { label: "records", value: `${stage.record_count_in ?? "-"} -> ${stage.record_count_out ?? "-"}` })
      ),
      expanded && e(React.Fragment, null,
        e(Fact, { label: "workflow_run_id", value: run.workflow_run_id }),
        e(Fact, { label: "trigger_type", value: run.trigger_type }),
        e(Fact, { label: "started_at", value: run.started_at }),
        e(Fact, { label: "finished_at", value: run.finished_at })
      )
    );
  }

  function OutputBlock({ data, link, expanded }) {
    const consumers = data.consumersByLink[link.lineage_link_id] || [];
    const targetRows = data.targetRowsByLink[link.lineage_link_id] || [];
    return e("div", { className: "flow-mini-card output" },
      e("strong", null, "Output link — what this run produced"),
      e("span", { className: "card-table-ref" }, "cp.output_link"),
      e(Fact, { label: "output_link_id", value: link.lineage_link_id }),
      e(Fact, { label: "edge_type", value: link.edge_type }),
      e(Fact, { label: "target_ref.path", value: link.target_ref?.path || "-" }),
      e(Fact, { label: "content_hash", value: link.target_ref?.content_hash || "-" }),
      e(Fact, { label: "record_count", value: link.record_count }),
      e(Fact, {
        label: "target rows",
        value: targetRows.length
          ? `${targetRows.length} in ${unique(targetRows.map((row) => `ods.${row.tableName}`)).join(", ")}`
          : "none"
      }),
      e(Fact, {
        label: "next consumers",
        value: consumers.length
          ? consumers.map((consumer) => {
              const run = data.runById[consumer.consumer_run_id];
              return `${run?.pipeline_type || "run"} / ${run?.dataset || shortId(consumer.consumer_run_id)}`;
            }).join(", ")
          : "none"
      }),
      expanded && e(Fact, { label: "transform_version", value: link.transform_version || "-" })
    );
  }

  function Fact({ label, value }) {
    return e("div", { className: "flow-fact" },
      e("span", null, label),
      e("code", null, value == null ? "-" : String(value))
    );
  }

  function normalize(snapshot) {
    const executionByWorkflow = Object.fromEntries(
      (snapshot.executions || []).map((execution) => [execution.workflow_run_id, execution])
    );
    const runById = Object.fromEntries((snapshot.runs || []).map((run) => [run.run_id, run]));
    const linkById = Object.fromEntries((snapshot.links || []).map((link) => [link.lineage_link_id, link]));
    const fileById = Object.fromEntries((snapshot.files || []).map((file) => [file.file_id, file]));
    const linksByRun = {};
    const targetRowsByLink = {};
    const consumersByLink = {};

    (snapshot.links || []).forEach((link) => {
      if (!linksByRun[link.consumer_run_id]) linksByRun[link.consumer_run_id] = [];
      linksByRun[link.consumer_run_id].push(link);
      (link.edges || []).forEach((edge) => {
        if (!edge.upstream_lineage_link_id) return;
        if (!consumersByLink[edge.upstream_lineage_link_id]) consumersByLink[edge.upstream_lineage_link_id] = [];
        consumersByLink[edge.upstream_lineage_link_id].push({
          consumer_run_id: link.consumer_run_id,
          consumer_link_id: link.lineage_link_id,
          edge_type: link.edge_type,
          record_count: edge.record_count,
        });
      });
    });

    Object.entries(snapshot.tables || {}).forEach(([tableName, rows]) => {
      rows.forEach((row) => {
        const linkId = row._ods_lineage_link_id;
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

  function compareRuns(a, b) {
    const order = { ingestion: 0, canonicalization: 1, merge: 2, sink: 3, aggregation: 4 };
    const datasetOrder = {
      customer: 0,
      transaction: 1,
      customer_transaction: 2,
      customer_transaction_daily: 3,
    };
    return (order[a.pipeline_type] ?? 99) - (order[b.pipeline_type] ?? 99)
      || (datasetOrder[a.dataset] ?? 99) - (datasetOrder[b.dataset] ?? 99)
      || String(a.started_at || "").localeCompare(String(b.started_at || ""));
  }

  function explain(run) {
    if (run.pipeline_type === "ingestion") {
      return "Starts from a raw file. The output link is anchored to cp.file_catalogue through source_file_id.";
    }
    if (run.pipeline_type === "canonicalization") {
      return "Consumes the exact raw_to_curated output link and creates the silver output link.";
    }
    if (run.pipeline_type === "merge") {
      return "Consumes the customer and transaction silver output links and creates the joined detail output.";
    }
    if (run.pipeline_type === "sink") {
      return "Writes target rows and stamps each row with the canonical_to_sink output link.";
    }
    if (run.pipeline_type === "aggregation") {
      return "Consumes the detail output and creates the daily aggregate output link.";
    }
    return "Consumes upstream output links and creates new output links.";
  }

  function unique(items) {
    return [...new Set(items)];
  }

  function shortId(id) {
    if (!id) return "-";
    return String(id).slice(0, 8);
  }

  ReactDOM.createRoot(document.getElementById("root")).render(e(App));
})();
