import { useEffect, useState, createContext, useContext } from 'react';

const YamlModalContext = createContext(() => {});

export function YamlModalProvider({ children }) {
  const [open, setOpen] = useState(null); // {domain,dataset,kind}
  const close = () => setOpen(null);
  return (
    <YamlModalContext.Provider value={setOpen}>
      {children}
      {open && <YamlModal {...open} onClose={close} />}
    </YamlModalContext.Provider>
  );
}

function YamlModal({ domain, dataset, kind, onClose }) {
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    setData(null); setErr(null);
    fetch(`/api/yaml/dataset/${domain}/${dataset}/${kind}`)
      .then(r => r.ok ? r.json() : Promise.reject(`HTTP ${r.status}`))
      .then(setData)
      .catch(e => setErr(String(e)));
  }, [domain, dataset, kind]);
  return (
    <div onClick={onClose}
         style={{
           position: 'fixed', inset: 0, background: 'rgba(15,23,42,0.55)',
           display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 100,
         }}>
      <div onClick={e => e.stopPropagation()}
           style={{
             background: '#fff', borderRadius: 8, width: 'min(800px, 92vw)',
             maxHeight: '88vh', display: 'flex', flexDirection: 'column',
             boxShadow: '0 10px 40px rgba(0,0,0,0.25)',
           }}>
        <header style={{ padding: '12px 16px', borderBottom: '1px solid #e2e8f0',
                          display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <div style={{ fontSize: 11, color: '#64748b' }}>{domain} · {dataset}</div>
            <div style={{ fontSize: 14, fontWeight: 600 }}>{kind}.yaml</div>
            {data?.path && <div style={{ fontSize: 10, color: '#94a3b8' }}>{data.path}</div>}
          </div>
          <button onClick={onClose}
                  style={{ background: 'transparent', border: 'none', fontSize: 20, cursor: 'pointer' }}>×</button>
        </header>
        <pre style={{
          margin: 0, padding: 16, overflow: 'auto', flex: 1,
          background: '#0f172a', color: '#e2e8f0',
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
          fontSize: 12, lineHeight: 1.5, whiteSpace: 'pre-wrap',
        }}>
          {err ? `Error: ${err}` : (data?.content ?? 'loading…')}
        </pre>
      </div>
    </div>
  );
}

/**
 * Map a pipeline_type to the declarative configs that STAGE actually
 * reads. Keeping this tight is what stops the dashboard from giving the
 * wrong impression that, say, the postgres-write step "uses" transform.yaml.
 *
 *  ingestion / stage      → contract (schema), quality (DQ rules), source
 *  canonicalize           → transform (THE rename/cast/drop)
 *  direct_postgres / load → delivery, reconciliation
 *  merge                  → dataset (merge composition)
 *  orchestration          → nothing — purely scheduling
 */
function configsForRun(pipelineType, run) {
  const schemaLabel = run?.dataset_schema_id
    ? `schema ${run.dataset_schema_id} v${run.dataset_schema_version}`
    : 'contract';
  const transformLabel = `transform · ${
    run?.dataset_is_canonical === 'True' ? 'canonical' : 'non-canonical'
  }`;
  const t = (pipelineType || '').toLowerCase();
  switch (t) {
    case 'ingestion':
    case 'stage':
      return [
        { kind: 'contract', label: schemaLabel },
        { kind: 'quality',  label: 'quality (DQ rules)' },
        { kind: 'source',   label: 'source' },
      ];
    case 'canonicalize':
      return [
        { kind: 'transform', label: transformLabel },
        { kind: 'contract',  label: schemaLabel },  // canonicalize honours schema
      ];
    case 'load':
    case 'direct_postgres':
    case 'postgres_write':
      return [
        { kind: 'delivery',       label: 'delivery' },
        { kind: 'reconciliation', label: 'reconciliation' },
      ];
    case 'merge':
      return [
        { kind: 'dataset', label: 'dataset (merge composition)' },
      ];
    case 'orchestration':
      return [];
    default:
      // Unknown pipeline_type — show the full set so we never hide info.
      return [
        { kind: 'contract',       label: schemaLabel },
        { kind: 'transform',      label: transformLabel },
        { kind: 'quality',        label: 'quality' },
        { kind: 'dataset',        label: 'dataset' },
        { kind: 'delivery',       label: 'delivery' },
        { kind: 'source',         label: 'source' },
        { kind: 'reconciliation', label: 'reconciliation' },
      ];
  }
}


function YamlBadge({ kind, label, domain, dataset }) {
  const open = useContext(YamlModalContext);
  if (!domain || !dataset) return <Tag>{label}</Tag>;
  const colors = {
    schema:    ['#dbeafe', '#1e40af'],
    transform: ['#fef3c7', '#92400e'],
    contract:  ['#dbeafe', '#1e40af'],
    quality:   ['#dcfce7', '#166534'],
  };
  const [bg, fg] = colors[kind] || ['#e0e7ff', '#3730a3'];
  return (
    <span onClick={() => open({ domain, dataset, kind: kind === 'schema' ? 'contract' : kind })}
          title={`open ${kind}.yaml`}
          style={{
            display: 'inline-block', marginRight: 4, marginBottom: 2,
            padding: '2px 8px', borderRadius: 4, fontSize: 11,
            background: bg, color: fg, cursor: 'pointer',
            border: `1px solid ${fg}33`,
          }}>
      📄 {label}
    </span>
  );
}

export default function NodeDetails({ node, link, onJumpToLink }) {
  // Empty state — show the write event itself.
  if (!node) {
    return (
      <div style={{ padding: 16 }}>
        <h3 style={{ marginTop: 0, fontSize: 14 }}>Write event</h3>
        {link ? <KV obj={link} /> : <Empty>Click a node to inspect.</Empty>}
      </div>
    );
  }

  const raw = node.data?.raw || {};
  const kind = raw.kind;
  const idStr = raw.id || '';

  return (
    <div style={{ padding: 16 }}>
      <h3 style={{ marginTop: 0, fontSize: 14 }}>
        {kind ? kind.replace('_', ' ') : 'node'}
      </h3>
      <div style={{ fontSize: 12, color: '#1f2937', wordBreak: 'break-all', marginBottom: 12 }}>
        {raw.label}
      </div>
      {renderPanel(kind, idStr, raw, onJumpToLink)}
    </div>
  );
}

function renderPanel(kind, idStr, raw, onJumpToLink) {
  if (kind === 'upstream_run' || kind === 'consumer_run') {
    const m = /^run:(.+)$/.exec(idStr);
    return m ? <RunPanel runId={m[1]} onJumpToLink={onJumpToLink} />
             : <KV obj={raw.data || {}} />;
  }
  if (kind === 'raw_file') {
    const m = /^file:(.+)$/.exec(idStr);
    return m ? <FilePanel fileId={m[1]} fallback={raw.data} />
             : <KV obj={raw.data || {}} />;
  }
  if (kind === 'write_event') {
    const m = /^link:(.+)$/.exec(idStr);
    return m ? <WriteEventPanel linkId={m[1]} fallback={raw.data}
                                 onJumpToLink={onJumpToLink} />
             : <KV obj={raw.data || {}} />;
  }
  if (kind === 'target') {
    const m = /^target:(.+)$/.exec(idStr);
    return m ? <TargetPanel ref_={m[1]} onJumpToLink={onJumpToLink} />
             : <KV obj={raw.data || {}} />;
  }
  return <KV obj={raw.data || {}} />;
}

function FilePanel({ fileId, fallback }) {
  const [data, setData] = useState(null);
  const [err, setErr]   = useState(null);
  useEffect(() => {
    setData(null); setErr(null);
    fetch(`/api/file/${fileId}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(setData)
      .catch(e => setErr(String(e)));
  }, [fileId]);
  if (err)  return <KV obj={fallback || {}} />;
  if (!data) return <Empty>loading file…</Empty>;
  return (
    <>
      <Section title="file_catalogue"><KV obj={data.file} /></Section>
      <Section title={`runs touching this file (${data.runs.length})`}>
        {data.runs.length === 0 && <Empty>None.</Empty>}
        <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
          {data.runs.map((r, i) => (
            <li key={i} style={{ padding: 6, marginBottom: 4, borderRadius: 4,
                                  background: '#ecfdf5', fontSize: 12 }}>
              <div><strong>{r.pipeline_type}</strong>
                <Pill status={r.status} />
              </div>
              <div style={{ color: '#475569' }}>
                {r.domain}/{r.dataset} · {r.business_date}
              </div>
              <Mono>{r.run_id}</Mono>
              {(r.record_count_source || r.record_count_target) && (
                <div style={{ color: '#475569' }}>
                  src={r.record_count_source ?? '—'} · tgt={r.record_count_target ?? '—'}
                </div>
              )}
            </li>
          ))}
        </ul>
      </Section>
    </>
  );
}

function WriteEventPanel({ linkId, fallback, onJumpToLink }) {
  const [data, setData] = useState(null);
  const [err, setErr]   = useState(null);
  useEffect(() => {
    setData(null); setErr(null);
    fetch(`/api/lineage/link/${linkId}/edges`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(setData)
      .catch(e => setErr(String(e)));
  }, [linkId]);
  if (err) return <KV obj={fallback || {}} />;
  if (!data) return <Empty>loading link edges…</Empty>;
  return (
    <>
      <Section title="lineage_link"><KV obj={fallback || {}} /></Section>
      <Section title={`lineage_edge rows (${data.edges.length})`}>
        {data.edges.length === 0 && <Empty>None.</Empty>}
        <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
          {data.edges.map((e, i) => (
            <li key={i} style={{ padding: 6, marginBottom: 4, borderRadius: 4,
                                  background: '#f5f3ff', fontSize: 12 }}>
              <div><strong>input_slot={e.input_slot || '—'}</strong>
                {e.upstream_status && <Pill status={e.upstream_status} />}
              </div>
              <div style={{ color: '#475569' }}>{e.edge_type} · {e.record_count ?? '—'} rows</div>
              {/* source_ref is what THIS write event actually read; show
                  it first. s3_raw_path is the ORIGINAL raw arrival the
                  bytes came from (joined via source_file_id) — only
                  surface when it differs from source_ref so it's clear
                  this is provenance, not the direct input. */}
              {e.source_ref && <Mono>read from: {e.source_ref}</Mono>}
              {e.s3_raw_path && e.s3_raw_path !== e.source_ref && (
                <Mono>originally raw: {e.s3_raw_path}</Mono>
              )}
              {e.upstream_run_id && <Mono>upstream_run: {e.upstream_run_id}</Mono>}
              {e.source_file_id && <Mono>file_id: {e.source_file_id}</Mono>}
            </li>
          ))}
        </ul>
      </Section>
    </>
  );
}

function TargetPanel({ ref_, onJumpToLink }) {
  const [data, setData] = useState(null);
  const [err, setErr]   = useState(null);
  useEffect(() => {
    setData(null); setErr(null);
    fetch(`/api/target?ref=${encodeURIComponent(ref_)}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(setData)
      .catch(e => setErr(String(e)));
  }, [ref_]);
  if (err)  return <Empty>error loading target: {err}</Empty>;
  if (!data) return <Empty>loading target…</Empty>;
  return (
    <Section title={`recent writes (${data.links.length})`}>
      {data.links.length === 0 && <Empty>None.</Empty>}
      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {data.links.map((l, i) => (
          <li key={i}
              onClick={() => onJumpToLink && onJumpToLink(l.lineage_link_id)}
              style={{
                padding: 6, marginBottom: 4, borderRadius: 4,
                background: '#fef9c3', cursor: 'pointer', fontSize: 12,
              }}>
            <div><strong>{l.pipeline_type || '?'}</strong>
              <Pill status={l.status} />
            </div>
            <div style={{ color: '#64748b' }}>
              {l.domain}/{l.dataset} · {l.business_date} ·
              {' '}{l.record_count ?? '—'} rows
            </div>
            <Mono>{l.lineage_link_id}</Mono>
            <div style={{ color: '#94a3b8', fontSize: 10 }}>{fmt(l.created_at)}</div>
          </li>
        ))}
      </ul>
    </Section>
  );
}

function RunPanel({ runId, onJumpToLink }) {
  const [data, setData] = useState(null);
  const [err, setErr]   = useState(null);

  useEffect(() => {
    setData(null); setErr(null);
    fetch(`/api/run/${runId}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(setData)
      .catch(e => setErr(String(e)));
  }, [runId]);

  if (err)  return <Empty>error loading run: {err}</Empty>;
  if (!data) return <Empty>loading run…</Empty>;

  const { run, stages, links } = data;

  const dom = run.domain, ds = run.dataset;
  return (
    <>
      <Section title={`Configs consumed by this ${run.pipeline_type || 'run'}`}>
        {dom && ds && (() => {
          const badges = configsForRun(run.pipeline_type, run);
          if (!badges.length) {
            return <Empty>No declarative config applies to this stage.</Empty>;
          }
          return badges.map(b => (
            <YamlBadge key={b.kind} kind={b.kind}
                       domain={dom} dataset={ds} label={b.label} />
          ));
        })()}
      </Section>

      <Section title="Run">
        <KV obj={run} skip={['orchestrators', 'runtime_context',
                              'dataset_schema_id','dataset_schema_version',
                              'dataset_transform_yaml_path','dataset_is_canonical']} />
      </Section>

      {run.orchestrators && run.orchestrators !== 'None' && (
        <Section title="Orchestrators">
          <Code>{pretty(run.orchestrators)}</Code>
        </Section>
      )}

      <Section title={`Stages (${stages.length})`}>
        {stages.length === 0 && <Empty>No stage events recorded.</Empty>}
        <ol style={{ paddingLeft: 18, margin: 0 }}>
          {stages.map((s, i) => (
            <li key={i} style={{ marginBottom: 6, fontSize: 12 }}>
              <div>
                <strong>{s.stage}</strong>
                <Pill status={s.status} />
                {s.event_type && <Tag>{s.event_type}</Tag>}
              </div>
              <div style={{ color: '#64748b' }}>
                {fmt(s.started_at)}
                {s.ended_at && <> → {fmt(s.ended_at)}</>}
              </div>
              {(s.record_count_in || s.record_count_out) && (
                <div style={{ color: '#475569' }}>
                  in={s.record_count_in ?? '—'} · out={s.record_count_out ?? '—'}
                </div>
              )}
              {s.input_ref && <Mono>in: {s.input_ref}</Mono>}
              {s.output_ref && <Mono>out: {s.output_ref}</Mono>}
              {s.error && <div style={{ color: '#b91c1c' }}>{s.error}</div>}
            </li>
          ))}
        </ol>
      </Section>

      <LinkSection
        title="Wrote (this run produced these write events)"
        emptyMsg="This run did not produce a lineage_link bundle."
        items={links.filter(l => l.role === 'consumer')}
        bg="#ecfdf5"
        onJumpToLink={onJumpToLink}
      />
      <LinkSection
        title="Read by (downstream steps used this run as input)"
        emptyMsg="No downstream step has consumed this run yet."
        items={links.filter(l => l.role === 'upstream')}
        bg="#eff6ff"
        onJumpToLink={onJumpToLink}
      />
    </>
  );
}

function LinkSection({ title, emptyMsg, items, bg, onJumpToLink }) {
  return (
    <Section title={`${title} (${items.length})`}>
      {items.length === 0 && <Empty>{emptyMsg}</Empty>}
      <ul style={{ listStyle: 'none', padding: 0, margin: 0 }}>
        {items.map((l, i) => (
          <li key={i}
              onClick={() => onJumpToLink && onJumpToLink(l.lineage_link_id)}
              style={{
                padding: 6, marginBottom: 4, borderRadius: 4,
                background: bg, cursor: 'pointer', fontSize: 12,
              }}>
            <div><strong>{l.edge_type}</strong></div>
            <Mono>{l.lineage_link_id}</Mono>
            <div style={{ color: '#64748b' }}>
              {l.target_ref || ''} · {l.record_count ?? '—'} rows
            </div>
          </li>
        ))}
      </ul>
    </Section>
  );
}


function Section({ title, children }) {
  return (
    <section style={{ marginBottom: 14 }}>
      <h4 style={{ fontSize: 12, margin: '6px 0', color: '#475569',
                   textTransform: 'uppercase', letterSpacing: 0.5 }}>{title}</h4>
      {children}
    </section>
  );
}

function KV({ obj, skip = [] }) {
  const skipSet = new Set(skip);
  const entries = Object.entries(obj || {})
    .filter(([k, v]) => v != null && v !== '' && !skipSet.has(k));
  if (!entries.length) return <Empty>No details.</Empty>;
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
      <tbody>
        {entries.map(([k, v]) => (
          <tr key={k} style={{ borderBottom: '1px solid #eef2f7' }}>
            <td style={{ padding: '4px 6px', color: '#64748b', verticalAlign: 'top', width: 130 }}>{k}</td>
            <td style={{ padding: '4px 6px', color: '#0f172a', wordBreak: 'break-all' }}>{String(v)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Pill({ status }) {
  const colors = {
    succeeded: ['#dcfce7', '#166534'],
    failed:    ['#fee2e2', '#991b1b'],
    running:   ['#dbeafe', '#1e40af'],
    partial:   ['#fef3c7', '#92400e'],
    skipped:   ['#e5e7eb', '#374151'],
    warned:    ['#fef3c7', '#92400e'],
  };
  const [bg, fg] = colors[status] || ['#e5e7eb', '#374151'];
  return (
    <span style={{
      display: 'inline-block', marginLeft: 6, padding: '0 6px',
      borderRadius: 999, fontSize: 10, background: bg, color: fg,
    }}>{status}</span>
  );
}

function Tag({ children }) {
  return (
    <span style={{
      display: 'inline-block', marginLeft: 4, padding: '0 5px',
      borderRadius: 4, fontSize: 10, background: '#f1f5f9', color: '#475569',
    }}>{children}</span>
  );
}

function Mono({ children }) {
  return (
    <div style={{
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
      fontSize: 11, color: '#334155', wordBreak: 'break-all',
    }}>{children}</div>
  );
}

function Code({ children }) {
  return (
    <pre style={{
      margin: 0, padding: 8, background: '#0f172a', color: '#e2e8f0',
      borderRadius: 4, fontSize: 11, overflow: 'auto',
    }}>{children}</pre>
  );
}

function Empty({ children }) {
  return <div style={{ fontSize: 12, color: '#94a3b8' }}>{children}</div>;
}

function fmt(ts) {
  if (!ts) return '';
  return String(ts).replace('T', ' ').replace(/\.\d+$/, '').slice(0, 19);
}

function pretty(s) {
  try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; }
}
