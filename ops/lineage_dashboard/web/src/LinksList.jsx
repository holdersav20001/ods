export default function LinksList({ links, selectedId, onSelect }) {
  return (
    <ul style={{ listStyle: 'none', margin: 0, padding: 0 }}>
      {links.map(l => {
        const sel = l.lineage_link_id === selectedId;
        return (
          <li key={l.lineage_link_id}
              onClick={() => onSelect(l.lineage_link_id)}
              style={{
                padding: '10px 12px',
                borderBottom: '1px solid #f1f5f9',
                cursor: 'pointer',
                background: sel ? '#eef2ff' : 'transparent',
              }}>
            <div style={{ fontSize: 12, color: '#1e293b', fontWeight: 600 }}>
              {l.pipeline_type || '?'} · {l.domain || ''}/{l.dataset || ''}
            </div>
            <div style={{ fontSize: 11, color: '#64748b' }}>
              {l.edge_type} · {l.record_count ?? '—'} rows
            </div>
            <div style={{ fontSize: 10, color: '#94a3b8', marginTop: 4 }}>
              {l.business_date} · {fmt(l.created_at)}
            </div>
            <StatusPill status={l.status} />
          </li>
        );
      })}
    </ul>
  );
}

function StatusPill({ status }) {
  if (!status) return null;
  const colors = {
    succeeded: ['#dcfce7', '#166534'],
    failed:    ['#fee2e2', '#991b1b'],
    running:   ['#dbeafe', '#1e40af'],
    partial:   ['#fef3c7', '#92400e'],
  };
  const [bg, fg] = colors[status] || ['#e5e7eb', '#374151'];
  return (
    <span style={{
      display: 'inline-block', marginTop: 4, padding: '1px 6px',
      borderRadius: 999, fontSize: 10, background: bg, color: fg,
    }}>{status}</span>
  );
}

function fmt(ts) {
  if (!ts) return '';
  return ts.replace('T', ' ').replace(/\.\d+$/, '').slice(0, 19);
}
