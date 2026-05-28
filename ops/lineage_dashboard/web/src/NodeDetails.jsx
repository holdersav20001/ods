export default function NodeDetails({ node, link }) {
  if (!node) {
    return (
      <div style={{ padding: 16 }}>
        <h3 style={{ marginTop: 0, fontSize: 14 }}>Write event</h3>
        {link ? <KV obj={link} /> : <Empty>Click a node to inspect.</Empty>}
      </div>
    );
  }
  const raw = node.data?.raw || {};
  return (
    <div style={{ padding: 16 }}>
      <h3 style={{ marginTop: 0, fontSize: 14 }}>
        {raw.kind ? raw.kind.replace('_', ' ') : 'node'}
      </h3>
      <div style={{ fontSize: 12, color: '#1f2937', wordBreak: 'break-all', marginBottom: 12 }}>
        {raw.label}
      </div>
      <KV obj={raw.data || {}} />
    </div>
  );
}

function KV({ obj }) {
  const entries = Object.entries(obj || {}).filter(([, v]) => v != null && v !== '');
  if (!entries.length) return <Empty>No details.</Empty>;
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
      <tbody>
        {entries.map(([k, v]) => (
          <tr key={k} style={{ borderBottom: '1px solid #eef2f7' }}>
            <td style={{ padding: '4px 6px', color: '#64748b', verticalAlign: 'top', width: 140 }}>{k}</td>
            <td style={{ padding: '4px 6px', color: '#0f172a', wordBreak: 'break-all' }}>
              {String(v)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Empty({ children }) {
  return <div style={{ fontSize: 12, color: '#94a3b8' }}>{children}</div>;
}
