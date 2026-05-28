import { useEffect, useState, useMemo, useCallback } from 'react';
import LineageGraph from './LineageGraph.jsx';
import LinksList from './LinksList.jsx';
import NodeDetails from './NodeDetails.jsx';

export default function App() {
  const [links, setLinks] = useState([]);
  const [selectedLinkId, setSelectedLinkId] = useState(null);
  const [trace, setTrace] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [error, setError] = useState(null);

  // Initial list
  useEffect(() => {
    fetch('/api/lineage/links?limit=100')
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(rows => {
        setLinks(rows);
        if (rows.length && !selectedLinkId) setSelectedLinkId(rows[0].lineage_link_id);
      })
      .catch(e => setError(String(e)));
  }, []);

  // Trace on selection
  useEffect(() => {
    if (!selectedLinkId) { setTrace(null); return; }
    setSelectedNode(null);
    fetch(`/api/lineage/trace/${selectedLinkId}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(setTrace)
      .catch(e => setError(String(e)));
  }, [selectedLinkId]);

  const onNodeClick = useCallback((_e, n) => setSelectedNode(n), []);

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr 360px', height: '100vh' }}>
      <aside style={{ borderRight: '1px solid #e5e7eb', overflowY: 'auto' }}>
        <header style={{ padding: 12, borderBottom: '1px solid #e5e7eb', background: '#f9fafb' }}>
          <h2 style={{ margin: 0, fontSize: 16 }}>ODS Lineage</h2>
          <div style={{ fontSize: 12, color: '#6b7280' }}>
            {links.length} recent write events
          </div>
        </header>
        <LinksList links={links} selectedId={selectedLinkId} onSelect={setSelectedLinkId} />
      </aside>

      <main style={{ position: 'relative' }}>
        {error && <Banner kind="error">{error}</Banner>}
        {trace && <LineageGraph trace={trace} onNodeClick={onNodeClick} />}
        {!trace && !error && <Empty>Select a write event on the left</Empty>}
      </main>

      <aside style={{ borderLeft: '1px solid #e5e7eb', overflowY: 'auto', background: '#fafafa' }}>
        <NodeDetails node={selectedNode} link={trace?.lineage_link} />
      </aside>
    </div>
  );
}

function Banner({ kind, children }) {
  const bg = kind === 'error' ? '#fef2f2' : '#eff6ff';
  const fg = kind === 'error' ? '#991b1b' : '#1e40af';
  return (
    <div style={{
      position: 'absolute', top: 8, left: 8, right: 8, zIndex: 10,
      padding: '8px 12px', background: bg, color: fg, borderRadius: 6,
      fontSize: 13,
    }}>{children}</div>
  );
}

function Empty({ children }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      height: '100%', color: '#9ca3af', fontSize: 14,
    }}>{children}</div>
  );
}
