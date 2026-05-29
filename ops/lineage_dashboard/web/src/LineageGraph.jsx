import { useMemo } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType } from 'reactflow';

const KIND_STYLES = {
  raw_file:       { bg: '#fff7ed', border: '#f97316', icon: '📄' },
  curated_file:   { bg: '#fef3c7', border: '#d97706', icon: '🪙' },
  canonical_file: { bg: '#dbeafe', border: '#2563eb', icon: '🧊' },
  staging_table:  { bg: '#f1f5f9', border: '#64748b', icon: '🗃️' },
  upstream_run:   { bg: '#eff6ff', border: '#3b82f6', icon: '⚙️' },
  write_event:    { bg: '#f3e8ff', border: '#8b5cf6', icon: '🔗' },
  consumer_run:   { bg: '#ecfdf5', border: '#10b981', icon: '🏷️' },
  target:         { bg: '#fef9c3', border: '#ca8a04', icon: '🗄️' },
  target_db:      { bg: '#fef9c3', border: '#ca8a04', icon: '🗄️' },
};

const KIND_ORDER = [
  'raw_file', 'staging_table', 'curated_file', 'canonical_file',
  'upstream_run', 'write_event', 'consumer_run', 'target', 'target_db',
];

export default function LineageGraph({ trace, onNodeClick }) {
  const { nodes, edges } = useMemo(() => layout(trace), [trace]);
  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodeClick={onNodeClick}
      fitView
      proOptions={{ hideAttribution: true }}
      defaultEdgeOptions={{
        type: 'smoothstep',
        animated: false,
        markerEnd: { type: MarkerType.ArrowClosed, color: '#94a3b8' },
        style: { stroke: '#94a3b8', strokeWidth: 1.5 },
      }}
    >
      <Background gap={20} color="#e5e7eb" />
      <Controls />
      <MiniMap zoomable pannable
               nodeColor={n => KIND_STYLES[n.data?.kind]?.border || '#9ca3af'} />
    </ReactFlow>
  );
}

function layout(trace) {
  const colX = Object.fromEntries(KIND_ORDER.map((k, i) => [k, 80 + i * 280]));
  const buckets = Object.fromEntries(KIND_ORDER.map(k => [k, []]));
  for (const n of trace.nodes) {
    (buckets[n.kind] || (buckets[n.kind] = [])).push(n);
  }

  const nodes = [];
  for (const kind of KIND_ORDER) {
    const list = buckets[kind] || [];
    list.forEach((n, i) => {
      const s = KIND_STYLES[kind] || { bg: '#f3f4f6', border: '#9ca3af', icon: '·' };
      nodes.push({
        id: n.id,
        position: { x: colX[kind], y: 40 + i * 130 },
        data: { kind, label: n.label, raw: n },
        style: {
          background: s.bg,
          border: `2px solid ${s.border}`,
          borderRadius: 10,
          padding: 10,
          fontSize: 12,
          width: 240,
          whiteSpace: 'pre-wrap',
        },
        sourcePosition: 'right',
        targetPosition: 'left',
      });
      // Embed icon + kind tag in label
      nodes[nodes.length - 1].data.label = (
        `${s.icon} ${kind.replace('_', ' ')}\n${n.label || ''}`
      );
    });
  }

  const edges = trace.edges.map(e => ({
    id: e.id,
    source: e.source,
    target: e.target,
    label: edgeLabel(e),
    labelStyle: { fontSize: 10, fill: '#475569' },
    labelBgStyle: { fill: '#ffffff', opacity: 0.85 },
    labelBgPadding: [4, 2],
    labelBgBorderRadius: 4,
    data: e,
  }));
  return { nodes, edges };
}

function edgeLabel(e) {
  const parts = [e.kind];
  if (e.data?.input_slot) parts.push(`input_slot=${e.data.input_slot}`);
  if (e.data?.record_count != null) parts.push(`${e.data.record_count} rows`);
  return parts.join(' · ');
}
