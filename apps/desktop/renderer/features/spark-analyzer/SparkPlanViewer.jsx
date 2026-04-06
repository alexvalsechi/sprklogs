/**
 * SparkPlanViewer.jsx
 * ─────────────────────────────────────────────────────────────────
 * Visualises the Spark SQL physical plan tree from sparkPlanInfo.
 * Rendered on Canvas2D (no SVG/DOM per node) for performance with
 * 500+ node plans.
 *
 * Entry points exposed on window:
 *   window.SparkPlanViewer.mount(executions, domNode)   – initial mount
 *   window.SparkPlanViewer.unmount(domNode)             – cleanup
 */

/* ── Imports (resolved by esbuild at build time) ────────────────── */
import React, { useCallback, useEffect, useRef, useState } from 'react';
import ReactDOM from 'react-dom/client';

// ─── Layout constants ────────────────────────────────────────────
const NODE_W = 200;
const NODE_H = 80;
const HGAP   = 20;
const VGAP   = 52;

// ─── Node type classification with cache ────────────────────────────
const NODE_TYPES = [
  { test: /^(Scan|.*TableScan)/i,               type: 'Scan',      color: '#22c55e' },
  { test: /Join/i,                               type: 'Join',      color: '#3b82f6' },
  { test: /^(BroadcastExchange|Exchange)/i,      type: 'Shuffle',   color: '#f59e0b' },
  { test: /Aggregate/i,                          type: 'Aggregate', color: '#06b6d4' },
  { test: /^Window$/i,                           type: 'Window',    color: '#ec4899' },
  { test: /^(Insert|.*Write|.*Command)/i,        type: 'Write',     color: '#ef4444' },
  { test: /^(InMemoryRelation|ReusedExchange)/i, type: 'Cache',     color: '#a855f7' },
];
const DEFAULT_TYPE  = { type: 'Op', color: '#64748b' };

// Cache for classifyNode results
const classifyCache = new Map();

function classifyNode(nodeName = '') {
  if (classifyCache.has(nodeName)) {
    return classifyCache.get(nodeName);
  }
  for (const nt of NODE_TYPES) {
    if (nt.test.test(nodeName)) {
      classifyCache.set(nodeName, nt);
      return nt;
    }
  }
  classifyCache.set(nodeName, DEFAULT_TYPE);
  return DEFAULT_TYPE;
}

// ─── Tree preprocessing ──────────────────────────────────────────

// Nodes to replace with their single child (only true wrappers with no semantics)
const TRANSPARENT = new Set(['InputAdapter', 'AdaptiveSparkPlan']);
function isTransparent(name = '') {
  return TRANSPARENT.has(name);
}

// Consecutive same-type chains: keep only deepest (closest to leaf)
// Kept minimal — only truly redundant duplicates
const COLLAPSIBLE = new Set(['ColumnarToRow', 'RowToColumnar']);

// Subtrees to prune entirely (internal bloom/subquery helpers)
const PRUNED = new Set(['GenerateBloomFilter', 'SubqueryBroadcast']);

const MAX_DEPTH   = 30;
const MAX_RECURSE = 1200;

function preprocessTree(node, depth = 0, callCount = { n: 0 }) {
  if (!node || typeof node !== 'object') return null;
  if (++callCount.n > MAX_RECURSE) return { nodeName: '… (truncado)', simpleString: '', metrics: [], children: [] };

  const name     = node.nodeName || '';
  const children = Array.isArray(node.children) ? node.children : [];

  // Step 3 — prune entire subtree
  if (PRUNED.has(name)) return null;

  // Step 4 — depth truncation
  if (depth >= MAX_DEPTH) {
    return { nodeName: '… (truncado)', simpleString: '', metrics: [], children: [] };
  }

  // Recurse into children first (post-order)
  const processedChildren = children
    .map(c => preprocessTree(c, depth + 1, callCount))
    .filter(Boolean);

  // Step 1 — remove transparent nodes (replace with only child)
  if (isTransparent(name) && processedChildren.length === 1) {
    return processedChildren[0];
  }

  // Build current node with processed children
  const cur = {
    nodeName:     name,
    simpleString: node.simpleString || '',
    metrics:      Array.isArray(node.metrics) ? node.metrics : [],
    children:     processedChildren,
  };

  // Step 2 — collapse consecutive same-type chains when parent has exactly 1 child
  if (
    COLLAPSIBLE.has(name) &&
    cur.children.length === 1 &&
    cur.children[0].nodeName === name
  ) {
    // Keep the child (lower in the chain = closer to leaf) and discard current
    return cur.children[0];
  }

  return cur;
}

// ─── Layout: simplified Reingold-Tilford ────────────────────────

function computeSubtreeWidth(node) {
  if (!node) return NODE_W;
  // Return cached value if available
  if (node._subtreeWidth !== undefined) return node._subtreeWidth;

  const kids = node.children || [];
  if (kids.length === 0) {
    node._subtreeWidth = NODE_W;
    return NODE_W;
  }
  const childSum = kids.reduce((s, c) => s + computeSubtreeWidth(c), 0);
  const spacing  = HGAP * (kids.length - 1);
  const result = Math.max(NODE_W, childSum + spacing);
  node._subtreeWidth = result;
  return result;
}

function assignPositions(node, x, depth, out) {
  if (!node) return;
  const sw   = computeSubtreeWidth(node);
  const cx   = x + sw / 2;
  const cy   = depth * (NODE_H + VGAP);
  const id   = out.length;
  node._id   = id;
  node._x    = cx;
  node._y    = cy;
  node._sw   = sw;
  out.push(node);

  const kids = node.children || [];
  if (kids.length === 0) return;
  const totalW = kids.reduce((s, c) => s + computeSubtreeWidth(c), 0)
               + HGAP * (kids.length - 1);
  let childX = cx - totalW / 2;
  for (const kid of kids) {
    const ksw = computeSubtreeWidth(kid);
    assignPositions(kid, childX, depth + 1, out);
    childX += ksw + HGAP;
  }
}

function buildLayout(root) {
  const nodes = [];
  if (!root) return { nodes, treeW: 0, treeH: 0, minX: 0, minY: 0 };
  assignPositions(root, 0, 0, nodes);
  const xs = nodes.map(n => n._x);
  const ys = nodes.map(n => n._y);
  const minX = Math.min(...xs) - NODE_W / 2;
  const minY = Math.min(...ys) - NODE_H / 2;
  const maxX = Math.max(...xs) + NODE_W / 2;
  const maxY = Math.max(...ys) + NODE_H / 2;
  return { nodes, treeW: maxX - minX, treeH: maxY - minY, minX, minY };
}

// ─── Stats helpers ───────────────────────────────────────────────

function collectStats(nodes) {
  let maxDepth = 0;
  const byType = {};
  for (const n of nodes) {
    const depth = Math.round(n._y / (NODE_H + VGAP));
    if (depth > maxDepth) maxDepth = depth;
    const { type } = classifyNode(n.nodeName);
    byType[type] = (byType[type] || 0) + 1;
  }
  return { total: nodes.length, maxDepth, byType };
}

// ─── Canvas renderer ─────────────────────────────────────────────

const BG         = '#0f0f0f';
const EDGE_COLOR = 'rgba(255,255,255,0.10)';
const NODE_FILL  = '#1a1a1a';
const NODE_BORDER = 'rgba(255,255,255,0.08)';
const TEXT_NAME  = '#e2e8f0';
const TEXT_DIM   = 'rgba(255,255,255,0.42)';
const SEL_BORDER = '#e35914';

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max - 1) + '…' : str;
}

function drawNode(ctx, node, selected, scale) {
  const { _x: cx, _y: cy } = node;
  const x = cx - NODE_W / 2;
  const y = cy - NODE_H / 2;
  const r = 6;
  const { color, type } = classifyNode(node.nodeName);
  const isStage = node.nodeName.startsWith('WholeStageCodegen');

  // Shadow for selected
  if (selected) {
    ctx.shadowColor = SEL_BORDER;
    ctx.shadowBlur  = 14;
  }

  // Background
  ctx.fillStyle   = isStage ? color + '18' : NODE_FILL;
  ctx.strokeStyle = selected ? SEL_BORDER : (isStage ? color + '70' : NODE_BORDER);
  ctx.lineWidth   = selected ? 2 / scale : (isStage ? 1.5 / scale : 1 / scale);
  ctx.beginPath();
  ctx.roundRect(x, y, NODE_W, NODE_H, r);
  ctx.fill();
  ctx.stroke();
  ctx.shadowBlur = 0;

  // Left color bar
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.roundRect(x, y + r, 4, NODE_H - r * 2, 2);
  ctx.fill();

  // Type badge
  const badgeX = x + 12;
  const badgeY = y + 8;
  ctx.fillStyle   = color + '30';
  ctx.strokeStyle = color + '70';
  ctx.lineWidth   = 0.8 / scale;
  ctx.beginPath();
  ctx.roundRect(badgeX, badgeY, 60, 15, 3);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle     = color;
  ctx.font          = 'bold 9px "DM Sans",sans-serif';
  ctx.textAlign     = 'left';
  ctx.textBaseline  = 'middle';
  ctx.fillText(truncate(type, 9), badgeX + 5, badgeY + 7.5);

  // Node name
  ctx.fillStyle    = TEXT_NAME;
  ctx.font         = '600 12px "DM Sans",sans-serif';
  ctx.textAlign    = 'left';
  ctx.textBaseline = 'top';
  ctx.fillText(truncate(node.nodeName, 26), x + 12, y + 28);

  // simpleString snippet (first 200 chars stripped to 1 line)
  const ss = (node.simpleString || '').replace(/\n/g, ' ').trim();
  if (ss) {
    ctx.fillStyle    = TEXT_DIM;
    ctx.font         = '10px "Inconsolata",monospace';
    ctx.textAlign    = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText(truncate(ss, 34), x + 12, y + 46);
  }

  // Metrics count badge (bottom-right)
  const mCount = (node.metrics || []).length;
  if (mCount > 0) {
    ctx.fillStyle    = TEXT_DIM;
    ctx.font         = '9px "Inconsolata",monospace';
    ctx.textAlign    = 'right';
    ctx.textBaseline = 'bottom';
    ctx.fillText(`${mCount}m`, x + NODE_W - 8, y + NODE_H - 6);
  }
}

function drawEdge(ctx, parent, child) {
  const px = parent._x;
  const py = parent._y + NODE_H / 2;
  const cx = child._x;
  const cy = child._y - NODE_H / 2;
  const midY = (py + cy) / 2;

  ctx.strokeStyle = EDGE_COLOR;
  ctx.lineWidth   = 1.5;
  ctx.beginPath();
  ctx.moveTo(px, py);
  ctx.bezierCurveTo(px, midY, cx, midY, cx, cy);
  ctx.stroke();

  // Arrow head
  const ah = 6;
  ctx.fillStyle = EDGE_COLOR;
  ctx.beginPath();
  ctx.moveTo(cx, cy);
  ctx.lineTo(cx - ah / 2, cy - ah);
  ctx.lineTo(cx + ah / 2, cy - ah);
  ctx.closePath();
  ctx.fill();
}

function drawEdges(ctx, node) {
  for (const child of (node.children || [])) {
    drawEdge(ctx, node, child);
    drawEdges(ctx, child);
  }
}

// Hit-test: returns node whose bounding box contains (wx, wy) in world coords
function hitTest(nodes, wx, wy) {
  // Iterate in reverse to hit top-drawn nodes first
  for (let i = nodes.length - 1; i >= 0; i--) {
    const n = nodes[i];
    if (
      wx >= n._x - NODE_W / 2 &&
      wx <= n._x + NODE_W / 2 &&
      wy >= n._y - NODE_H / 2 &&
      wy <= n._y + NODE_H / 2
    ) return n;
  }
  return null;
}

// ─── Stage group rendering (WholeStageCodegen bounding boxes) ───

function collectDescendants(node, out = []) {
  for (const child of (node.children || [])) {
    out.push(child);
    collectDescendants(child, out);
  }
  return out;
}

function drawStageGroups(ctx, rootNode) {
  if (!rootNode) return;
  function visit(node) {
    if (!node) return;
    if (node.nodeName && node.nodeName.startsWith('WholeStageCodegen')) {
      const desc = collectDescendants(node);
      const all  = [node, ...desc];
      const pad  = 14;
      const minX = Math.min(...all.map(n => n._x - NODE_W / 2)) - pad;
      const minY = node._y - NODE_H / 2 - pad;
      const maxX = Math.max(...all.map(n => n._x + NODE_W / 2)) + pad;
      const maxY = Math.max(...all.map(n => n._y + NODE_H / 2)) + pad;
      const { color } = classifyNode(node.nodeName);
      // Filled background
      ctx.fillStyle   = color + '0d';
      ctx.strokeStyle = color + '40';
      ctx.lineWidth   = 1;
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.roundRect(minX, minY, maxX - minX, maxY - minY, 8);
      ctx.fill();
      ctx.stroke();
      ctx.setLineDash([]);
    }
    for (const child of (node.children || [])) visit(child);
  }
  visit(rootNode);
}

// ─── Legend data ─────────────────────────────────────────────────
const LEGEND_ITEMS = [
  ...NODE_TYPES.map(nt => ({ label: nt.type, color: nt.color })),
  { label: DEFAULT_TYPE.type, color: DEFAULT_TYPE.color },
];

// ─── React components ────────────────────────────────────────────

const NodeDetailPanel = React.memo(function NodeDetailPanel({ node, onClose }) {
  if (!node) return null;
  const { color } = classifyNode(node.nodeName);
  return (
    <div style={{
      position: 'absolute', right: 0, top: 0, bottom: 0, width: 320,
      background: '#0f0f0f', borderLeft: '1px solid rgba(255,255,255,0.08)',
      display: 'flex', flexDirection: 'column', zIndex: 10,
      fontSize: 13, color: '#e2e8f0', fontFamily: '"DM Sans", sans-serif',
    }}>
      {/* Header */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 14px', borderBottom: '1px solid rgba(255,255,255,0.08)',
        background: '#000000',
      }}>
        <span style={{
          fontFamily: '"Inconsolata",monospace', fontSize: 11,
          color: color, fontWeight: 700, letterSpacing: '0.06em',
          textTransform: 'uppercase',
        }}>
          {classifyNode(node.nodeName).type}
        </span>
        <button
          onClick={onClose}
          style={{
            background: 'none', border: '1px solid rgba(255,255,255,0.15)',
            borderRadius: 4, color: '#aaa', cursor: 'pointer', padding: '2px 8px',
            fontSize: 11, fontFamily: 'inherit',
          }}
        >✕</button>
      </div>

      {/* Content (scrollable) */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px' }}>
        {/* Node name */}
        <div style={{ marginBottom: 10 }}>
          <div style={{
            fontSize: 10, color: 'rgba(255,255,255,0.4)', fontFamily: '"Inconsolata",monospace',
            textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4,
          }}>Node</div>
          <div style={{
            fontSize: 13, fontWeight: 600, color: '#fff',
            wordBreak: 'break-word',
          }}>{node.nodeName}</div>
        </div>

        {/* simpleString */}
        {node.simpleString && (
          <div style={{ marginBottom: 12 }}>
            <div style={{
              fontSize: 10, color: 'rgba(255,255,255,0.4)', fontFamily: '"Inconsolata",monospace',
              textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4,
            }}>Simple String</div>
            <pre style={{
              background: '#0a0a0a', border: '1px solid rgba(255,255,255,0.08)',
              borderRadius: 4, padding: '8px 10px', margin: 0,
              fontFamily: '"Inconsolata",monospace', fontSize: 11,
              color: '#cbd5e1', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
              maxHeight: 180, overflowY: 'auto',
            }}>{node.simpleString}</pre>
          </div>
        )}

        {/* Metrics */}
        {node.metrics && node.metrics.length > 0 && (
          <div>
            <div style={{
              fontSize: 10, color: 'rgba(255,255,255,0.4)', fontFamily: '"Inconsolata",monospace',
              textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 6,
            }}>Metrics ({node.metrics.length})</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              {node.metrics.map((m, i) => (
                <div key={i} style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                  background: 'rgba(255,255,255,0.03)', borderRadius: 3,
                  padding: '4px 8px', gap: 8,
                }}>
                  <span style={{
                    color: '#94a3b8', fontFamily: '"Inconsolata",monospace', fontSize: 11,
                    flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  }} title={m.name || String(m)}>{m.name || String(m)}</span>
                  {m.value !== undefined && (
                    <span style={{
                      color: '#e2e8f0', fontFamily: '"Inconsolata",monospace', fontSize: 11,
                      flexShrink: 0,
                    }}>{String(m.value)}</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
});

const Legend = React.memo(function Legend() {
  return (
    <div style={{
      position: 'absolute', bottom: 12, left: 12, zIndex: 5,
      background: 'rgba(0,0,0,0.88)', border: '1px solid rgba(255,255,255,0.10)',
      borderRadius: 6, padding: '8px 10px',
      display: 'flex', flexDirection: 'column', gap: 4,
    }}>
      {LEGEND_ITEMS.map(({ label, color }) => (
        <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <div style={{ width: 10, height: 10, borderRadius: 2, background: color, flexShrink: 0 }} />
          <span style={{
            fontFamily: '"Inconsolata",monospace', fontSize: 10,
            color: 'rgba(255,255,255,0.65)', letterSpacing: '0.04em',
          }}>{label}</span>
        </div>
      ))}
    </div>
  );
});

const StatsBar = React.memo(function StatsBar({ stats, style }) {
  if (!stats) return null;
  const typesSorted = Object.entries(stats.byType).sort((a, b) => b[1] - a[1]);
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 14, flexWrap: 'wrap',
      fontFamily: '"Inconsolata",monospace', fontSize: 11,
      color: 'rgba(255,255,255,0.55)',
      ...style,
    }}>
      <span><b style={{ color: '#e2e8f0' }}>{stats.total}</b> nodes</span>
      <span>depth <b style={{ color: '#e2e8f0' }}>{stats.maxDepth}</b></span>
      {typesSorted.map(([type, count]) => {
        const nt = NODE_TYPES.find(x => x.type === type) || DEFAULT_TYPE;
        return (
          <span key={type}>
            <span style={{ color: nt.color, fontWeight: 700 }}>{count}</span>
            {' '}{type}
          </span>
        );
      })}
    </div>
  );
});

// ─── Main Canvas component ───────────────────────────────────────

function PlanCanvas({ layout, selectedNode, onSelectNode }) {
  const canvasRef = useRef(null);
  const stateRef  = useRef({ tx: 0, ty: 0, scale: 1, dragging: false, lastX: 0, lastY: 0 });
  const rafRef    = useRef(null);

  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const { tx, ty, scale } = stateRef.current;
    const { nodes } = layout;

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = BG;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    ctx.save();
    ctx.translate(tx, ty);
    ctx.scale(scale, scale);

    // Compute visible world bounds for culling
    const invScale = 1 / scale;
    const vx1 = -tx * invScale;
    const vy1 = -ty * invScale;
    const vx2 = vx1 + canvas.width  * invScale;
    const vy2 = vy1 + canvas.height * invScale;

    // Draw stage group backgrounds (behind everything)
    if (layout.root) {
      ctx.save();
      drawStageGroups(ctx, layout.root);
      ctx.restore();
    }

    // Draw edges first (they're behind nodes)
    if (layout.root) {
      ctx.save();
      drawEdges(ctx, layout.root);
      ctx.restore();
    }

    // Draw nodes (culled)
    for (const node of nodes) {
      const nx = node._x - NODE_W / 2;
      const ny = node._y - NODE_H / 2;
      if (nx > vx2 || nx + NODE_W < vx1 || ny > vy2 || ny + NODE_H < vy1) continue;
      drawNode(ctx, node, node === selectedNode, scale);
    }

    ctx.restore();
  }, [layout, selectedNode]);

  // Helper: fit the current layout to the canvas dimensions
  const fitLayout = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas || layout.nodes.length === 0) return;
    const { treeW, treeH, minX, minY } = layout;
    const cw = canvas.width;
    const ch = canvas.height;
    if (cw === 0 || ch === 0) return;
    const PAD = 40;
    const k   = Math.min(1.0, cw / (treeW + PAD), ch / (treeH + PAD));
    stateRef.current.scale = k;
    stateRef.current.tx    = (cw - treeW * k) / 2 - minX * k;
    stateRef.current.ty    = PAD / 2 - minY * k;
    draw();
  }, [layout, draw]);

  // Auto-fit whenever the layout changes
  useEffect(() => { fitLayout(); }, [fitLayout]);

  // Resize observer — update canvas resolution and re-fit
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ro = new ResizeObserver(entries => {
      for (const e of entries) {
        canvas.width  = e.contentRect.width;
        canvas.height = e.contentRect.height;
        fitLayout();
      }
    });
    ro.observe(canvas.parentElement || canvas);
    return () => ro.disconnect();
  }, [fitLayout]);

  // Re-draw whenever dependencies change
  useEffect(() => { draw(); }, [draw]);

  // Non-passive wheel listener — must be attached via addEventListener to allow
  // preventDefault() to actually stop page scroll (React synthetic onWheel is passive)
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const handler = (e) => {
      e.preventDefault();
      e.stopPropagation();
      const rect  = canvas.getBoundingClientRect();
      const mx    = e.clientX - rect.left;
      const my    = e.clientY - rect.top;
      const { tx, ty, scale } = stateRef.current;
      const delta = -e.deltaY * 0.0008;
      const newScale = Math.max(0.03, Math.min(4, scale * (1 + delta)));
      const ratio    = newScale / scale;
      stateRef.current.scale = newScale;
      stateRef.current.tx    = mx - (mx - tx) * ratio;
      stateRef.current.ty    = my - (my - ty) * ratio;
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      rafRef.current = requestAnimationFrame(draw);
    };
    canvas.addEventListener('wheel', handler, { passive: false });
    return () => canvas.removeEventListener('wheel', handler);
  }, [draw]);

  /* ── Event handlers ── */
  const schedDraw = () => {
    if (rafRef.current) cancelAnimationFrame(rafRef.current);
    rafRef.current = requestAnimationFrame(draw);
  };

  const onMouseDown = (e) => {
    if (e.button !== 0) return;
    stateRef.current.dragging = true;
    stateRef.current.lastX    = e.clientX;
    stateRef.current.lastY    = e.clientY;
  };

  const onMouseMove = (e) => {
    if (!stateRef.current.dragging) return;
    const dx = e.clientX - stateRef.current.lastX;
    const dy = e.clientY - stateRef.current.lastY;
    stateRef.current.lastX = e.clientX;
    stateRef.current.lastY = e.clientY;
    stateRef.current.tx   += dx;
    stateRef.current.ty   += dy;
    schedDraw();
  };

  const onMouseUp = (e) => {
    stateRef.current.dragging = false;
  };

  const onClick = (e) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect  = canvas.getBoundingClientRect();
    const mx    = e.clientX - rect.left;
    const my    = e.clientY - rect.top;
    const { tx, ty, scale } = stateRef.current;
    const wx = (mx - tx) / scale;
    const wy = (my - ty) / scale;
    const hit = hitTest(layout.nodes, wx, wy);
    onSelectNode(hit || null);
  };

  return (
    <canvas
      ref={canvasRef}
      style={{ width: '100%', height: '100%', display: 'block', cursor: 'grab' }}
      onMouseDown={onMouseDown}
      onMouseMove={onMouseMove}
      onMouseUp={onMouseUp}
      onMouseLeave={onMouseUp}
      onClick={onClick}
    />
  );
}

// ─── Top-level app ───────────────────────────────────────────────

function countNodes(node, count = { n: 0 }) {
  if (!node) return count.n;
  count.n++;
  for (const c of (node.children || [])) countNodes(c, count);
  return count.n;
}

// ─── Pipeline classification helpers ────────────────────────────

function classifyExecution(ex) {
  const root = ex.sparkPlanInfo?.nodeName || '';
  const nc   = countNodes(ex.sparkPlanInfo);
  if (/Insert|Write/i.test(root) && !/Create/i.test(root)) return 'write';
  if (/CreateView|CreateTable|Command/i.test(root))         return 'ddl';
  if (nc > 50)                                               return 'compute';
  return 'init';
}

const STAGE_META = {
  init:    { label: 'Leitura / Contagem', color: '#22c55e',  order: 0 },
  ddl:     { label: 'Definição de Views', color: '#a855f7',  order: 1 },
  compute: { label: 'Processamento',      color: '#3b82f6',  order: 2 },
  write:   { label: 'Escrita de Saída',   color: '#ef4444',  order: 3 },
};

function collectUniqueSources(executions) {
  const seen = new Set();
  function visit(node) {
    if (!node) return;
    if (/^Scan/i.test(node.nodeName || '')) {
      const ss = node.simpleString || '';
      // extract last path segment or first bracket group
      const m = ss.match(/\[([^\]]+)\]/) || ss.match(/([^\s/\\]+)$/);
      const label = (m ? m[1] : ss).trim().slice(0, 50);
      if (label) seen.add(label);
    }
    for (const c of (node.children || [])) visit(c);
  }
  for (const ex of executions) visit(ex.sparkPlanInfo);
  return [...seen];
}

// ─── Unified Pipeline View ───────────────────────────────────────

function PipelineView({ executions, onOpen }) {
  if (!executions || !Array.isArray(executions) || executions.length === 0) {
    return (
      <div style={{
        background: BG, width: '100%', height: '100%',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontFamily: '"DM Sans", sans-serif', color: 'rgba(255,255,255,0.4)',
      }}>
        <span style={{ fontFamily: '"Inconsolata",monospace', fontSize: 12 }}>
          Nenhuma execução encontrada
        </span>
      </div>
    );
  }

  // Group executions by stage type
  const groups = { init: [], ddl: [], compute: [], write: [] };
  for (const ex of executions) {
    const execType = classifyExecution(ex);
    if (groups[execType]) groups[execType].push(ex);
  }

  const sources = collectUniqueSources(executions);

  // Build ordered pipeline stages
  const stages = [
    {
      key: 'source',
      label: 'Origens de Dados',
      color: '#22c55e',
      items: sources.map(s => ({ label: s, isSrc: true })),
    },
    ...Object.entries(STAGE_META)
      .sort((a, b) => a[1].order - b[1].order)
      .map(([key, meta]) => ({
        key,
        label: meta.label,
        color: meta.color,
        items: groups[key],
      })),
  ].filter(s => s.items.length > 0);

  const CARD_W = 200;
  const btnStyle = {
    background: 'none', border: '1px solid rgba(255,255,255,0.10)',
    borderRadius: 4, color: 'rgba(255,255,255,0.5)', cursor: 'pointer',
    padding: '1px 8px', fontFamily: '"Inconsolata",monospace', fontSize: 9,
    textTransform: 'uppercase', letterSpacing: '0.05em',
  };

  return (
    <div style={{
      background: BG, width: '100%', height: '100%',
      display: 'flex', flexDirection: 'column',
      fontFamily: '"DM Sans", sans-serif',
    }}>
      {/* Header */}
      <div style={{
        padding: '10px 16px', borderBottom: '1px solid rgba(255,255,255,0.07)',
        background: '#000000', flexShrink: 0,
        display: 'flex', alignItems: 'center', gap: 20, flexWrap: 'wrap',
      }}>
        <span style={{
          fontFamily: '"Inconsolata",monospace', fontSize: 10,
          color: 'rgba(255,255,255,0.45)', textTransform: 'uppercase', letterSpacing: '0.1em',
        }}>Fluxo Unificado</span>
        <span style={{ fontFamily: '"Inconsolata",monospace', fontSize: 11, color: 'rgba(255,255,255,0.4)' }}>
          <b style={{ color: '#e2e8f0' }}>{executions.length}</b> execuções
          {' · '}
          <b style={{ color: '#e2e8f0' }}>{sources.length}</b> origens
        </span>
      </div>

      {/* Pipeline lanes */}
      <div style={{ flex: 1, overflowY: 'auto', overflowX: 'auto', padding: '20px 16px' }}>
        <div style={{ display: 'flex', alignItems: 'flex-start', gap: 0, minWidth: stages.length * 240 }}>
          {stages.map((stage, si) => (
            <React.Fragment key={stage.key}>
              {/* Stage column */}
              <div style={{ width: CARD_W + 16, flexShrink: 0 }}>
                {/* Stage header */}
                <div style={{
                  marginBottom: 8, paddingBottom: 6,
                  borderBottom: `2px solid ${stage.color}50`,
                  display: 'flex', alignItems: 'center', gap: 6,
                }}>
                  <div style={{ width: 8, height: 8, borderRadius: '50%', background: stage.color, flexShrink: 0 }} />
                  <span style={{
                    fontFamily: '"Inconsolata",monospace', fontSize: 10,
                    color: stage.color, textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 700,
                  }}>{stage.label}</span>
                  <span style={{
                    fontFamily: '"Inconsolata",monospace', fontSize: 10,
                    color: 'rgba(255,255,255,0.3)', marginLeft: 'auto',
                  }}>{stage.items.length}</span>
                </div>

                {/* Cards */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {stage.items.slice(0, 15).map((item, idx) => {
                    if (item.isSrc) {
                      // Source label card
                      return (
                        <div key={idx} style={{
                          background: '#161616', border: `1px solid ${stage.color}30`,
                          borderLeft: `3px solid ${stage.color}`,
                          borderRadius: 5, padding: '7px 10px',
                        }}>
                          <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.75)', wordBreak: 'break-all' }}>
                            {item.label}
                          </div>
                        </div>
                      );
                    }
                    // Execution card
                    const nc  = countNodes(item.sparkPlanInfo);
                    const { color } = STAGE_META[classifyExecution(item)] || { color: stage.color };
                    const nodeName = String(item.sparkPlanInfo?.nodeName || '—');
                    const desc = item.description ? String(item.description).slice(0, 40) : null;
                    return (
                      <div key={item.executionId} style={{
                        background: '#161616', border: `1px solid ${color}25`,
                        borderLeft: `3px solid ${color}`,
                        borderRadius: 5, padding: '7px 10px', cursor: 'pointer',
                      }}
                        onClick={() => onOpen(item.executionId)}
                      >
                        <div style={{
                          fontFamily: '"Inconsolata",monospace', fontSize: 9,
                          color: color, marginBottom: 3, letterSpacing: '0.06em',
                        }}>#{item.executionId}</div>
                        <div style={{
                          fontSize: 11, color: 'rgba(255,255,255,0.65)', lineHeight: 1.35,
                          marginBottom: 4, wordBreak: 'break-word',
                          display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden',
                        }}>
                          {nodeName}
                        </div>
                        {desc && (
                          <div style={{
                            fontSize: 10, color: 'rgba(255,255,255,0.35)', fontFamily: '"Inconsolata",monospace',
                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                          }}>{desc}</div>
                        )}
                        <div style={{
                          marginTop: 5, display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                        }}>
                          <span style={{ fontFamily: '"Inconsolata",monospace', fontSize: 9, color: 'rgba(255,255,255,0.25)' }}>
                            {nc} nós
                          </span>
                          <button style={btnStyle} onClick={e => { e.stopPropagation(); onOpen(item.executionId); }}>
                            ver plano →
                          </button>
                        </div>
                      </div>
                    );
                  })}
                  {stage.items.length > 15 && (
                    <div style={{
                      textAlign: 'center', fontFamily: '"Inconsolata",monospace', fontSize: 10,
                      color: 'rgba(255,255,255,0.25)', padding: '4px 0',
                    }}>+{stage.items.length - 15} mais</div>
                  )}
                </div>
              </div>

              {/* Arrow connector */}
              {si < stages.length - 1 && (
                <div style={{
                  flexShrink: 0, width: 40, display: 'flex', flexDirection: 'column',
                  alignItems: 'center', justifyContent: 'center', paddingTop: 36,
                }}>
                  <div style={{ width: 24, height: 1, background: 'rgba(255,255,255,0.15)' }} />
                  <div style={{
                    width: 0, height: 0,
                    borderTop: '4px solid transparent', borderBottom: '4px solid transparent',
                    borderLeft: '6px solid rgba(255,255,255,0.20)', marginLeft: 1,
                  }} />
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
      </div>
    </div>
  );
}

// ─── Overview panel ───────────────────────────────────────────────

function OverviewPanel({ executions, onSelect }) {
  const totalNodes = executions.reduce((sum, ex) => sum + countNodes(ex.sparkPlanInfo), 0);
  return (
    <div style={{
      background: BG, width: '100%', height: '100%',
      display: 'flex', flexDirection: 'column',
      fontFamily: '"DM Sans", sans-serif',
    }}>
      {/* Header bar */}
      <div style={{
        padding: '10px 16px',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
        background: '#000000',
        flexShrink: 0,
        display: 'flex', alignItems: 'center', gap: 24, flexWrap: 'wrap',
      }}>
        <span style={{
          fontFamily: '"Inconsolata",monospace', fontSize: 10,
          color: 'rgba(255,255,255,0.45)', textTransform: 'uppercase', letterSpacing: '0.1em',
        }}>Visão Geral — Plano Físico SQL</span>
        <span style={{
          fontFamily: '"Inconsolata",monospace', fontSize: 11,
          color: 'rgba(255,255,255,0.5)',
        }}>
          <b style={{ color: '#e2e8f0' }}>{executions.length}</b> execuções
          {' · '}
          <b style={{ color: '#e2e8f0' }}>{totalNodes}</b> nós totais
        </span>
      </div>

      {/* Cards grid */}
      <div style={{
        flex: 1, overflowY: 'auto', padding: 14,
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(210px, 1fr))',
        gap: 8, alignContent: 'start',
      }}>
        {executions.map((ex, idx) => {
          const nc = countNodes(ex.sparkPlanInfo);
          return (
            <div
              key={ex.executionId}
              onClick={() => onSelect(ex.executionId)}
              style={{
                background: '#161616',
                border: '1px solid rgba(255,255,255,0.07)',
                borderRadius: 6, padding: '10px 12px', cursor: 'pointer',
              }}
            >
              <div style={{
                fontFamily: '"Inconsolata",monospace', fontSize: 10,
                color: '#e35914', letterSpacing: '0.08em', marginBottom: 4,
              }}>
                #{ex.executionId}
              </div>
              {ex.description && (
                <div style={{
                  fontSize: 11, color: 'rgba(255,255,255,0.65)',
                  marginBottom: 6, lineHeight: 1.4,
                  display: '-webkit-box',
                  WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
                  overflow: 'hidden', wordBreak: 'break-word',
                }}>
                  {ex.description}
                </div>
              )}
              <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                marginTop: 4,
              }}>
                <span style={{
                  fontFamily: '"Inconsolata",monospace', fontSize: 10,
                  color: 'rgba(255,255,255,0.35)',
                }}>{nc} nós</span>
                <span style={{
                  fontFamily: '"Inconsolata",monospace', fontSize: 9,
                  color: 'rgba(255,255,255,0.25)', textTransform: 'uppercase',
                  letterSpacing: '0.06em',
                }}>ver plano →</span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── Main app ────────────────────────────────────────────────────

function SparkPlanApp({ executions }) {
  // Chronological order (natural execution order)
  const sorted = [...executions].sort((a, b) => a.executionId - b.executionId);

  // Start with the execution that has the most nodes (most complete plan)
  const largestExec = sorted.length > 0
    ? sorted.reduce((best, ex) => countNodes(ex.sparkPlanInfo) >= countNodes(best.sparkPlanInfo) ? ex : best, sorted[0])
    : null;

  // ── All hooks declared unconditionally (Rules of Hooks) ──────
  const [selectedExecId, setSelectedExecId] = useState(
    largestExec ? largestExec.executionId : null
  );
  const [selectedNode, setSelectedNode] = useState(null);
  const [showOverview, setShowOverview] = useState(false);
  const [showPipeline, setShowPipeline] = useState(false);

  const currentExec = sorted.find(e => e.executionId === selectedExecId) || sorted[0] || null;

  const layoutData = React.useMemo(() => {
    if (!currentExec) return { nodes: [], treeW: 0, treeH: 0, minX: 0, minY: 0, root: null };
    const processed = preprocessTree(currentExec.sparkPlanInfo);
    if (!processed) return { nodes: [], treeW: 0, treeH: 0, minX: 0, minY: 0, root: null };
    const { nodes, treeW, treeH, minX, minY } = buildLayout(processed);
    return { nodes, treeW, treeH, minX, minY, root: processed };
  }, [currentExec]);

  const stats = React.useMemo(() => collectStats(layoutData.nodes), [layoutData]);
  // ─────────────────────────────────────────────────────────────

  if (sorted.length === 0) {
    return (
      <div style={{
        background: BG, color: 'rgba(255,255,255,0.4)', fontFamily: '"DM Sans",sans-serif',
        fontSize: 13, display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', borderRadius: 6,
      }}>
        Nenhum plano físico SQL disponível.
      </div>
    );
  }

  // ── Pipeline (unified flow) mode ─────────────────────────────
  if (showPipeline) {
    return (
      <PipelineView
        executions={sorted}
        onOpen={id => { setSelectedExecId(id); setSelectedNode(null); setShowPipeline(false); }}
      />
    );
  }

  // ── Overview mode (accessed via toolbar button) ──────────────
  if (showOverview) {
    return (
      <OverviewPanel
        executions={sorted}
        onSelect={id => { setSelectedExecId(id); setSelectedNode(null); setShowOverview(false); }}
      />
    );
  }

  return (
    <div style={{
      background: BG, borderRadius: 6, overflow: 'hidden',
      display: 'flex', flexDirection: 'column',
      width: '100%', height: '100%', position: 'relative',
      fontFamily: '"DM Sans", sans-serif',
    }}>
      {/* Toolbar */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap',
        padding: '7px 12px',
        background: '#000000',
        borderBottom: '1px solid rgba(255,255,255,0.07)',
        flexShrink: 0,
      }}>
        {/* Pipeline button */}
        <button
          onClick={() => { setSelectedNode(null); setShowPipeline(true); }}
          style={{
            background: 'none', border: '1px solid rgba(255,255,255,0.12)',
            borderRadius: 4, color: 'rgba(255,255,255,0.55)', cursor: 'pointer',
            padding: '3px 10px', fontFamily: '"Inconsolata",monospace', fontSize: 10,
            letterSpacing: '0.05em', textTransform: 'uppercase', flexShrink: 0,
          }}
          title="Ver fluxo unificado (origens → processamento → escrita)"
        >⇢ Pipeline</button>

        {/* Overview / list button */}
        <button
          onClick={() => { setSelectedNode(null); setShowOverview(true); }}
          style={{
            background: 'none', border: '1px solid rgba(255,255,255,0.12)',
            borderRadius: 4, color: 'rgba(255,255,255,0.55)', cursor: 'pointer',
            padding: '3px 10px', fontFamily: '"Inconsolata",monospace', fontSize: 10,
            letterSpacing: '0.05em', textTransform: 'uppercase', flexShrink: 0,
          }}
          title={`${sorted.length} execuções disponíveis`}
        >☰ Lista</button>

        {/* Execution selector */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 }}>
          <label style={{
            fontFamily: '"Inconsolata",monospace', fontSize: 10,
            color: 'rgba(255,255,255,0.38)', textTransform: 'uppercase', letterSpacing: '0.08em',
          }}>Execução</label>
          <select
            value={selectedExecId ?? ''}
            onChange={e => {
              const v = e.target.value;
              setSelectedExecId(isNaN(v) ? v : Number(v));
              setSelectedNode(null);
            }}
            style={{
              background: '#000000', border: '1px solid rgba(255,255,255,0.15)',
              borderRadius: 4, color: '#e2e8f0', padding: '3px 8px',
              fontFamily: '"Inconsolata",monospace', fontSize: 11, cursor: 'pointer', outline: 'none',
            }}
          >
            {sorted.map(ex => (
              <option key={ex.executionId} value={ex.executionId}>
                #{ex.executionId}{ex.description ? ' — ' + ex.description.slice(0, 40) : ''}
              </option>
            ))}
          </select>
        </div>

        {/* Stats bar */}
        <StatsBar stats={stats} style={{ flex: 1, minWidth: 0 }} />

        {/* Export PNG */}
        <button
          onClick={() => {
            const canvas = document.querySelector('#spark-plan-canvas-root canvas');
            if (!canvas) return;
            const link = document.createElement('a');
            link.download = `spark-plan-exec${selectedExecId}.png`;
            link.href = canvas.toDataURL('image/png');
            link.click();
          }}
          style={{
            background: 'none', border: '1px solid rgba(255,255,255,0.12)',
            borderRadius: 4, color: 'rgba(255,255,255,0.55)', cursor: 'pointer',
            padding: '3px 10px', fontFamily: '"Inconsolata",monospace', fontSize: 10,
            letterSpacing: '0.05em', textTransform: 'uppercase', flexShrink: 0,
          }}
          title="Exportar diagrama como PNG"
        >↓ PNG</button>
      </div>

      {/* Canvas area + side panel */}
      <div style={{ flex: 1, position: 'relative', overflow: 'hidden', minHeight: 0 }}>
        {/* Canvas */}
        <div style={{
          position: 'absolute', inset: 0,
          right: selectedNode ? 320 : 0,
          transition: 'right 0.18s ease',
        }}>
          <PlanCanvas
            layout={layoutData}
            selectedNode={selectedNode}
            onSelectNode={setSelectedNode}
          />
        </div>

        {/* Legend */}
        <Legend />

        {/* Click hint */}
        {layoutData.nodes.length > 0 && !selectedNode && (
          <div style={{
            position: 'absolute', bottom: 12, right: 12, zIndex: 5,
            background: 'rgba(0,0,0,0.78)', border: '1px solid rgba(255,255,255,0.08)',
            borderRadius: 4, padding: '4px 10px',
            fontFamily: '"Inconsolata",monospace', fontSize: 10,
            color: 'rgba(255,255,255,0.35)',
          }}>
            scroll · drag · clique no nó para detalhes
          </div>
        )}

        {/* Node detail panel */}
        {selectedNode && (
          <NodeDetailPanel
            node={selectedNode}
            onClose={() => setSelectedNode(null)}
          />
        )}
      </div>
    </div>
  );
}

// ─── Public API ──────────────────────────────────────────────────

const _roots = new Map();

function mount(executions, domNode) {
  if (!domNode) return;
  let root = _roots.get(domNode);
  if (!root) {
    root = ReactDOM.createRoot(domNode);
    _roots.set(domNode, root);
  }
  root.render(<SparkPlanApp executions={Array.isArray(executions) ? executions : []} />);
}

function unmount(domNode) {
  const root = _roots.get(domNode);
  if (root) {
    root.unmount();
    _roots.delete(domNode);
  }
}

window.SparkPlanViewer = { mount, unmount };
