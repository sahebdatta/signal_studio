import { useState, useEffect, useRef, useCallback, createContext, useContext } from "react";
import Footer from "./Footer/Footer";

// ═══════════════════════════════════════════════════════════════════════════
//  CONFIG — change BASE_URL and WS_URL to point at your backend
// ═══════════════════════════════════════════════════════════════════════════

// const BASE_URL = "http://0.0.0.0:8000/api/v1";
// const WS_URL = "ws://0.0.0.0:8000/ws";
const BASE_URL = "/api/v1";
const WS_URL = "/ws";

// ═══════════════════════════════════════════════════════════════════════════
//  API CLIENT  — thin wrapper around fetch with auth header + error handling
// ═══════════════════════════════════════════════════════════════════════════

const getToken = () => localStorage.getItem("iiot_token") || "";

const api = {
  async request(method, path, body) {
    const opts = {
      method,
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${getToken()}`,
      },
    };
    if (body !== undefined) opts.body = JSON.stringify(body);
    const res = await fetch(`${BASE_URL}${path}`, opts);
    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: res.statusText }));
      throw Object.assign(new Error(err.detail || "Request failed"), { status: res.status, data: err });
    }
    return res.status === 204 ? null : res.json();
  },
  get: (path) => api.request("GET", path),
  post: (path, body) => api.request("POST", path, body),
  put: (path, body) => api.request("PUT", path, body),
  delete: (path) => api.request("DELETE", path),
};

// ─── Domain API calls ──────────────────────────────────────────────────────

const groupsApi = {
  list: () => api.get("/groups"),
  create: (payload) => api.post("/groups", payload),
  update: (id, payload) => api.put(`/groups/${id}`, payload),
  delete: (id) => api.delete(`/groups/${id}`),
};

const sourcesApi = {
  list: (groupId) => api.get(`/groups/${groupId}/sources`),
  create: (groupId, payload) => api.post(`/groups/${groupId}/sources`, payload),
  update: (groupId, id, payload) => api.put(`/groups/${groupId}/sources/${id}`, payload),
  delete: (groupId, id) => api.delete(`/groups/${groupId}/sources/${id}`),
  test: (payload) => api.post("/sources/test-connection", payload),
  // nodeId is URL-encoded here; "root" fetches the address-space root
  browseNode: (payload, nodeId = "root") =>
    api.post(`/sources/browse-variables/${encodeURIComponent(nodeId)}`, payload),
};

const sinksApi = {
  list: (groupId) => api.get(`/groups/${groupId}/sinks`),
  create: (groupId, payload) => api.post(`/groups/${groupId}/sinks`, payload),
  update: (groupId, id, payload) => api.put(`/groups/${groupId}/sinks/${id}`, payload),
  delete: (groupId, id) => api.delete(`/groups/${groupId}/sinks/${id}`),
  test: (payload) => api.post("/sink/test-connection-sink", payload),
};

const sensorsApi = {
  list: () => api.get("/soft-sensors"),
  create: (payload) => api.post("/soft-sensors", payload),
  update: (id, payload) => api.put(`/soft-sensors/${id}`, payload),
  delete: (id) => api.delete(`/soft-sensors/${id}`),
  validate: (payload) => api.post("/soft-sensors/validate-formula", payload),
};

// ═══════════════════════════════════════════════════════════════════════════
//  WEBSOCKET MANAGER  — singleton, reconnects automatically
// ═══════════════════════════════════════════════════════════════════════════

class WSManager {
  constructor() {
    this.ws = null;
    this.listeners = {};   // topic → Set of callbacks
    this.reconnectTimer = null;
    this.shouldRun = false;
  }

  connect() {
    this.shouldRun = true;
    this._open();
  }

  disconnect() {
    this.shouldRun = false;
    clearTimeout(this.reconnectTimer);
    if (this.ws) { this.ws.onclose = null; this.ws.close(); this.ws = null; }
  }

  _open() {
    // const url = `${WS_URL}?token=${getToken()}`;
    const url = `${WS_URL}`;
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      this._emit("__status__", { connected: true });
      // Re-subscribe to all active topics after reconnect
      Object.keys(this.listeners).filter(t => !t.startsWith("__")).forEach(topic => {
        this._send({ type: "subscribe", topic });
      });
    };

    this.ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        // msg shape: { topic: string, payload: any }
        this._emit(msg.topic, msg.payload);
      } catch { /* ignore malformed frames */ }
    };

    this.ws.onerror = () => this._emit("__status__", { connected: false, error: true });

    this.ws.onclose = () => {
      this._emit("__status__", { connected: false });
      if (this.shouldRun) {
        this.reconnectTimer = setTimeout(() => this._open(), 3000);
      }
    };
  }

  subscribe(topic, cb) {
    if (!this.listeners[topic]) this.listeners[topic] = new Set();
    this.listeners[topic].add(cb);
    // Tell backend we want this topic's events
    if (!topic.startsWith("__") && this.ws?.readyState === WebSocket.OPEN) {
      this._send({ type: "subscribe", topic });
    }
    return () => this.unsubscribe(topic, cb);
  }

  unsubscribe(topic, cb) {
    this.listeners[topic]?.delete(cb);
    if (!this.listeners[topic]?.size && !topic.startsWith("__")) {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this._send({ type: "unsubscribe", topic });
      }
    }
  }

  _send(obj) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }

  _emit(topic, payload) {
    this.listeners[topic]?.forEach(cb => cb(payload));
  }
}

const wsManager = new WSManager();

// ─── React hook: subscribe to a WS topic ──────────────────────────────────

function useWsTopic(topic, onMessage) {
  const cbRef = useRef(onMessage);
  cbRef.current = onMessage;
  useEffect(() => {
    const unsub = wsManager.subscribe(topic, payload => cbRef.current(payload));
    return unsub;
  }, [topic]);
}

// ─── React hook: generic async API call with loading/error state ───────────

function useApiCall() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const call = useCallback(async (apiFn, ...args) => {
    setLoading(true);
    setError(null);
    try {
      const result = await apiFn(...args);
      return result;
    } catch (e) {
      setError(e.message || "Unexpected error");
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  return { loading, error, call };
}

// ═══════════════════════════════════════════════════════════════════════════
//  DESIGN TOKENS
// ═══════════════════════════════════════════════════════════════════════════

const C = {
  bg: "#0f1117", surface: "#161b27", card: "#1c2333", border: "#2a3347",
  borderHover: "#3d4f6b", accent: "#3b82f6", accentDim: "#1e3a5f",
  success: "#10b981", successDim: "#052e1c", warning: "#f59e0b", warningDim: "#2d1f02",
  danger: "#ef4444", dangerDim: "#2d0a0a", text: "#e2e8f0", textMuted: "#64748b",
  textDim: "#94a3b8", purple: "#8b5cf6", purpleDim: "#1e1333", teal: "#14b8a6", tealDim: "#062520",
};

const css = `
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Sora:wght@300;400;500;600&display=swap');
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:${C.bg};color:${C.text};font-family:'Sora',sans-serif;font-size:13px}
  ::-webkit-scrollbar{width:4px;height:4px}
  ::-webkit-scrollbar-track{background:${C.bg}}
  ::-webkit-scrollbar-thumb{background:${C.border};border-radius:2px}
  input,select,textarea{background:${C.bg};border:1px solid ${C.border};color:${C.text};border-radius:6px;padding:8px 12px;font-family:'Sora',sans-serif;font-size:13px;outline:none;transition:border-color 0.2s;width:100%}
  input:focus,select:focus,textarea:focus{border-color:${C.accent}}
  select option{background:${C.card}}
  button{cursor:pointer;font-family:'Sora',sans-serif;font-size:12px;font-weight:500;border:none;border-radius:6px;transition:all 0.15s}
  .pulse{animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.4}}
  @keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
  .fade-in{animation:fadeIn 0.2s ease forwards}
  .btn-primary{background:${C.accent};color:#fff;padding:8px 16px}
  .btn-primary:hover{background:#2563eb}
  .btn-primary:disabled{opacity:0.4;cursor:not-allowed}
  .btn-ghost{background:transparent;color:${C.textDim};border:1px solid ${C.border};padding:6px 12px}
  .btn-ghost:hover{border-color:${C.borderHover};color:${C.text};background:${C.card}}
  .btn-danger{background:${C.dangerDim};color:${C.danger};border:1px solid ${C.danger}44;padding:5px 10px}
  .btn-danger:hover{background:#3d1010}
  .btn-success{background:${C.success};color:#052e1c;padding:8px 16px;font-weight:600}
  .btn-success:hover{background:#059669;color:#fff}
  label{color:${C.textDim};font-size:12px;display:block;margin-bottom:4px;letter-spacing:0.04em}
`;

// ═══════════════════════════════════════════════════════════════════════════
//  UI PRIMITIVES
// ═══════════════════════════════════════════════════════════════════════════

const StatusBadge = ({ status }) => {
  const m = {
    online: { c: C.success, d: C.successDim, l: "Online" }, offline: { c: C.textMuted, d: C.bg, l: "Offline" },
    error: { c: C.danger, d: C.dangerDim, l: "Error" }, connecting: { c: C.warning, d: C.warningDim, l: "Connecting" },
    active: { c: C.success, d: C.successDim, l: "Active" }
  }[status] || { c: C.textMuted, d: C.bg, l: status };
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 5, background: m.d, border: `1px solid ${m.c}33`, borderRadius: 20, padding: "2px 8px" }}>
      <span style={{ width: 6, height: 6, borderRadius: "50%", background: m.c, flexShrink: 0 }}
        className={["online", "connecting", "active"].includes(status) ? "pulse" : ""} />
      <span style={{ color: m.c, fontSize: 11, fontWeight: 500 }}>{m.l}</span>
    </span>
  );
};

const Card = ({ children, style = {} }) => (
  <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, padding: "14px 16px", ...style }}>{children}</div>
);

const Modal = ({ title, onClose, children, width = 560 }) => (
  <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.75)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000, padding: 16 }}>
    <div className="fade-in" style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 12, width: "100%", maxWidth: width, maxHeight: "92vh", overflow: "auto" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "16px 20px", borderBottom: `1px solid ${C.border}` }}>
        <span style={{ fontWeight: 600, fontSize: 14 }}>{title}</span>
        <button onClick={onClose} style={{ background: "transparent", color: C.textMuted, fontSize: 20, border: "none", cursor: "pointer", lineHeight: 1, padding: "0 4px" }}>×</button>
      </div>
      <div style={{ padding: 20 }}>{children}</div>
    </div>
  </div>
);

const Stepper = ({ steps, current, onStepClick }) => (
  <div style={{ display: "flex", alignItems: "center", marginBottom: 24 }}>
    {steps.map((s, i) => (
      <div key={i} style={{ display: "flex", alignItems: "center", flex: i < steps.length - 1 ? 1 : "none" }}>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
          <div onClick={() => onStepClick && i < current && onStepClick(i)}
            style={{
              width: 28, height: 28, borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 12, fontWeight: 600, transition: "all 0.25s",
              background: i < current ? C.success : i === current ? C.accent : C.border,
              color: i <= current ? "white" : C.textMuted,
              border: `2px solid ${i === current ? C.accent : "transparent"}`,
              cursor: onStepClick && i < current ? "pointer" : "default"
            }}>
            {i < current ? "✓" : i + 1}
          </div>
          <span style={{ fontSize: 10, color: i === current ? C.accent : C.textMuted, whiteSpace: "nowrap" }}>{s}</span>
        </div>
        {i < steps.length - 1 && <div style={{ flex: 1, height: 1, background: i < current ? C.success : C.border, margin: "0 8px", marginBottom: 18 }} />}
      </div>
    ))}
  </div>
);

const FF = ({ label, children }) => (
  <div style={{ marginBottom: 14 }}><label>{label}</label>{children}</div>
);

const Spinner = () => (
  <span style={{ display: "inline-block", width: 14, height: 14, border: `2px solid ${C.border}`, borderTopColor: C.accent, borderRadius: "50%", animation: "spin 0.7s linear infinite", verticalAlign: "middle", marginRight: 6 }} />
);

const ErrorBanner = ({ msg, onDismiss }) => msg ? (
  <div style={{ padding: "8px 12px", background: C.dangerDim, border: `1px solid ${C.danger}44`, borderRadius: 6, color: C.danger, fontSize: 12, marginBottom: 12, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
    <span>⚠ {msg}</span>
    {onDismiss && <button onClick={onDismiss} style={{ background: "transparent", border: "none", color: C.danger, cursor: "pointer", padding: "0 4px", fontSize: 14 }}>×</button>}
  </div>
) : null;

// ═══════════════════════════════════════════════════════════════════════════
//  OPC UA TREE BROWSER
//  A single tree node row. Clicking the chevron expands/collapses; clicking
//  the checkbox (Variable nodes only) toggles selection.
// ═══════════════════════════════════════════════════════════════════════════

function TreeNode({ node, depth, connPayload, selected, onToggle, liveValues, nodeCache, setNodeCache }) {
  const [open, setOpen] = useState(false);
  const [children, setChildren] = useState(null);   // null = not yet fetched
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const isVar = node.node_class === "Variable";
  const isChecked = isVar && selected.includes(node.node_id);
  const indent = depth * 18;

  const handleExpand = async () => {
    if (!node.has_children) return;
    const next = !open;
    setOpen(next);
    if (!next || children !== null) return;           // collapse or already loaded

    // Check cache first
    if (nodeCache[node.node_id]) {
      setChildren(nodeCache[node.node_id]);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const resp = await sourcesApi.browseNode(connPayload, node.node_id);
      const kids = resp?.children || [];
      setChildren(kids);
      setNodeCache(c => ({ ...c, [node.node_id]: kids }));
    } catch (e) {
      setError(e.message || "Browse failed");
    } finally {
      setLoading(false);
    }
  };

  const lv = liveValues[node.node_id];
  const displayValue = lv !== undefined ? lv.toFixed(3) : (node.value !== undefined ? Number(node.value).toFixed(3) : "—");

  return (
    <>
      <div
        style={{
          display: "flex", alignItems: "center", gap: 6,
          padding: "5px 10px", paddingLeft: 10 + indent,
          borderBottom: `1px solid ${C.border}18`,
          background: isChecked ? C.accentDim : "transparent",
          cursor: isVar ? "pointer" : "default",
          transition: "background 0.12s",
        }}
        onClick={() => isVar && onToggle(node)}
      >
        {/* Expand chevron — only for Object nodes with children */}
        <span
          onClick={e => { e.stopPropagation(); handleExpand(); }}
          style={{
            width: 16, height: 16, display: "flex", alignItems: "center", justifyContent: "center",
            flexShrink: 0, cursor: node.has_children ? "pointer" : "default",
            color: node.has_children ? C.textDim : "transparent",
            fontSize: 10, userSelect: "none",
            transition: "transform 0.18s",
            transform: open ? "rotate(90deg)" : "rotate(0deg)",
          }}
        >
          {node.has_children ? "▶" : ""}
        </span>

        {/* Node icon */}
        <span style={{ fontSize: 13, flexShrink: 0, color: isVar ? C.teal : C.accent, lineHeight: 1 }}>
          {isVar ? "◆" : "▣"}
        </span>

        {/* Display name */}
        <span style={{ flex: 1, fontSize: 12, color: isVar ? C.text : C.textDim, fontWeight: isVar ? 400 : 500 }}>
          {node.display_name}
        </span>

        {/* Variable metadata */}
        {isVar && (
          <>
            <span style={{ fontSize: 11, color: C.textMuted, fontFamily: "IBM Plex Mono, monospace", minWidth: 80, textAlign: "right" }}>
              {displayValue}
              {node.unit ? <span style={{ color: C.textMuted, marginLeft: 3 }}>{node.unit}</span> : null}
            </span>
            <span style={{ fontSize: 10, color: C.textMuted, minWidth: 38, textAlign: "right" }}>
              {node.data_type}
            </span>
            <span style={{ width: 16, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
              <input
                type="checkbox"
                checked={isChecked}
                onChange={() => onToggle(node)}
                onClick={e => e.stopPropagation()}
                style={{ cursor: "pointer", accentColor: C.accent }}
              />
            </span>
          </>
        )}

        {/* Folder node item count hint */}
        {!isVar && !loading && children !== null && (
          <span style={{ fontSize: 10, color: C.textMuted }}>
            {children.length} item{children.length !== 1 ? "s" : ""}
          </span>
        )}

        {loading && (
          <span style={{ fontSize: 10, color: C.textMuted }}>
            <Spinner />
          </span>
        )}
      </div>

      {error && (
        <div style={{ paddingLeft: 10 + indent + 22, padding: "4px 10px 4px " + (10 + indent + 22) + "px", fontSize: 11, color: C.danger }}>
          ⚠ {error}
        </div>
      )}

      {/* Render children when expanded */}
      {open && children && children.map(child => (
        <TreeNode
          key={child.node_id}
          node={child}
          depth={depth + 1}
          connPayload={connPayload}
          selected={selected}
          onToggle={onToggle}
          liveValues={liveValues}
          nodeCache={nodeCache}
          setNodeCache={setNodeCache}
        />
      ))}
    </>
  );
}

// ─── OpcuaTreeBrowser ─────────────────────────────────────────────────────
// Loads the root level on mount then renders the lazy tree.

function OpcuaTreeBrowser({ connPayload, selected, onToggle, liveValues }) {
  const [roots, setRoots] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  // Shared cache: nodeId → children[] so revisiting a node is instant
  const [nodeCache, setNodeCache] = useState({});

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    sourcesApi.browseNode(connPayload, "root")
      .then(resp => { if (!cancelled) { setRoots(resp?.children || []); setLoading(false); } })
      .catch(e => { if (!cancelled) { setError(e.message || "Browse failed"); setLoading(false); } });
    return () => { cancelled = true; };
  }, []);  // run once when step 2 mounts

  if (loading) return (
    <div style={{ padding: "24px 0", textAlign: "center", color: C.textMuted, fontSize: 12 }}>
      <Spinner /> Loading address space…
    </div>
  );

  if (error) return (
    <div style={{ padding: "16px", color: C.danger, fontSize: 12 }}>⚠ {error}</div>
  );

  return (
    <div style={{ border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
      {/* Column headers */}
      <div style={{
        display: "flex", alignItems: "center", gap: 6,
        padding: "7px 10px", background: C.surface,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <span style={{ width: 16, flexShrink: 0 }} />
        <span style={{ width: 16, flexShrink: 0 }} />
        <span style={{ flex: 1, fontSize: 11, color: C.textMuted, fontWeight: 500 }}>Node</span>
        <span style={{ minWidth: 80, fontSize: 11, color: C.textMuted, textAlign: "right" }}>Value</span>
        <span style={{ minWidth: 38, fontSize: 11, color: C.textMuted, textAlign: "right" }}>Type</span>
        <span style={{ width: 16 }} />
      </div>

      <div style={{ maxHeight: 340, overflowY: "auto" }}>
        {roots.map(node => (
          <TreeNode
            key={node.node_id}
            node={node}
            depth={0}
            connPayload={connPayload}
            selected={selected}
            onToggle={onToggle}
            liveValues={liveValues}
            nodeCache={nodeCache}
            setNodeCache={setNodeCache}
          />
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  SOURCE MODAL  — connects to backend for test + browse (tree), then saves
// ═══════════════════════════════════════════════════════════════════════════

function SourceModal({ groupId, existing, onClose, onSaved }) {
  const isEdit = !!existing;
  const { loading, error, call } = useApiCall();

  const [step, setStep] = useState(0);
  const [srcType, setSrcType] = useState(existing?.protocol || "opcua");
  const [addr, setAddr] = useState(existing?.connection?.endpoint || "opc.tcp://");
  const [extras, setExtras] = useState(existing?.connection || {});
  const [connected, setConnected] = useState(isEdit);

  // selectedNodes: map of node_id → full node object (carries display_name, data_type, unit, value)
  // This replaces the old flat `selected` string array and `variables` list.
  const [selectedNodes, setSelectedNodes] = useState(
    Object.fromEntries(
      (existing?.variables || []).map(v => [v.tag, {
        node_id: v.tag, display_name: v.name,
        node_class: "Variable", data_type: v.data_type || "Float",
        unit: v.unit || "", value: v.last_value,
      }])
    )
  );

  const [aliases, setAliases] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.alias || ""]))
  );
  const [descs, setDescs] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.description || ""]))
  );
  const [unit, setUnit] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.unit || ""]))
  );
  const [alarm_low, setAlarmLow] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.alarm_low || 0]))
  );
  const [alarm_high, setAlarmHigh] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.alarm_high || 0]))
  );
  const [warning_low, setWarningLow] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.warning_low || 0]))
  );
  const [warning_high, setWarningHigh] = useState(
    Object.fromEntries((existing?.variables || []).map(v => [v.tag, v.warning_high || 0]))
  );
  const [liveValues, setLiveValues] = useState({});

  // Live values via WebSocket while on browse step
  useWsTopic(`source.browse.${groupId}`, payload => {
    if (payload?.values) setLiveValues(v => ({ ...v, ...payload.values }));
  });

  const setX = (k, v) => setExtras(e => ({ ...e, [k]: v }));

  const buildConnectionPayload = () => {
    if (srcType === "opcua") return { protocol: "opcua", connection: { endpoint: addr } };
    if (srcType === "modbus") return { protocol: "modbus", connection: { host: extras.host, port: Number(extras.port) || 502, unit_id: Number(extras.unitId) || 1 } };
    if (srcType === "tcpip") return { protocol: "tcpip", connection: { host: extras.host, port: Number(extras.port) || 9000, format: extras.fmt || "json" } };
    if (srcType === "rest") return { protocol: "rest", connection: { endpoint: addr, poll_interval_ms: Number(extras.poll) || 1000, auth_header: extras.auth || null } };
    return { protocol: srcType, connection: {} };
  };

  const handleConnect = async () => {
    const result = await call(sourcesApi.test, buildConnectionPayload());
    if (result?.success) setConnected(true);
  };


  // Toggle a Variable node in/out of the selection map
  const toggleNode = node => {
    setSelectedNodes(prev => {
      const next = { ...prev };
      if (next[node.node_id]) {
        delete next[node.node_id];
      } else {
        next[node.node_id] = node;
      }
      return next;
    });
  };

  const selectedIds = Object.keys(selectedNodes);
  const selectedArray = Object.values(selectedNodes);

  // ── Final save
  const handleSave = async () => {
    const payload = {
      protocol: srcType,
      connection: buildConnectionPayload().connection,
      variables: selectedArray.map(n => ({
        tag: n.node_id,
        name: n.display_name,
        alias: aliases[n.node_id] || n.display_name,
        description: descs[n.node_id] || "",
        data_type: n.data_type,
        unit: unit[n.node_id] || "",
        alarm_low: parseFloat(alarm_low[n.node_id]) || 0,
        alarm_high: parseFloat(alarm_high[n.node_id]) || 0,
        warning_low: parseFloat(warning_low[n.node_id]) || 0,
        warning_high: parseFloat(warning_high[n.node_id]) || 0,
      })),
    };
    let saved;
    if (isEdit) {
      saved = await call(sourcesApi.update, groupId, existing.id, payload);
    } else {
      saved = await call(sourcesApi.create, groupId, payload);
    }
    if (saved) { onSaved(saved); onClose(); }
  };

  const connPayload = buildConnectionPayload();

  return (
    <Modal title={isEdit ? "Edit Data Source" : "Add Data Source"} onClose={onClose} width={680}>
      <Stepper steps={["Type", "Configure", "Browse", "Aliases", "Review"]} current={step} onStepClick={setStep} />
      <ErrorBanner msg={error} />

      {/* ── Step 0: Type ── */}
      {step === 0 && (
        <div className="fade-in">
          <label>Source Type</label>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 6 }}>
            {[
              ["opcua", "OPC UA", "Industrial standard protocol"],
              // ["modbus", "Modbus TCP", "Legacy PLC connectivity"],
              // ["tcpip", "TCP/IP Raw", "Raw socket stream"],
              // ["rest", "REST API", "HTTP polling source"],
            ].map(([k, lbl, sub]) => (
              <div key={k} onClick={() => setSrcType(k)}
                style={{ padding: "12px 14px", border: `1.5px solid ${srcType === k ? C.accent : C.border}`, borderRadius: 8, cursor: "pointer", background: srcType === k ? C.accentDim : "transparent", transition: "all 0.15s" }}>
                <div style={{ fontWeight: 600, marginBottom: 2, color: srcType === k ? C.accent : C.text }}>{lbl}</div>
                <div style={{ fontSize: 11, color: C.textMuted }}>{sub}</div>
              </div>
            ))}
          </div>
          <div style={{ marginTop: 20, display: "flex", justifyContent: "flex-end" }}>
            <button className="btn-primary" onClick={() => setStep(1)}>Next →</button>
          </div>
        </div>
      )}

      {/* ── Step 1: Configure ── */}
      {step === 1 && (
        <div className="fade-in">
          {srcType === "opcua" && <FF label="OPC UA Server Address"><input value={addr} onChange={e => setAddr(e.target.value)} placeholder="opc.tcp://192.168.1.100:4840" /></FF>}
          {srcType === "modbus" && <>
            <FF label="Host / IP Address"><input value={extras.host || ""} onChange={e => setX("host", e.target.value)} placeholder="192.168.1.200" /></FF>
            <FF label="Port"><input value={extras.port || ""} onChange={e => setX("port", e.target.value)} placeholder="502" /></FF>
            <FF label="Unit ID"><input value={extras.unitId || ""} onChange={e => setX("unitId", e.target.value)} placeholder="1" /></FF>
          </>}
          {srcType === "tcpip" && <>
            <FF label="Host"><input value={extras.host || ""} onChange={e => setX("host", e.target.value)} placeholder="192.168.1.50" /></FF>
            <FF label="Port"><input value={extras.port || ""} onChange={e => setX("port", e.target.value)} placeholder="9000" /></FF>
            <FF label="Data Format">
              <select value={extras.fmt || "json"} onChange={e => setX("fmt", e.target.value)}>
                <option value="json">JSON</option><option value="csv">CSV</option><option value="raw">Raw Bytes</option>
              </select>
            </FF>
          </>}
          {srcType === "rest" && <>
            <FF label="Endpoint URL"><input value={addr} onChange={e => setAddr(e.target.value)} placeholder="https://api.example.com/data" /></FF>
            <FF label="Poll Interval (ms)"><input value={extras.poll || ""} onChange={e => setX("poll", e.target.value)} placeholder="1000" /></FF>
            <FF label="Auth Header"><input value={extras.auth || ""} onChange={e => setX("auth", e.target.value)} placeholder="Bearer …" /></FF>
          </>}

          {!connected ? (
            <button className="btn-primary" onClick={handleConnect} disabled={loading} style={{ marginTop: 8 }}>
              {loading && <Spinner />}{loading ? "Testing connection…" : "Test & Connect"}
            </button>
          ) : (
            <div style={{ marginTop: 10, padding: "8px 12px", background: C.successDim, border: `1px solid ${C.success}44`, borderRadius: 6, color: C.success, fontSize: 12 }}>
              ✓ Connection verified — server ready
            </div>
          )}
          <div style={{ marginTop: 16, display: "flex", justifyContent: "space-between" }}>
            <button className="btn-ghost" onClick={() => setStep(0)}>← Back</button>
            <button className="btn-primary" onClick={() => setStep(2)}> {/*disabled={!connected}>*/}
              Browse Variables →
            </button>
          </div>
        </div>
      )}

      {/* ── Step 2: Browse (tree) ── */}
      {step === 2 && (
        <div className="fade-in">
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
            <div style={{ fontSize: 12, color: C.textMuted }}>
              Expand folders to navigate the address space. Check{" "}
              <span style={{ color: C.teal }}>◆ Variable</span> nodes to select them.
            </div>
            {selectedIds.length > 0 && (
              <span style={{
                fontSize: 11, background: C.accentDim, color: C.accent,
                border: `1px solid ${C.accent}44`, borderRadius: 20,
                padding: "2px 10px", whiteSpace: "nowrap",
              }}>
                {selectedIds.length} selected
              </span>
            )}
          </div>

          <OpcuaTreeBrowser
            connPayload={connPayload}
            selected={selectedIds}
            onToggle={toggleNode}
            liveValues={liveValues}
          />

          {/* Selected tags summary strip */}
          {selectedIds.length > 0 && (
            <div style={{ marginTop: 10, display: "flex", gap: 6, flexWrap: "wrap",  maxHeight: 150, overflowY: "auto" }}>
              {selectedArray.map(n => (
                <span key={n.node_id}
                  onClick={() => toggleNode(n)}
                  style={{
                    fontSize: 11, background: C.tealDim, color: C.teal,
                    border: `1px solid ${C.teal}44`, borderRadius: 4,
                    padding: "2px 8px", cursor: "pointer",
                    display: "inline-flex", alignItems: "center", gap: 5,
                  }}
                  title={`Click to deselect ${n.display_name}`}
                >
                  {n.display_name}
                  <span style={{ color: C.textMuted, fontSize: 12, lineHeight: 1 }}>×</span>
                </span>
              ))}
            </div>
          )}

          <div style={{ marginTop: 14, display: "flex", justifyContent: "space-between" }}>
            <button className="btn-ghost" onClick={() => setStep(1)}>← Back</button>
            <button className="btn-primary" onClick={() => setStep(3)}> {/*disabled={selectedIds.length === 0}>*/}
              Set Aliases →
            </button>
          </div>
        </div>
      )}

      {/* ── Step 3: Aliases ── */}
      {step === 3 && (
        <div className="fade-in">
          <div style={{ fontSize: 12, color: C.textMuted, marginBottom: 14 }}>
            Assign human-readable names, descriptions and limits to each selected variable.
          </div>
          <div style={{ maxHeight: 400, overflowY: "auto" }}>
            {selectedArray.map(n => (
              <Card key={n.node_id} style={{ marginBottom: 10, padding: "12px 14px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
                  <span title={n.node_id} style={{ fontSize: 11, color: C.textMuted, fontFamily: "IBM Plex Mono, monospace", background: C.bg, border: `1px solid ${C.border}`, borderRadius: 4, padding: "2px 6px", maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {n.display_name} | {n.node_id}
                  </span>
                  <span style={{ color: C.accent, fontFamily: "IBM Plex Mono, monospace", fontSize: 12, flexShrink: 0, marginLeft: 8 }}>
                    {liveValues[aliases[n.node_id]] !== undefined ? liveValues[aliases[n.node_id]].toFixed(3) : (n.value !== undefined ? Number(n.value).toFixed(3) : "—")}
                    {n.unit ? <span style={{ color: C.textMuted, marginLeft: 4 }}>{n.unit}</span> : null}
                  </span>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 8 }}>
                  <div>
                    <label>Alias Name</label>
                    <input value={aliases[n.node_id] || ""} onChange={e => setAliases(a => ({ ...a, [n.node_id]: e.target.value }))} placeholder={n.display_name} />
                  </div>
                  <div>
                    <label>Description</label>
                    <input value={descs[n.node_id] || ""} onChange={e => setDescs(d => ({ ...d, [n.node_id]: e.target.value }))} placeholder="Optional…" />
                  </div>
                  <div>
                    <label>Unit</label>
                    <input value={unit[n.node_id] || ""} onChange={e => setUnit(u => ({ ...u, [n.node_id]: e.target.value }))} placeholder="" />
                  </div>
                </div>
                <div style={{ height: 2, background: C.border, margin: "2px 0" }} />
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 60 }}>
                  <div>
                    {/* <label>alarm low</label> */}
                    <input style={{textAlign: "center"}} value={alarm_low[n.node_id] || 0} onChange={e => setAlarmLow(a => ({ ...a, [n.node_id]: e.target.value }))} />
                  </div>
                  <div>
                    {/* <label>warning low</label> */}
                    <input style={{textAlign: "center"}} value={warning_low[n.node_id] || 0} onChange={e => setWarningLow(w => ({ ...w, [n.node_id]: e.target.value }))} />
                  </div>
                  <div>
                    {/* <label>warning high</label> */}
                    <input style={{textAlign: "center"}} value={warning_high[n.node_id] || 0} onChange={e => setWarningHigh(w => ({ ...w, [n.node_id]: e.target.value }))} />
                  </div>
                  <div>
                    {/* <label>alarm high</label> */}
                    <input style={{textAlign: "center"}} value={alarm_high[n.node_id] || 0} onChange={e => setAlarmHigh(a => ({ ...a, [n.node_id]: e.target.value }))} />
                  </div>
                </div>
                <svg height="10" width={"100%"}>
                  <line x1="0" y1="5" x2="50" y2="5" stroke="red" strokeWidth="3" />
                  <line x1="50" y1="-10" x2="50" y2="10" stroke="red" strokeWidth="4" />
                  <line x1="55" y1="5" x2="220" y2="5" stroke="yellow" strokeWidth="3" />
                  <line x1="220" y1="-10" x2="220" y2="10" stroke="yellow" strokeWidth="4" />
                  <line x1="225" y1="5" x2="375" y2="5" stroke="green" strokeWidth="3" />
                  <line x1="380" y1="-10" x2="380" y2="10" stroke="yellow" strokeWidth="4" />
                  <line x1="380" y1="5" x2="545" y2="5" stroke="yellow" strokeWidth="3" />
                  <line x1="550" y1="-10" x2="550" y2="10" stroke="red" strokeWidth="4" />
                  <line x1="550" y1="5" x2="600" y2="5" stroke="red" strokeWidth="3" />
                </svg>
              </Card>
            ))}
          </div>
          <div style={{ marginTop: 16, display: "flex", justifyContent: "space-between" }}>
            <button className="btn-ghost" onClick={() => setStep(2)}>← Back</button>
            <button className="btn-primary" onClick={() => setStep(4)}>Review →</button>
          </div>
        </div>
      )}

      {/* ── Step 4: Review ── */}
      {step === 4 && (
        <div className="fade-in">
          <div style={{ padding: "12px 14px", background: C.bg, borderRadius: 8, border: `1px solid ${C.border}`, marginBottom: 14 }}>
            {[["Protocol", srcType.toUpperCase()], ["Endpoint", addr], ["Variables", `${selectedIds.length} selected`]].map(([k, v]) => (
              <div key={k} style={{ display: "flex", gap: 10, marginBottom: 8 }}>
                <span style={{ color: C.textMuted, fontSize: 12, width: 80, flexShrink: 0 }}>{k}</span>
                <span style={{ fontFamily: k === "Endpoint" ? "IBM Plex Mono, monospace" : "inherit", fontSize: 12, color: k === "Variables" ? C.success : C.text, fontWeight: k === "Variables" ? 600 : 400 }}>{v}</span>
              </div>
            ))}
          </div>
          <div style={{ maxHeight: 220, overflowY: "auto" }}>
            {selectedArray.map(n => (
              <div key={n.node_id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 0", borderBottom: `1px solid ${C.border}22`, fontSize: 12 }}>
                <div>
                  <div style={{ color: C.text, fontWeight: 500 }}>{aliases[n.node_id] || n.display_name}</div>
                  <div style={{ fontSize: 11, color: C.textMuted, fontFamily: "IBM Plex Mono, monospace", marginTop: 1 }}>{n.node_id}</div>
                </div>
                <div style={{ textAlign: "right", flexShrink: 0, marginLeft: 12 }}>
                  <div style={{ fontSize: 11, color: C.textDim }}>{descs[n.node_id] || "—"}</div>
                  <div style={{ fontSize: 11, color: C.textMuted }}>{liveValues[aliases[n.node_id]] !== undefined ? liveValues[aliases[n.node_id]].toFixed(3) : (n.value !== undefined ? Number(n.value).toFixed(3) : "—")} {n.unit || "—"} [{n.data_type}]</div>
                </div>
              </div>
            ))}
          </div>
          <div style={{ marginTop: 20, display: "flex", justifyContent: "space-between" }}>
            <button className="btn-ghost" onClick={() => setStep(3)}>← Back</button>
            <button className="btn-success" onClick={handleSave} disabled={loading}>
              {loading && <Spinner />}{isEdit ? "Update Source" : "Save Source"}
            </button>
          </div>
        </div>
      )}
    </Modal>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  SINK MODAL
// ═══════════════════════════════════════════════════════════════════════════

function SinkModal({ groupId, existing, onClose, onSaved }) {
  const isEdit = !!existing;
  const { loading, error, call } = useApiCall();

  const [sinkType, setSinkType] = useState(existing?.protocol || "mqtt");
  const [addr, setAddr] = useState(existing?.connection?.endpoint || "mqtt://");
  const [extras, setExtras] = useState(existing?.connection || {});

  const [type, setType] = useState(existing?.adapter_type || "mqtt");
  const [cfg, setCfg] = useState(existing?.config || {});
  const set = (k, v) => setCfg(c => ({ ...c, [k]: v }));

  const setX = (k, v) => setExtras(e => ({ ...e, [k]: v }));

  const [connected, setConnected] = useState(isEdit);

  const handleSave = async () => {
    if (type === "mqtt") {
      cfg.qos === undefined ? cfg.qos = 0 : cfg.qos = cfg.qos
      cfg.topic === undefined ? cfg.topic = "factory/zone1/signals" : cfg.topic = cfg.topic
      cfg.username === undefined ? cfg.username = "" : cfg.username = cfg.username
      cfg.password === undefined ? cfg.password = "" : cfg.password = cfg.password
    }
    const payload = { adapter_type: type, config: cfg };
    let saved;
    if (isEdit) {
      saved = await call(sinksApi.update, groupId, existing.id, payload);
    } else {
      saved = await call(sinksApi.create, groupId, payload);
    }
    if (saved) { onSaved(saved); onClose(); }
  };

  const buildConnectionPayload = () => {
    if (type === "mqtt") {
      cfg.qos === undefined ? cfg.qos = 0 : cfg.qos = cfg.qos
      cfg.topic === undefined ? cfg.topic = "factory/zone1/signals" : cfg.topic = cfg.topic
      cfg.username === undefined ? cfg.username = "" : cfg.username = cfg.username
      cfg.password === undefined ? cfg.password = "" : cfg.password = cfg.password

      return { protocol: "mqtt", connection: { endpoint: cfg.broker_url, username: cfg.username, password: cfg.password, topic: cfg.topic, qos: cfg.qos } };
    }
    if (type === "influxdb") return { protocol: "influxdb", connection: { host: extras.host, port: Number(extras.port) || 502, unit_id: Number(extras.unitId) || 1 } };
    return { protocol: type, connection: {} };
  };

  const handleConnectSink = async () => {
    const result = await call(sinksApi.test, buildConnectionPayload());
    if (result?.success) setConnected(true);
  };

  return (
    <Modal title={isEdit ? "Edit Data Sink" : "Add Data Sink"} onClose={onClose}>
      <ErrorBanner msg={error} />
      <FF label="Adapter Type">
        <select value={type} onChange={e => setType(e.target.value)}>
          <option value="mqtt">MQTT Broker</option>
          <option value="influxdb">InfluxDB</option>
          {/* <option value="kafka">Apache Kafka</option>
          <option value="webhook">Webhook / HTTP</option> */}
        </select>
      </FF>

      {type === "mqtt" && <>
        {/* {srcType === "opcua" && <FF label="OPC UA Server Address"><input value={addr} onChange={e => setAddr(e.target.value)} placeholder="opc.tcp://192.168.1.100:4840" /></FF>} */}

        <FF label="Broker URL"><input value={cfg.broker_url} onChange={e => set("broker_url", e.target.value)} placeholder="mqtt://broker.example.com:1883" /></FF>
        <FF label="Username"><input value={cfg.username || ""} onChange={e => set("username", e.target.value)} /></FF>
        <FF label="Password"><input value={cfg.password || ""} onChange={e => set("password", e.target.value)} type="password" /></FF>

        <FF label="Topic"><input value={cfg.topic || ""} onChange={e => set("topic", e.target.value)} placeholder="factory/zone1/signals" /></FF>
        <FF label="QoS Level">
          <select value={cfg.qos ?? 0} onChange={e => set("qos", Number(e.target.value))}>
            <option value={0}>0 — At most once</option><option value={1}>1 — At least once</option><option value={2}>2 — Exactly once</option>
          </select>
        </FF>
      </>}
      {type === "influxdb" && <>
        <FF label="Server URL"><input value={cfg.server_url || ""} onChange={e => set("server_url", e.target.value)} placeholder="http://influxdb.local:8086" /></FF>
        <FF label="API Token"><input value={cfg.token || ""} onChange={e => set("token", e.target.value)} type="password" placeholder="your-influx-token" /></FF>
        <FF label="Bucket"><input value={cfg.bucket || ""} onChange={e => set("bucket", e.target.value)} placeholder="sensor_data" /></FF>
        <FF label="Organization"><input value={cfg.org || ""} onChange={e => set("org", e.target.value)} placeholder="my-org" /></FF>
      </>}
      {type === "kafka" && <>
        <FF label="Bootstrap Servers"><input value={cfg.bootstrap_servers || ""} onChange={e => set("bootstrap_servers", e.target.value)} placeholder="kafka1:9092,kafka2:9092" /></FF>
        <FF label="Topic"><input value={cfg.topic || ""} onChange={e => set("topic", e.target.value)} placeholder="sensor-stream" /></FF>
        <FF label="Serialization"><select value={cfg.serialization || "json"} onChange={e => set("serialization", e.target.value)}><option value="json">JSON</option><option value="avro">Avro</option><option value="protobuf">Protobuf</option></select></FF>
      </>}
      {type === "webhook" && <>
        <FF label="Endpoint URL"><input value={cfg.endpoint_url || ""} onChange={e => set("endpoint_url", e.target.value)} placeholder="https://api.example.com/ingest" /></FF>
        <FF label="HTTP Method"><select value={cfg.method || "POST"} onChange={e => set("method", e.target.value)}><option>POST</option><option>PUT</option></select></FF>
        <FF label="Auth Header"><input value={cfg.auth_header || ""} onChange={e => set("auth_header", e.target.value)} placeholder="Authorization: Bearer …" /></FF>
      </>}

      {!connected ? (
        <button className="btn-primary" onClick={handleConnectSink} disabled={loading} style={{ marginTop: 8 }}>
          {loading && <Spinner />}{loading ? "Testing connection…" : "Test & Connect"}
        </button>
      ) : (
        <div style={{ marginTop: 10, padding: "8px 12px", background: C.successDim, border: `1px solid ${C.success}44`, borderRadius: 6, color: C.success, fontSize: 12 }}>
          ✓ Connection verified — server ready
        </div>
      )}
      {/* <div style={{ marginTop: 16, display: "flex", justifyContent: "space-between" }}> */}
      {/* <button className="btn-ghost" onClick={() => setStep(0)}>← Back</button> */}
      {/* <button className="btn-primary" disabled={!connected}>
              Browse Variables →
            </button>
          </div> */}

      <div style={{ marginTop: 20, display: "flex", justifyContent: "flex-end", gap: 8 }}>
        <button className="btn-ghost" onClick={onClose}>Cancel</button>
        <button className="btn-success" onClick={handleSave} disabled={loading}>
          {loading && <Spinner />}{isEdit ? "Update Sink" : "Save Sink"}
        </button>
      </div>
    </Modal>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  SENSOR MODAL
// ═══════════════════════════════════════════════════════════════════════════

const FORMULA_VARS = ["Temperature_Zone1", "Main_Pressure", "Inlet_Flow", "Motor_Vibration"];

function SensorModal({ existing, onClose, onSaved }) {
  const isEdit = !!existing;
  const { loading, error, call } = useApiCall();
  const parseVars = f => FORMULA_VARS.filter(v => f.includes(v));

  const [name, setName] = useState(existing?.name || "");
  const [formula, setFormula] = useState(existing?.formula || "");
  const [output, setOutput] = useState(existing?.output_tag || "");
  const [unit, setUnit] = useState(existing?.unit || "");
  const [fVars, setFVars] = useState(existing ? parseVars(existing.formula) : []);
  const [valErr, setValErr] = useState(null);

  const handleFormula = v => { setFormula(v); setFVars(parseVars(v)); setValErr(null); };

  // Validate formula against backend before saving
  const handleSave = async () => {
    if (!name || !formula) return;
    // Validate first
    const validation = await call(sensorsApi.validate, {
      formula,
      input_tags: parseVars(formula),
    });
    if (!validation?.valid) {
      setValErr(validation?.error || "Formula validation failed");
      return;
    }

    const payload = {
      name,
      formula,
      input_tags: parseVars(formula),
      output_tag: output || "out",
      unit,
    };
    let saved;
    if (isEdit) {
      saved = await call(sensorsApi.update, existing.id, payload);
    } else {
      saved = await call(sensorsApi.create, payload);
    }
    if (saved) { onSaved(saved); onClose(); }
  };

  return (
    <Modal title={isEdit ? "Edit Soft Sensor" : "Create Soft Sensor"} onClose={onClose}>
      <ErrorBanner msg={error || valErr} onDismiss={() => setValErr(null)} />
      <FF label="Sensor Name"><input value={name} onChange={e => setName(e.target.value)} placeholder="e.g. Energy Efficiency Index" /></FF>
      <FF label="Formula Expression">
        <textarea value={formula} onChange={e => handleFormula(e.target.value)} rows={3}
          placeholder="e.g. Main_Pressure * Inlet_Flow / 1000" style={{ resize: "vertical" }} />
        <div style={{ marginTop: 6, fontSize: 11, color: C.textMuted }}>Available: {FORMULA_VARS.join(", ")}</div>
      </FF>
      {fVars.length > 0 && (
        <div style={{ marginBottom: 14, display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
          <span style={{ fontSize: 11, color: C.textMuted }}>Detected:</span>
          {fVars.map(v => <span key={v} style={{ fontSize: 11, background: C.purpleDim, color: C.purple, border: `1px solid ${C.purple}33`, borderRadius: 4, padding: "2px 7px" }}>{v}</span>)}
        </div>
      )}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <FF label="Output Tag"><input value={output} onChange={e => setOutput(e.target.value)} placeholder="e.g. EEI" /></FF>
        <FF label="Unit"><input value={unit} onChange={e => setUnit(e.target.value)} placeholder="e.g. kW, %, bar" /></FF>
      </div>
      <div style={{ marginTop: 4, display: "flex", justifyContent: "flex-end", gap: 8 }}>
        <button className="btn-ghost" onClick={onClose}>Cancel</button>
        <button onClick={handleSave} disabled={loading}
          style={{ padding: "8px 16px", background: C.purple, color: "white", borderRadius: 6, border: "none", fontFamily: "Sora,sans-serif", fontSize: 12, fontWeight: 500, cursor: loading ? "not-allowed" : "pointer", opacity: loading ? 0.6 : 1 }}>
          {loading && <Spinner />}{isEdit ? "Update Sensor" : "Create Sensor"}
        </button>
      </div>
    </Modal>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  SIGNAL CONFIGURATOR  — loads groups from API, receives status via WS
// ═══════════════════════════════════════════════════════════════════════════

const TYPE_LABEL = { mqtt: "MQTT", influxdb: "InfluxDB", kafka: "Kafka", webhook: "Webhook" };

function SignalConfigurator() {
  const [groups, setGroups] = useState([]);
  const [activeIdx, setActiveIdx] = useState(0);
  const [loading, setLoading] = useState(true);
  const [apiError, setApiError] = useState(null);
  const [srcModal, setSrcModal] = useState(null);
  const [sinkModal, setSinkModal] = useState(null);
  const [saveState, setSaveState] = useState("idle"); // idle | saving | saved | error
  const [deleteState, setDeleteState] = useState("idle"); // idle | saving | saved | error
  const { call } = useApiCall();

  // ── Load groups on mount ──
  useEffect(() => {
    // wsManager.connect();
    (async () => {
      const data = await call(groupsApi.list);
      if (data) setGroups(data);
      setLoading(false);
    })();
    // return () => wsManager.disconnect();
  }, []);

  // ── WebSocket: real-time status updates for sources and sinks ──
  useWsTopic("source.status", ({ source_id, group_id, status }) => {
    setGroups(gs => gs.map(g =>
      g.id === group_id
        ? { ...g, sources: g.sources.map(s => s.id === source_id ? { ...s, status } : s) }
        : g
    ));
  });

  useWsTopic("sink.status", ({ sink_id, group_id, status }) => {
    setGroups(gs => gs.map(g =>
      g.id === group_id
        ? { ...g, sinks: g.sinks.map(s => s.id === sink_id ? { ...s, status } : s) }
        : g
    ));
  });

  const group = groups[activeIdx];

  // ── Mutations ──
  const updateGroupName = async (id, name) => {
    setGroups(gs => gs.map(g => g.id === id ? { ...g, name } : g));
    await call(groupsApi.update, id, { name });
  };

  const addGroup = async () => {
    const ng = await call(groupsApi.create, { name: `Group ${groups.length + 1}` });
    if (ng) { setGroups(g => [...g, ng]); setActiveIdx(groups.length); }
  };

  const deleteGroup = async (id) => {
    await call(groupsApi.delete, id);
    setGroups(gs => gs.filter(g => g.id !== id));
    setActiveIdx(0);
  };

  const onSourceSaved = (src) => {
    setGroups(gs => gs.map((g, i) => i === activeIdx
      ? { ...g, sources: g.sources.find(s => s.id === src.id) ? g.sources.map(s => s.id === src.id ? src : s) : [...g.sources, src] }
      : g
    ));
  };

  const onSinkSaved = (sk) => {
    setGroups(gs => gs.map((g, i) => i === activeIdx
      ? { ...g, sinks: g.sinks.find(s => s.id === sk.id) ? g.sinks.map(s => s.id === sk.id ? sk : s) : [...g.sinks, sk] }
      : g
    ));
  };

  const deleteSource = async (srcId) => {
    await call(sourcesApi.delete, group.id, srcId);
    setGroups(gs => gs.map((g, i) => i === activeIdx ? { ...g, sources: g.sources.filter(s => s.id !== srcId) } : g));
  };

  const deleteSink = async (skId) => {
    await call(sinksApi.delete, group.id, skId);
    setGroups(gs => gs.map((g, i) => i === activeIdx ? { ...g, sinks: g.sinks.filter(s => s.id !== skId) } : g));
  };

  // "Save Group" persists the full group (sources + sinks order) in one call
  const handleSaveGroup = async () => {
    setSaveState("saving");
    const result = await call(groupsApi.update, group.id, {
      name: group.name,
      source_ids: group.sources.map(s => s.id),
      sink_ids: group.sinks.map(s => s.id),
    });
    setSaveState(result ? "saved" : "error");
    setTimeout(() => setSaveState("idle"), 2500);
  };

  const handleDeleteGroup = async () => {
    setDeleteState("saving");
    const result = await call(groupsApi.delete, group.id);
    setGroups(gs => gs.filter(g => g.id !== group.id));
    setActiveIdx(0);
    setDeleteState(result ? "saved" : "error");
    setTimeout(() => setDeleteState("idle"), 2500);
  };

  if (loading) return (
    <div style={{ padding: "60px 0", textAlign: "center", color: C.textMuted }}>
      <Spinner /> Loading groups…
    </div>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      <ErrorBanner msg={apiError} />

      {/* Group tabs */}
      <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
        {groups.map((g, i) => (
          <button key={g.id} onClick={() => setActiveIdx(i)}
            style={{
              padding: "6px 14px", borderRadius: 6, border: `1px solid ${i === activeIdx ? C.accent : C.border}`,
              background: i === activeIdx ? C.accentDim : "transparent", color: i === activeIdx ? C.accent : C.textMuted,
              fontFamily: "Sora,sans-serif", fontSize: 12, cursor: "pointer"
            }}>
            ⬡ {g.name}
          </button>
        ))}
        {/* <button onClick={addGroup}
          style={{
            padding: "6px 12px", borderRadius: 6, border: `1px dashed ${C.border}`, background: "transparent",
            color: C.textMuted, fontFamily: "Sora,sans-serif", fontSize: 12, cursor: "pointer"
          }}>
          ＋ New Group
        </button> */}
      </div>

      {group && (
        <div className="fade-in">
          <Card style={{ marginBottom: 16, padding: "12px 16px" }}>
            <label>Group Name</label>
            <input value={group.name || ""}
              onChange={e => setGroups(gs => gs.map((g, i) => i === activeIdx ? { ...g, name: e.target.value } : g))}
              onBlur={e => updateGroupName(group.id, e.target.value)} />
          </Card>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            {/* Sources column */}
            <div>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: C.textDim, letterSpacing: "0.08em", textTransform: "uppercase" }}>◈ Data Sources</span>
                <button className="btn-ghost" onClick={() => setSrcModal("add")} style={{ fontSize: 11 }}>＋ Add</button>
              </div>
              {(group.sources || []).length === 0 && (
                <div style={{ padding: "20px 0", textAlign: "center", color: C.textMuted, fontSize: 12, border: `1px dashed ${C.border}`, borderRadius: 8 }}>No sources configured</div>
              )}
              {(group.sources || []).map(src => (
                <Card key={src.id} style={{ marginBottom: 8 }}>
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 8 }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 4 }}>{src.protocol?.toUpperCase()}</div>
                      <StatusBadge status={src.status || "offline"} />
                    </div>
                    <div style={{ display: "flex", gap: 4 }}>
                      <button className="btn-ghost" onClick={() => setSrcModal(src)} style={{ padding: "4px 10px", fontSize: 11 }}>✎ Edit</button>
                      <button className="btn-danger" onClick={() => deleteSource(src.id)} style={{ padding: "4px 8px", fontSize: 11 }}>✕</button>
                    </div>
                  </div>
                  <div style={{ fontFamily: "IBM Plex Mono,monospace", fontSize: 11, color: C.textMuted, background: C.bg, borderRadius: 4, padding: "4px 8px", marginBottom: 4 }}>
                    {src.connection?.endpoint || src.connection?.host || "—"}
                  </div>
                  <div style={{ fontSize: 11, color: C.textDim }}>{(src.variables || []).length} variable(s) mapped</div>
                </Card>
              ))}
            </div>

            {/* Sinks column */}
            <div>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                <span style={{ fontSize: 12, fontWeight: 600, color: C.textDim, letterSpacing: "0.08em", textTransform: "uppercase" }}>◉ Data Sinks</span>
                <button className="btn-ghost" onClick={() => setSinkModal("add")} style={{ fontSize: 11 }}>＋ Add</button>
              </div>
              {(group.sinks || []).length === 0 && (
                <div style={{ padding: "20px 0", textAlign: "center", color: C.textMuted, fontSize: 12, border: `1px dashed ${C.border}`, borderRadius: 8 }}>No sinks configured</div>
              )}
              {(group.sinks || []).map(sk => (
                <Card key={sk.id} style={{ marginBottom: 8 }}>
                  <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", marginBottom: 8 }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 4 }}>{TYPE_LABEL[sk.adapter_type] || sk.adapter_type?.toUpperCase()}</div>
                      <StatusBadge status={sk.status || "offline"} />
                    </div>
                    <div style={{ display: "flex", gap: 4 }}>
                      <button className="btn-ghost" onClick={() => setSinkModal(sk)} style={{ padding: "4px 10px", fontSize: 11 }}>✎ Edit</button>
                      <button className="btn-danger" onClick={() => deleteSink(sk.id)} style={{ padding: "4px 8px", fontSize: 11 }}>✕</button>
                    </div>
                  </div>
                  <div style={{ fontFamily: "IBM Plex Mono,monospace", fontSize: 11, color: C.textMuted, background: C.bg, borderRadius: 4, padding: "4px 8px", marginBottom: 4 }}>
                    {sk.config?.broker_url || sk.config?.server_url || sk.config?.bootstrap_servers || sk.config?.endpoint_url || "—"}
                  </div>
                  {sk.config?.topic && <div style={{ fontSize: 11, color: C.textDim }}>Topic: {sk.config.topic}</div>}
                  {sk.config?.bucket && <div style={{ fontSize: 11, color: C.textDim }}>Bucket: {sk.config.bucket} · Org: {sk.config.org}</div>}
                </Card>
              ))}
            </div>
          </div>

          <div style={{ marginTop: 16, display: "flex", justifyContent: "flex-end", alignItems: "center", gap: 12 }}>
            {saveState === "error" && <span style={{ fontSize: 12, color: C.danger }}>⚠ Save failed</span>}
            <button onClick={handleSaveGroup} disabled={saveState === "saving"}
              style={{
                padding: "10px 28px", fontSize: 13,
                background: saveState === "saved" ? "#059669" : C.success,
                color: "#052e1c", fontWeight: 600, borderRadius: 6, border: "none",
                fontFamily: "Sora,sans-serif", cursor: "pointer", transition: "background 0.3s",
                opacity: saveState === "saving" ? 0.7 : 1
              }}>
              {saveState === "saving" ? <><Spinner />Saving…</> : saveState === "saved" ? "✓ Saved!" : "✓ Save Group"}
            </button>
            {/* <button onClick={handleDeleteGroup} disabled={deleteState === "saving"}
              style={{
                padding: "10px 28px", fontSize: 13,
                background: deleteState === "saved" ? "#960505" : C.danger,
                color: "#ffffff", fontWeight: 600, borderRadius: 6, border: "none",
                fontFamily: "Sora,sans-serif", cursor: "pointer", transition: "background 0.3s",
                opacity: deleteState === "saving" ? 0.7 : 1
              }}>
              {deleteState === "saving" ? <><Spinner />Saving…</> : deleteState === "saved" ? "✓ Saved!" : "X Delete Group"}
            </button> */}
          </div>
          
        </div>
      )}

      {srcModal && <SourceModal groupId={group?.id} existing={srcModal === "add" ? null : srcModal} onClose={() => setSrcModal(null)} onSaved={onSourceSaved} />}
      {sinkModal && <SinkModal groupId={group?.id} existing={sinkModal === "add" ? null : sinkModal} onClose={() => setSinkModal(null)} onSaved={onSinkSaved} />}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  SOFT SENSOR BUILDER  — loads sensors from API, gets live values via WS
// ═══════════════════════════════════════════════════════════════════════════

function SoftSensorBuilder() {
  const [sensors, setSensors] = useState([]);
  const [loading, setLoading] = useState(true);
  const [liveValues, setLiveValues] = useState({});
  const [sensorModal, setSensorModal] = useState(null);
  const [tab, setTab] = useState("formula");
  const { call } = useApiCall();


  // Load sensors
  useEffect(() => {
    (async () => {
      const data = await call(sensorsApi.list);
      if (data) setSensors(data);
      setLoading(false);
    })();
  }, []);

  // Live computed values via WebSocket
  useWsTopic("soft_sensor.values", ({ sensor_id, value, timestamp }) => {
    setLiveValues(v => ({ ...v, [sensor_id]: value }));
  });

  // Status updates (e.g. formula error at runtime)
  useWsTopic("soft_sensor.status", ({ sensor_id, status, error_message }) => {
    setSensors(ss => ss.map(s => s.id === sensor_id ? { ...s, status, error_message } : s));
  });

  const onSensorSaved = (s) => {
    setSensors(ss => ss.find(x => x.id === s.id) ? ss.map(x => x.id === s.id ? s : x) : [...ss, s]);
  };

  const delSensor = async (id) => {
    await call(sensorsApi.delete, id);
    setSensors(ss => ss.filter(x => x.id !== id));
  };

  return (
    <div>
      <div style={{ display: "flex", gap: 4, borderBottom: `1px solid ${C.border}`, marginBottom: 20 }}>
        {[["formula", "Formula Builder"], ["mlmodel", "ML Model Proxy"], ["analytics", "Signal Analytics"]].map(([k, l]) => (
        // {[["formula", "Formula Builder"]].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)}
            style={{
              padding: "8px 16px", background: "transparent", border: "none", borderBottom: `2px solid ${tab === k ? C.purple : "transparent"}`,
              color: tab === k ? C.purple : C.textMuted, fontFamily: "Sora,sans-serif", fontSize: 12, cursor: "pointer", marginBottom: -1, transition: "all 0.15s"
            }}>
            {l}
          </button>
        ))}
      </div>

      {tab === "formula" && (
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
            <div>
              <div style={{ fontWeight: 600, fontSize: 14, marginBottom: 2 }}>Formula-based Soft Sensors</div>
              <div style={{ fontSize: 12, color: C.textMuted }}>Derive virtual signals using math expressions on physical inputs</div>
            </div>
            <button className="btn-ghost" onClick={() => setSensorModal("add")} style={{ borderColor: `${C.purple}66`, color: C.purple }}>＋ New Sensor</button>
          </div>

          {loading && <div style={{ padding: "30px 0", textAlign: "center", color: C.textMuted, fontSize: 12 }}><Spinner /> Loading sensors…</div>}

          {!loading && sensors.length === 0 && (
            <div style={{ padding: "30px 0", textAlign: "center", color: C.textMuted, fontSize: 12, border: `1px dashed ${C.border}`, borderRadius: 8 }}>No soft sensors configured</div>
          )}

          {sensors.map(s => (
            <Card key={s.id} style={{ marginBottom: 10 }}>
              <div style={{ display: "flex", gap: 16, alignItems: "flex-start" }}>
                <div style={{ flex: 1 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
                    <span style={{ fontWeight: 600, fontSize: 13 }}>{s.name}</span>
                    <StatusBadge status={s.status || "active"} />
                  </div>
                  {s.error_message && <div style={{ fontSize: 11, color: C.danger, marginBottom: 6 }}>⚠ {s.error_message}</div>}
                  <div style={{
                    fontFamily: "IBM Plex Mono,monospace", fontSize: 12, color: C.accent, background: C.bg,
                    border: `1px solid ${C.border}`, borderRadius: 4, padding: "5px 10px", marginBottom: 8
                  }}>{s.formula}</div>
                  <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                    {(s.input_tags || []).map(v => (
                      <span key={v} style={{ fontSize: 11, background: C.purpleDim, color: C.purple, border: `1px solid ${C.purple}33`, borderRadius: 4, padding: "2px 7px" }}>{v}</span>
                    ))}
                    <span style={{ fontSize: 11, color: C.textMuted }}>→</span>
                    <span style={{ fontSize: 11, background: C.tealDim, color: C.teal, border: `1px solid ${C.teal}33`, borderRadius: 4, padding: "2px 7px" }}>{s.output_tag}</span>
                  </div>
                </div>
                <div style={{ textAlign: "right", minWidth: 90 }}>
                  <div style={{ fontSize: 22, fontFamily: "IBM Plex Mono,monospace", fontWeight: 500, color: C.teal }}>
                    {liveValues[s.id] !== undefined ? Number(liveValues[s.id]).toFixed(3) : (s.last_value ?? "—")}
                  </div>
                  <div style={{ fontSize: 11, color: C.textMuted, marginBottom: 8 }}>{s.unit}</div>
                  <div style={{ display: "flex", gap: 4, justifyContent: "flex-end" }}>
                    <button className="btn-ghost" onClick={() => setSensorModal(s)} style={{ padding: "4px 10px", fontSize: 11 }}>✎ Edit</button>
                    <button className="btn-danger" onClick={() => delSensor(s.id)} style={{ padding: "4px 8px", fontSize: 11 }}>✕</button>
                  </div>
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}

      {tab === "mlmodel" && (
        <div className="fade-in" style={{ textAlign: "center", padding: "40px 20px" }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>⬟</div>
          <div style={{ fontWeight: 600, marginBottom: 8 }}>ML Model Proxy</div>
          <div style={{ color: C.textMuted, fontSize: 13, maxWidth: 400, margin: "0 auto 20px" }}>Connect trained ML models (ONNX, scikit-learn, TensorFlow) and map their outputs as virtual soft sensor signals.</div>
          {/* <button className="btn-ghost">＋ Import Model</button> */}
          <div style={{ color: C.textMuted, fontSize: 13, maxWidth: 400, margin: "0 auto 20px" }}>Coming Soon !</div>
        </div>
      )}

      {tab === "analytics" && (
        <div className="fade-in" style={{ textAlign: "center", padding: "40px 20px" }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>◈</div>
          <div style={{ fontWeight: 600, marginBottom: 8 }}>Signal Analytics</div>
          <div style={{ color: C.textMuted, fontSize: 13, maxWidth: 400, margin: "0 auto 20px" }}>Statistical transforms: rolling averages, FFT, anomaly detection, threshold alerts — applied to any configured signal.</div>
          {/* <button className="btn-ghost">＋ Add Analytics Rule</button> */}
          <div style={{ color: C.textMuted, fontSize: 13, maxWidth: 400, margin: "0 auto 20px" }}>Coming Soon !</div>
        </div>
      )}

      {sensorModal && (
        <SensorModal existing={sensorModal === "add" ? null : sensorModal} onClose={() => setSensorModal(null)} onSaved={onSensorSaved} />
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════
//  APP SHELL  — WebSocket connection indicator in header
// ═══════════════════════════════════════════════════════════════════════════

export default function App() {
  const [section, setSection] = useState("signal");
  const [wsOnline, setWsOnline] = useState(false);

  useEffect(() => {
    const unsub = wsManager.subscribe("__status__", ({ connected }) => setWsOnline(connected));
    wsManager.connect();
    return () => { unsub(); wsManager.disconnect(); };
  }, []);

  return (
    <>
      <style>{css}
        @keyframes spin {"{ from{transform:rotate(0deg)} to{transform:rotate(360deg)} }"}
      </style>

      {/* layout-container */}
      <div style={{ minHeight: "100vh", background: C.bg, display: "flex", flexDirection: "column" }}>

        {/* header */}
        <div style={{ borderBottom: `1px solid ${C.border}`, background: C.surface, padding: "0 24px", display: "flex", alignItems: "center" }}>
          <div style={{ padding: "14px 0", marginRight: 32 }}>
            <span style={{ fontWeight: 700, fontSize: 15, letterSpacing: "-0.02em" }}>Signal</span>
            <span style={{ fontWeight: 300, fontSize: 15, color: C.textMuted }}> Studio</span>
          </div>
          {[["signal", "Signal Configurator"], ["sensor", "Soft Sensor Builder"]].map(([k, l]) => (
            <button key={k} onClick={() => setSection(k)}
              style={{
                padding: "16px 20px", background: "transparent", border: "none",
                borderBottom: `2px solid ${section === k ? C.accent : "transparent"}`,
                color: section === k ? C.text : C.textMuted, fontFamily: "Sora,sans-serif", fontSize: 13,
                cursor: "pointer", fontWeight: section === k ? 500 : 400, transition: "all 0.15s", marginBottom: -1
              }}>
              {l}
            </button>
          ))}
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 11, color: wsOnline ? C.success : C.textMuted, display: "flex", alignItems: "center", gap: 4 }}>
              <span style={{ width: 6, height: 6, borderRadius: "50%", background: wsOnline ? C.success : C.textMuted, display: "inline-block" }} className={wsOnline ? "pulse" : ""} />
              {wsOnline ? "Live" : "Disconnected"}
            </span>
            {/* <StatusBadge status="online" /> */}
            <span style={{ fontSize: 12, color: C.textMuted, fontFamily: "IBM Plex Mono,monospace" }}>v1.0.0</span>
          </div>
        </div>
          
        {/* mid-container */}
        <div style={{ maxWidth: 900, margin: "0 auto", padding: "24px 20px" ,flexGrow: 1, width: "100vh"}}>
          {section === "signal" ? <SignalConfigurator /> : <SoftSensorBuilder />}
        </div>
      
      {/* footer */}
      <Footer />

      </div>
    </>
  );
}