import { useCallback, useEffect, useRef, useState } from 'react';
import type { Node, Edge } from 'reactflow';
import { FiMessageSquare, FiSend, FiX, FiMinus, FiZap } from 'react-icons/fi';
import type { TurboNodeData } from './TurboNode';
import FunctionIcon from './FunctionIcon';

const BACKEND = 'http://localhost:8000';

interface Message {
  role: 'user' | 'assistant';
  content: string;
}

interface Action {
  type: 'add_node' | 'delete_node' | 'add_edge' | 'delete_edge' | 'update_node';
  id?: string;
  node?: any;
  edge?: any;
  label?: string;
  properties?: Record<string, any>;
  handles?: string[][];
}

interface Props {
  nodes: Node<TurboNodeData>[];
  edges: Edge[];
  setNodes: React.Dispatch<React.SetStateAction<Node<TurboNodeData>[]>>;
  setEdges: React.Dispatch<React.SetStateAction<Edge[]>>;
  pipelineName: string;
}

function parseActions(text: string): { clean: string; actions: Action[] } {
  const match = text.match(/<actions>([\s\S]*?)<\/actions>/);
  if (!match) return { clean: text, actions: [] };
  try {
    const actions: Action[] = JSON.parse(match[1].trim());
    const clean = text.replace(/<actions>[\s\S]*?<\/actions>/, '').trim();
    return { clean, actions };
  } catch {
    return { clean: text, actions: [] };
  }
}

export default function ChatAgent({ nodes, edges, setNodes, setEdges, pipelineName }: Props) {
  const [open, setOpen]         = useState(false);
  const [minimised, setMinimised] = useState(false);
  const [messages, setMessages]  = useState<Message[]>([]);
  const [input, setInput]        = useState('');
  const [streaming, setStreaming] = useState(false);
  const bottomRef  = useRef<HTMLDivElement>(null);
  const inputRef   = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  useEffect(() => {
    if (open && !minimised) inputRef.current?.focus();
  }, [open, minimised]);

  const applyActions = useCallback((actions: Action[]) => {
    for (const action of actions) {
      if (action.type === 'add_node' && action.node) {
        const n = action.node;
        setNodes(prev => [...prev, {
          id:       n.id,
          type:     'turbo',
          position: { x: n.pos_x ?? 200, y: n.pos_y ?? 200 },
          data: {
            icon:       <FunctionIcon />,
            title:      n.label,
            nodeType:   n.node_type,
            handles:    n.handles ?? [['field', 'STRING']],
            properties: n.properties ?? {},
            flinkSql:   '',
          },
        }]);
      } else if (action.type === 'delete_node' && action.id) {
        const id = action.id;
        setNodes(prev => prev.filter(n => n.id !== id));
        setEdges(prev => prev.filter(e => e.source !== id && e.target !== id));
      } else if (action.type === 'add_edge' && action.edge) {
        const e = action.edge;
        setEdges(prev => [...prev, {
          id:          e.id,
          source:      e.source_id,
          target:      e.target_id,
          sourceHandle: e.source_handle ?? 'out-0',
          targetHandle: e.target_handle ?? 'in-0',
          type:        'turbo',
          markerEnd:   'edge-circle',
          markerStart: 'edge-circle',
        }]);
      } else if (action.type === 'delete_edge' && action.id) {
        const id = action.id;
        setEdges(prev => prev.filter(e => e.id !== id));
      } else if (action.type === 'update_node' && action.id) {
        const id = action.id;
        setNodes(prev => prev.map(n => {
          if (n.id !== id) return n;
          return {
            ...n,
            data: {
              ...n.data,
              ...(action.label     ? { title: action.label }                   : {}),
              ...(action.handles   ? { handles: action.handles }               : {}),
              ...(action.properties ? { properties: action.properties }        : {}),
            },
          };
        }));
      }
    }
  }, [setNodes, setEdges]);

  const send = useCallback(async () => {
    const text = input.trim();
    if (!text || streaming) return;
    setInput('');

    const userMsg: Message = { role: 'user', content: text };
    const history = [...messages, userMsg];
    setMessages(history);
    setStreaming(true);

    const workflow = {
      nodes: nodes.map(n => ({
        id:         n.id,
        node_type:  n.data.nodeType ?? 'kafka',
        label:      n.data.title,
        pos_x:      n.position.x,
        pos_y:      n.position.y,
        properties: { ...(n.data.properties ?? {}), _handles: n.data.handles },
      })),
      edges: edges.map(e => ({
        id:        e.id,
        source_id: e.source,
        target_id: e.target,
      })),
    };

    let accumulated = '';
    const assistantIdx = history.length;
    setMessages(prev => [...prev, { role: 'assistant', content: '' }]);

    try {
      const res = await fetch(`${BACKEND}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history, workflow }),
      });

      const reader = res.body!.getReader();
      const decoder = new TextDecoder();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value);
        for (const line of chunk.split('\n')) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6);
          if (data === '[DONE]') break;
          try {
            accumulated += JSON.parse(data).text ?? '';
            setMessages(prev => prev.map((m, i) =>
              i === assistantIdx ? { ...m, content: accumulated } : m
            ));
          } catch { /* ignore parse errors on partial chunks */ }
        }
      }

      const { clean, actions } = parseActions(accumulated);
      if (actions.length > 0) {
        setMessages(prev => prev.map((m, i) =>
          i === assistantIdx ? { ...m, content: clean } : m
        ));
        applyActions(actions);
      }
    } catch (err) {
      setMessages(prev => prev.map((m, i) =>
        i === assistantIdx
          ? { ...m, content: `Error: ${String(err)}` }
          : m
      ));
    } finally {
      setStreaming(false);
    }
  }, [input, messages, streaming, nodes, edges, applyActions]);

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
  };

  return (
    <div className="chat-agent-root">
      {/* Floating toggle button */}
      {!open && (
        <button className="chat-fab" onClick={() => { setOpen(true); setMinimised(false); }} title="Open AI assistant">
          <FiMessageSquare size={22} />
        </button>
      )}

      {/* Chat panel */}
      {open && (
        <div className={`chat-panel${minimised ? ' chat-minimised' : ''}`}>
          {/* Header */}
          <div className="chat-header">
            <span className="chat-header-title">
              <FiZap size={13} className="chat-zap" />
              Claude Assistant
              {pipelineName && <span className="chat-pipeline-label">{pipelineName}</span>}
            </span>
            <div className="chat-header-btns">
              <button onClick={() => setMinimised(v => !v)} title={minimised ? 'Expand' : 'Minimise'}>
                <FiMinus size={13} />
              </button>
              <button onClick={() => setOpen(false)} title="Close">
                <FiX size={13} />
              </button>
            </div>
          </div>

          {!minimised && (
            <>
              {/* Messages */}
              <div className="chat-messages">
                {messages.length === 0 && (
                  <div className="chat-empty">
                    Ask me to add nodes, connect topics, rename fields, or explain the pipeline.
                  </div>
                )}
                {messages.map((m, i) => (
                  <div key={i} className={`chat-bubble chat-bubble-${m.role}`}>
                    {m.content || <span className="chat-cursor" />}
                  </div>
                ))}
                <div ref={bottomRef} />
              </div>

              {/* Input */}
              <div className="chat-input-row">
                <textarea
                  ref={inputRef}
                  className="chat-input nodrag"
                  rows={2}
                  placeholder="Ask Claude to modify the pipeline…"
                  value={input}
                  onChange={e => setInput(e.target.value)}
                  onKeyDown={onKey}
                  disabled={streaming}
                />
                <button
                  className={`chat-send${streaming ? ' chat-send-busy' : ''}`}
                  onClick={send}
                  disabled={streaming || !input.trim()}
                  title="Send (Enter)"
                >
                  <FiSend size={15} />
                </button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
