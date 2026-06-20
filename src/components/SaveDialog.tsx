import { useEffect, useRef, useState } from 'react';
import { FiSave } from 'react-icons/fi';

interface Props {
  initialName: string;
  onSave: (name: string) => void;
  onCancel: () => void;
}

export default function SaveDialog({ initialName, onSave, onCancel }: Props) {
  const [name, setName] = useState(initialName);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
    inputRef.current?.select();
  }, []);

  const submit = () => {
    const trimmed = name.trim();
    if (trimmed) onSave(trimmed);
  };

  return (
    <div className="dialog-backdrop" onClick={onCancel}>
      <div className="dialog-box" onClick={e => e.stopPropagation()}>
        <h3 className="dialog-title">Save Workflow</h3>
        <p className="dialog-hint">Give this workflow a name.</p>
        <input
          ref={inputRef}
          className="dialog-input"
          value={name}
          onChange={e => setName(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') submit();
            if (e.key === 'Escape') onCancel();
          }}
          placeholder="e.g. orders-to-clickhouse"
        />
        <div className="dialog-actions">
          <button
            className="toolbar-btn"
            style={{ background: 'transparent', border: '1px solid #333', color: '#888' }}
            onClick={onCancel}
          >
            Cancel
          </button>
          <button className="toolbar-btn" onClick={submit} disabled={!name.trim()}>
            <FiSave size={13} /> Save
          </button>
        </div>
      </div>
    </div>
  );
}
