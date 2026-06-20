import { createContext, useContext } from 'react';
import type { EditableNodeData, ConnectedSource } from '../components/NodeEditModal';

interface NodeEditCtx {
  openPanel: (nodeId: string, data: EditableNodeData, connectedSources: ConnectedSource[]) => void;
}

export const NodeEditContext = createContext<NodeEditCtx>({ openPanel: () => {} });
export const useNodeEdit = () => useContext(NodeEditContext);
