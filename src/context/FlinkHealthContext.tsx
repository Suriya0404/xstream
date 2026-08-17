import { createContext, useContext } from 'react';
import type { FlinkHealth } from '../hooks/useFlinkHeartbeat';

export const FlinkHealthContext = createContext<FlinkHealth>('unknown');
export const useFlinkHealth = () => useContext(FlinkHealthContext);
