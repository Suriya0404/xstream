import { useState } from 'react';
import { ThemeProvider } from './context/ThemeContext';
import WorkflowList from './components/WorkflowList';
import Workspace from './components/Workspace';
import MonitorPanel from './components/MonitorPanel';

type View = 'home' | 'workspace' | 'monitor';

export default function App() {
  const [view, setView] = useState<View>('home');
  const [pipelineName, setPipelineName] = useState('');

  return (
    <ThemeProvider>
      {view === 'home' ? (
        <WorkflowList
          onOpen={name => { setPipelineName(name); setView('workspace'); }}
          onNew={() => { setPipelineName(''); setView('workspace'); }}
          onMonitor={() => setView('monitor')}
        />
      ) : view === 'monitor' ? (
        <MonitorPanel onBack={() => setView('home')} />
      ) : (
        <Workspace
          initialPipelineName={pipelineName}
          onHome={() => setView('home')}
        />
      )}
    </ThemeProvider>
  );
}
