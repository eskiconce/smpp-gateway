import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Snapshot {
  by_state: Record<string, number>;
  by_connector: { connector_id: number; count: number }[];
  today: number;
  last_5min: number;
  generated_at: string;
}

export default function Dashboard() {
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const s = await apiFetch<Snapshot>('/metrics');
        if (alive) { setSnap(s); setError(''); }
      } catch (err) {
        if (alive) setError((err as Error).message);
      }
    };
    load();
    const id = window.setInterval(load, 2000);
    return () => { alive = false; window.clearInterval(id); };
  }, []);

  const stateOrder = ['delivered', 'undeliv', 'expired', 'rejected', 'buffered', 'accepted', 'failed', 'pending'];

  return (
    <div>
      <h1>Dashboard</h1>
      {error && <p className="error">{error}</p>}
      {snap && (
        <>
          <div className="kpis">
            <div className="kpi"><b>{snap.today}</b>Enviados hoy</div>
            <div className="kpi"><b>{snap.last_5min}</b>Ultimos 5 min</div>
            <div className="kpi"><b>{snap.by_state['delivered'] ?? 0}</b>Entregados</div>
            <div className="kpi"><b>{snap.by_state['undeliv'] ?? 0}</b>No entregados</div>
          </div>
          <div className="card">
            <h2>Estado</h2>
            <table>
              <thead><tr><th>Estado</th><th>Cantidad</th></tr></thead>
              <tbody>
                {stateOrder.filter((s) => (snap.by_state[s] ?? 0) > 0).map((s) => (
                  <tr key={s}><td>{s}</td><td>{snap.by_state[s]}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card">
            <h2>Por conector</h2>
            <table>
              <thead><tr><th>Conector</th><th>Enviados hoy</th></tr></thead>
              <tbody>
                {snap.by_connector.map((c) => (
                  <tr key={c.connector_id}><td>{c.connector_id}</td><td>{c.count}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
