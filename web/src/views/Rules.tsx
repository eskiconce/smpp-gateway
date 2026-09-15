import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Rule { id: number; priority: number; tenant_id: string; prefix: string; regex: string; connector_id: number; group_id: number; }

export default function Rules() {
  const [items, setItems] = useState<Rule[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<Rule[]>('/admin/routing-rules')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Reglas de Ruteo</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Prioridad</th><th>Tenant</th><th>Prefix</th><th>Regex</th><th>Conector</th><th>Grupo</th></tr></thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.id}><td>{r.id}</td><td>{r.priority}</td><td>{r.tenant_id}</td>
              <td>{r.prefix}</td><td>{r.regex}</td><td>{r.connector_id}</td><td>{r.group_id}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
