import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Webhook { id: number; url: string; active: boolean; events: string[]; }

export default function Webhooks() {
  const [items, setItems] = useState<Webhook[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<Webhook[]>('/admin/webhooks')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Webhooks</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>URL</th><th>Activo</th><th>Eventos</th></tr></thead>
        <tbody>
          {items.map((w) => (
            <tr key={w.id}><td>{w.id}</td><td>{w.url}</td><td>{w.active ? 'Si' : 'No'}</td>
              <td>{w.events.join(', ')}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
