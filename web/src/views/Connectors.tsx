import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Connector { id: number; name: string; type: string; host: string; port: number; enabled: boolean; }

export default function Connectors() {
  const [items, setItems] = useState<Connector[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<Connector[]>('/admin/connectors')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Conectores</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Tipo</th><th>Host</th><th>Puerto</th><th>Habilitado</th></tr></thead>
        <tbody>
          {items.map((c) => (
            <tr key={c.id}><td>{c.id}</td><td>{c.name}</td><td>{c.type}</td>
              <td>{c.host}</td><td>{c.port}</td><td>{c.enabled ? 'Si' : 'No'}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
