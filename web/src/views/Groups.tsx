import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Group { id: number; name: string; members: { connector_id: number; weight: number }[]; }

export default function Groups() {
  const [items, setItems] = useState<Group[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<Group[]>('/admin/groups')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Grupos</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Miembros</th></tr></thead>
        <tbody>
          {items.map((g) => (
            <tr key={g.id}><td>{g.id}</td><td>{g.name}</td>
              <td>{g.members.map((m) => `${m.connector_id}:${m.weight}`).join(', ')}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
