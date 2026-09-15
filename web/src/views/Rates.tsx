import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface RateTable { id: number; name: string; active: boolean; }

export default function Rates() {
  const [items, setItems] = useState<RateTable[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<RateTable[]>('/admin/rate-tables')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Tarifas</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Activa</th></tr></thead>
        <tbody>
          {items.map((t) => (
            <tr key={t.id}><td>{t.id}</td><td>{t.name}</td><td>{t.active ? 'Si' : 'No'}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
