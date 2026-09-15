import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Tenant { id: string; name: string; status: string; balance: number; mode: string; }

export default function Tenants() {
  const [items, setItems] = useState<Tenant[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<Tenant[]>('/admin/tenants')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Tenants</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Estado</th><th>Saldo</th><th>Modo</th></tr></thead>
        <tbody>
          {items.map((t) => (
            <tr key={t.id}><td>{t.id}</td><td>{t.name}</td><td>{t.status}</td>
              <td>{t.balance}</td><td>{t.mode}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
