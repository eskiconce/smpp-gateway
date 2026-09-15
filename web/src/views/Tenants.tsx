import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Tenant {
  id: string; name: string; status: string; routing_tag?: string;
  balance?: number; mode?: string; api_key?: string;
}

export default function Tenants() {
  const [items, setItems] = useState<Tenant[]>([]);
  const [form, setForm] = useState({ id: '', name: '', mode: 'prepaid', api_key: '' });
  const [credit, setCredit] = useState<Record<string, string>>({});

  const load = async () => {
    const data = await apiFetch<Tenant[]>('/admin/tenants');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    await apiFetch('/admin/tenants', { method: 'POST', body: JSON.stringify(form) });
    setForm({ id: '', name: '', mode: 'prepaid', api_key: '' });
    await load();
  };

  const doCredit = async (id: string) => {
    const amount = Number(credit[id] || '0');
    if (!amount) return;
    await apiFetch(`/admin/tenants/${id}/credit`, {
      method: 'POST', body: JSON.stringify({ amount }),
    });
    setCredit({ ...credit, [id]: '' });
    await load();
  };

  return (
    <div>
      <h1>Tenants</h1>
      <div className="card">
        <h2>Nuevo tenant</h2>
        <form onSubmit={create} className="toolbar">
          <input placeholder="id (slug)" value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })} required />
          <input placeholder="nombre" value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <select value={form.mode} onChange={(e) => setForm({ ...form, mode: e.target.value })}>
            <option value="prepaid">prepaid</option>
            <option value="postpaid">postpaid</option>
          </select>
          <input placeholder="api_key" value={form.api_key}
            onChange={(e) => setForm({ ...form, api_key: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Estado</th><th>Saldo</th>
          <th>Modo</th><th>Credito</th><th>Api key</th></tr></thead>
        <tbody>
          {items.map((t) => (
            <tr key={t.id}>
              <td>{t.id}</td><td>{t.name}</td><td>{t.status}</td><td>{t.balance}</td>
              <td>{t.mode}</td>
              <td>
                <input value={credit[t.id] || ''} placeholder="monto" style={{ width: 80 }}
                  onChange={(e) => setCredit({ ...credit, [t.id]: e.target.value })} />
                <button onClick={() => void doCredit(t.id)}>Abonar</button>
              </td>
              <td>{t.api_key}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
