import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Webhook {
  id: number; tenant_id?: string; url: string; auth_token?: string;
  events?: string[]; active?: boolean;
}

export default function Webhooks() {
  const [items, setItems] = useState<Webhook[]>([]);
  const [form, setForm] = useState({ url: '', auth_token: '', events: 'DLR', active: true, tenant_id: '' });

  const load = async () => {
    const data = await apiFetch<Webhook[]>('/admin/webhooks');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    await apiFetch('/admin/webhooks', {
      method: 'POST',
      body: JSON.stringify({
        url: form.url,
        auth_token: form.auth_token,
        events: form.events.split(',').map((s) => s.trim()).filter(Boolean),
        active: form.active,
        tenant_id: form.tenant_id || undefined,
      }),
    });
    setForm({ url: '', auth_token: '', events: 'DLR', active: true, tenant_id: '' });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/webhooks/${id}`, { method: 'DELETE' });
    await load();
  };

  return (
    <div>
      <h1>Webhooks</h1>
      <div className="card">
        <h2>Nuevo webhook</h2>
        <form onSubmit={create} className="toolbar">
          <input placeholder="url" value={form.url}
            onChange={(e) => setForm({ ...form, url: e.target.value })} required />
          <input placeholder="auth_token (opc)" value={form.auth_token}
            onChange={(e) => setForm({ ...form, auth_token: e.target.value })} />
          <input placeholder="events (comma-sep, ej: DLR)" value={form.events}
            onChange={(e) => setForm({ ...form, events: e.target.value })} />
          <input placeholder="tenant_id (opc)" value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })} />
          <label><input type="checkbox" checked={form.active}
            onChange={(e) => setForm({ ...form, active: e.target.checked })} /> Activo</label>
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Tenant</th><th>URL</th><th>Eventos</th><th>Activo</th><th></th></tr></thead>
        <tbody>
          {items.map((w) => (
            <tr key={w.id}>
              <td>{w.id}</td><td>{w.tenant_id}</td><td>{w.url}</td>
              <td>{w.events?.join(', ')}</td><td>{w.active ? 'Si' : 'No'}</td>
              <td><button onClick={() => void remove(w.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
