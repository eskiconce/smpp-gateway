import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Rule {
  id: number; priority: number; tenant_id?: string; from?: string;
  prefix?: string; regex?: string; routing_tag?: string;
  connector_id?: number; group_id?: number;
}

export default function Rules() {
  const [items, setItems] = useState<Rule[]>([]);
  const [form, setForm] = useState({
    priority: 1, tenant_id: '', from: '', prefix: '', regex: '',
    routing_tag: '', connector_id: 0, group_id: 0,
  });
  const [dragIdx, setDragIdx] = useState<number | null>(null);

  const load = async () => {
    const data = await apiFetch<Rule[]>('/admin/routing-rules');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.connector_id && !form.group_id) return;
    await apiFetch('/admin/routing-rules', { method: 'POST', body: JSON.stringify(form) });
    setForm({ priority: 1, tenant_id: '', from: '', prefix: '', regex: '',
      routing_tag: '', connector_id: 0, group_id: 0 });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/routing-rules/${id}`, { method: 'DELETE' });
    await load();
  };

  const onDragStart = (idx: number) => setDragIdx(idx);
  const onDragOver = (e: React.DragEvent) => e.preventDefault();
  const onDrop = async (targetIdx: number) => {
    if (dragIdx === null || dragIdx === targetIdx) return;
    const newItems = [...items];
    const [moved] = newItems.splice(dragIdx, 1);
    newItems.splice(targetIdx, 0, moved);
    setItems(newItems);
    setDragIdx(null);
    for (let i = 0; i < newItems.length; i++) {
      if (newItems[i].priority !== i + 1) {
        await apiFetch(`/admin/routing-rules/${newItems[i].id}/priority`, {
          method: 'PUT', body: JSON.stringify({ priority: i + 1 }),
        });
      }
    }
    await load();
  };

  return (
    <div>
      <h1>Reglas de Ruteo</h1>
      <div className="card">
        <h2>Nueva regla</h2>
        <form onSubmit={create} className="toolbar">
          <input type="number" placeholder="prioridad" value={form.priority}
            onChange={(e) => setForm({ ...form, priority: Number(e.target.value) })} />
          <input placeholder="tenant_id" value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })} />
          <input placeholder="from" value={form.from}
            onChange={(e) => setForm({ ...form, from: e.target.value })} />
          <input placeholder="prefix" value={form.prefix}
            onChange={(e) => setForm({ ...form, prefix: e.target.value })} />
          <input placeholder="regex" value={form.regex}
            onChange={(e) => setForm({ ...form, regex: e.target.value })} />
          <input type="number" placeholder="connector_id" value={form.connector_id || ''}
            onChange={(e) => setForm({ ...form, connector_id: Number(e.target.value) })} />
          <input type="number" placeholder="group_id" value={form.group_id || ''}
            onChange={(e) => setForm({ ...form, group_id: Number(e.target.value) })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <p>Arrastra para reordenar prioridad</p>
      <table>
        <thead><tr><th>Prio</th><th>ID</th><th>Tenant</th><th>From</th><th>Prefix</th>
          <th>Regex</th><th>Conector</th><th>Grupo</th><th></th></tr></thead>
        <tbody>
          {items.map((r, idx) => (
            <tr key={r.id} draggable onDragStart={() => onDragStart(idx)}
              onDragOver={onDragOver} onDrop={() => void onDrop(idx)}
              className="drag-row">
              <td>{r.priority}</td><td>{r.id}</td><td>{r.tenant_id}</td>
              <td>{r.from}</td><td>{r.prefix}</td><td>{r.regex}</td>
              <td>{r.connector_id}</td><td>{r.group_id}</td>
              <td><button onClick={() => void remove(r.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
