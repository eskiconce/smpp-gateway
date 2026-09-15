import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Connector {
  id: number; name: string; type: 'smpp' | 'http'; host: string; port: number;
  system_id?: string; password?: string; bind_mode?: string; concurrency?: number;
  max_msg_per_sec?: number; enquire_link_interval?: number; tls?: boolean; enabled?: boolean;
}

const EMPTY: Connector = {
  id: 0, name: '', type: 'smpp', host: '', port: 2775, system_id: '',
  password: '', bind_mode: 'transceiver', concurrency: 1, max_msg_per_sec: 0,
  enquire_link_interval: 30, tls: false, enabled: true,
};

export default function Connectors() {
  const [items, setItems] = useState<Connector[]>([]);
  const [form, setForm] = useState<Connector>({ ...EMPTY });
  const [tests, setTests] = useState<Record<string, string>>({});

  const load = async () => {
    const data = await apiFetch<Connector[]>('/admin/connectors');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    if (form.id) {
      await apiFetch(`/admin/connectors/${form.id}`, {
        method: 'PUT', body: JSON.stringify(form),
      });
    } else {
      await apiFetch('/admin/connectors', { method: 'POST', body: JSON.stringify(form) });
    }
    setForm({ ...EMPTY });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/connectors/${id}`, { method: 'DELETE' });
    await load();
  };

  const testConn = async (id: number) => {
    try {
      const r = await apiFetch<{ ok: boolean; detail: string }>(
        `/admin/connectors/${id}/test`, { method: 'POST' });
      setTests({ ...tests, [id]: `${r.ok ? 'OK' : 'FALLO'}: ${r.detail}` });
    } catch (err) {
      setTests({ ...tests, [id]: (err as Error).message });
    }
  };

  return (
    <div>
      <h1>Conectores</h1>
      <div className="card">
        <h2>{form.id ? 'Editar conector' : 'Nuevo conector'}</h2>
        <form onSubmit={save} className="toolbar">
          <input placeholder="nombre" value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <select value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value as 'smpp' | 'http' })}>
            <option value="smpp">SMPP</option>
            <option value="http">HTTP</option>
          </select>
          <input placeholder="host" value={form.host}
            onChange={(e) => setForm({ ...form, host: e.target.value })} required />
          <input type="number" placeholder="port" value={form.port}
            onChange={(e) => setForm({ ...form, port: Number(e.target.value) })} />
          <input placeholder="system_id" value={form.system_id}
            onChange={(e) => setForm({ ...form, system_id: e.target.value })} />
          <input placeholder="password" type="password" value={form.password || ''}
            onChange={(e) => setForm({ ...form, password: e.target.value })} />
          <input type="number" placeholder="msg/s" value={form.max_msg_per_sec}
            onChange={(e) => setForm({ ...form, max_msg_per_sec: Number(e.target.value) })} />
          <button type="submit">Guardar</button>
          {form.id ? <button type="button" onClick={() => setForm({ ...EMPTY })}>Cancelar</button> : null}
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Nombre</th><th>Tipo</th><th>Direccion</th>
          <th>System ID</th><th>msg/s</th><th>Activo</th><th>Acciones</th></tr></thead>
        <tbody>
          {items.map((c) => (
            <tr key={c.id}>
              <td>{c.id}</td><td>{c.name}</td><td>{c.type}</td>
              <td>{c.host}:{c.port}</td><td>{c.system_id}</td>
              <td>{c.max_msg_per_sec}</td><td>{c.enabled ? 'si' : 'no'}</td>
              <td>
                <button onClick={() => { setForm({ ...c, password: '' }); }}>Editar</button>
                <button onClick={() => void testConn(c.id)}>Test</button>
                <button onClick={() => void remove(c.id)}>Eliminar</button>
                {tests[c.id] && <span> {tests[c.id]}</span>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
