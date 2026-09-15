import { useEffect, useState } from 'react';
import { apiFetch } from '../api';
import { ConnectorOption } from './shared';

interface Group {
  id: number; name: string; members: { connector_id: number; weight: number }[];
}

export default function Groups() {
  const [items, setItems] = useState<Group[]>([]);
  const [connectors, setConnectors] = useState<ConnectorOption[]>([]);
  const [name, setName] = useState('');

  const loadGroups = async () => {
    setItems(await apiFetch<Group[]>('/admin/groups') ?? []);
  };
  const loadConnectors = async () => {
    setConnectors(await apiFetch<ConnectorOption[]>('/admin/connectors') ?? []);
  };
  useEffect(() => { void loadGroups(); void loadConnectors(); }, []);

  const createGroup = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name) return;
    await apiFetch('/admin/groups', { method: 'POST', body: JSON.stringify({ name }) });
    setName('');
    await loadGroups();
  };

  const removeGroup = async (id: number) => {
    await apiFetch(`/admin/groups/${id}`, { method: 'DELETE' });
    await loadGroups();
  };

  const saveMembers = async (g: Group, cid: number, weight: number) => {
    const members = g.members
      .filter((m) => m.connector_id !== cid || weight > 0)
      .concat(weight > 0 ? [{ connector_id: cid, weight }] : []);
    await apiFetch(`/admin/groups/${g.id}/members`, {
      method: 'PUT', body: JSON.stringify({ members }),
    });
    await loadGroups();
  };

  return (
    <div>
      <h1>Grupos</h1>
      <div className="card">
        <form onSubmit={createGroup} className="toolbar">
          <input placeholder="nombre del grupo" value={name}
            onChange={(e) => setName(e.target.value)} />
          <button type="submit">Crear</button>
        </form>
      </div>
      {items.length === 0 && <p>Sin grupos. Crea uno para balancear conectores.</p>}
      {items.map((g) => (
        <div className="card" key={g.id}>
          <h2>{g.name} <button onClick={() => void removeGroup(g.id)}>Eliminar</button></h2>
          {connectors.map((c) => {
            const m = g.members.find((x) => x.connector_id === c.id);
            return (
              <div key={c.id} className="toolbar">
                <span style={{ width: 200 }}>{c.name}</span>
                <input type="number" min={0} placeholder={m ? String(m.weight) : 'peso (0 = fuera)'}
                  onBlur={(e) => void saveMembers(g, c.id, Number(e.target.value) || 0)} />
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}
