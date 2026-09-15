import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface RateTable { id: number; name: string; active: boolean; }
interface RateEntry { id: number; prefix: string; price: number; connector_id?: number; valid_from?: string; valid_to?: string; }

export default function Rates() {
  const [tenants, setTenants] = useState<{ id: string; name: string }[]>([]);
  const [tenant, setTenant] = useState('');
  const [tables, setTables] = useState<RateTable[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [entries, setEntries] = useState<RateEntry[]>([]);
  const [newTable, setNewTable] = useState('');
  const [newEntry, setNewEntry] = useState({ prefix: '', price: '', connector_id: '' });

  const th: Record<string, string> = tenant ? { 'X-Tenant-ID': tenant } : {};

  const loadTenants = async () => {
    setTenants(await apiFetch<{ id: string; name: string }[]>('/admin/tenants') ?? []);
  };
  const loadTables = async (tid: string) => {
    if (!tid) return;
    const data = await apiFetch<RateTable[]>('/admin/rate-tables', {}, { 'X-Tenant-ID': tid });
    setTables(data ?? []);
  };
  const loadEntries = async (tableId: number) => {
    const data = await apiFetch<RateEntry[]>(`/admin/rate-tables/${tableId}/entries`, {}, th);
    setEntries(data ?? []);
  };

  useEffect(() => { void loadTenants(); }, []);
  useEffect(() => { if (tenant) void loadTables(tenant); }, [tenant]);

  const createTable = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTable || !tenant) return;
    const r = await apiFetch<{ id: number }>('/admin/rate-tables',
      { method: 'POST', body: JSON.stringify({ name: newTable, active: true }) }, th);
    setSelected(r.id);
    setNewTable('');
    await loadTables(tenant);
    await loadEntries(r.id);
  };

  const createEntry = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selected || !newEntry.prefix || !newEntry.price) return;
    await apiFetch(`/admin/rate-tables/${selected}/entries`,
      { method: 'POST', body: JSON.stringify({
        prefix: newEntry.prefix,
        price: Number(newEntry.price),
        connector_id: Number(newEntry.connector_id) || undefined,
      }) }, th);
    setNewEntry({ prefix: '', price: '', connector_id: '' });
    await loadEntries(selected);
  };

  const removeEntry = async (id: number) => {
    await apiFetch(`/admin/rate-entries/${id}`, { method: 'DELETE' });
    if (selected) await loadEntries(selected);
  };

  return (
    <div>
      <h1>Tarifas</h1>
      <div className="toolbar">
        <label>Tenant{' '}
          <select value={tenant} onChange={(e) => { setTenant(e.target.value); setSelected(null); setEntries([]); }}>
            <option value="">Seleccionar tenant</option>
            {tenants.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </label>
      </div>
      {tenant && (
        <div className="card">
          <form onSubmit={createTable} className="toolbar">
            <input placeholder="nombre de tabla" value={newTable}
              onChange={(e) => setNewTable(e.target.value)} />
            <button type="submit">Nueva tabla</button>
          </form>
          {tables.map((t) => (
            <div key={t.id} className="toolbar">
              <button onClick={() => { setSelected(t.id); void loadEntries(t.id); }}>
                {t.name}{t.active ? '' : ' (inactiva)'}
              </button>
            </div>
          ))}
        </div>
      )}
      {selected && (
        <div className="card">
          <h2>Entradas de tabla {selected}</h2>
          <form onSubmit={createEntry} className="toolbar">
            <input placeholder="prefix msisdn" value={newEntry.prefix}
              onChange={(e) => setNewEntry({ ...newEntry, prefix: e.target.value })} required />
            <input placeholder="precio" value={newEntry.price}
              onChange={(e) => setNewEntry({ ...newEntry, price: e.target.value })} required />
            <input placeholder="connector_id (opc)" value={newEntry.connector_id}
              onChange={(e) => setNewEntry({ ...newEntry, connector_id: e.target.value })} />
            <button type="submit">Agregar</button>
          </form>
          <table>
            <thead><tr><th>Prefix</th><th>Precio</th><th>Conector</th><th>Vigencia</th><th></th></tr></thead>
            <tbody>
              {entries.map((en) => (
                <tr key={en.id}>
                  <td>{en.prefix}</td><td>{en.price}</td><td>{en.connector_id ?? '-'}</td>
                  <td>{en.valid_from ?? ''} {en.valid_to ? `\u2192 ${en.valid_to}` : ''}</td>
                  <td><button onClick={() => void removeEntry(en.id)}>Eliminar</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
