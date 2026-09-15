import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface Msg {
  id: string; tenant_id?: string; source_addr: string; msisdn: string;
  text: string; segments: number; connector_id?: number; state: string;
  try_count?: number; smsc_msgid?: string; amount?: number;
  created_at: string; updated_at?: string;
}

export default function Messages() {
  const [filters, setFilters] = useState({ msisdn: '', state: '', limit: '25' });
  const [total, setTotal] = useState(0);
  const [items, setItems] = useState<Msg[]>([]);
  const [error, setError] = useState('');

  const search = async (offset = 0) => {
    setError('');
    try {
      const params = new URLSearchParams();
      if (filters.msisdn) params.set('msisdn', filters.msisdn);
      if (filters.state) params.set('state', filters.state);
      params.set('limit', filters.limit || '25');
      params.set('offset', String(offset));
      const data = await apiFetch<{ total: number; items: Msg[] }>(`/admin/messages?${params}`);
      setItems(data.items);
      setTotal(data.total);
    } catch (err) {
      setError((err as Error).message);
    }
  };

  useEffect(() => { void search(0); }, []);

  return (
    <div>
      <h1>Mensajes</h1>
      <div className="toolbar">
        <input value={filters.msisdn} placeholder="msisdn"
          onChange={(e) => setFilters({ ...filters, msisdn: e.target.value })} />
        <input value={filters.state} placeholder="estado"
          onChange={(e) => setFilters({ ...filters, state: e.target.value })} />
        <select value={filters.limit}
          onChange={(e) => setFilters({ ...filters, limit: e.target.value })}>
          <option value="10">10</option>
          <option value="25">25</option>
          <option value="50">50</option>
        </select>
        <button onClick={() => void search(0)}>Buscar</button>
      </div>
      <p>Total: {total}</p>
      {error && <p className="error">{error}</p>}
      <table>
        <thead>
          <tr><th>ID</th><th>Tenant</th><th>MSISDN</th><th>Estado</th>
              <th>Seg</th><th>Conector</th><th>SmscMsgid</th><th>Fecha</th></tr>
        </thead>
        <tbody>
          {items.map((m) => (
            <tr key={m.id}>
              <td>{m.id}</td><td>{m.tenant_id}</td><td>{m.msisdn}</td>
              <td>{m.state}</td><td>{m.segments}</td><td>{m.connector_id}</td>
              <td>{m.smsc_msgid}</td><td>{new Date(m.created_at).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {total > (filters.limit ? Number(filters.limit) : 25) && (
        <button onClick={() => void search(Number(filters.limit))}>Siguiente</button>
      )}
    </div>
  );
}
