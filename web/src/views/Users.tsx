import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface User { id: number; username: string; role: string; tenant_id: string; }

export default function Users() {
  const [items, setItems] = useState<User[]>([]);
  const [form, setForm] = useState({ username: '', password: '', role: 'viewer', tenant_id: '' });

  const load = async () => {
    const data = await apiFetch<User[]>('/admin/users');
    setItems(data ?? []);
  };
  useEffect(() => { void load(); }, []);

  const create = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.username || !form.password) return;
    await apiFetch('/admin/users', {
      method: 'POST',
      body: JSON.stringify({
        username: form.username,
        password: form.password,
        role: form.role,
        tenant_id: form.tenant_id || undefined,
      }),
    });
    setForm({ username: '', password: '', role: 'viewer', tenant_id: '' });
    await load();
  };

  const remove = async (id: number) => {
    await apiFetch(`/admin/users/${id}`, { method: 'DELETE' });
    await load();
  };

  return (
    <div>
      <h1>Usuarios</h1>
      <div className="card">
        <h2>Nuevo usuario</h2>
        <form onSubmit={create} className="toolbar">
          <input placeholder="username" value={form.username}
            onChange={(e) => setForm({ ...form, username: e.target.value })} required />
          <input placeholder="password" type="password" value={form.password}
            onChange={(e) => setForm({ ...form, password: e.target.value })} required />
          <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
            <option value="superadmin">superadmin</option>
            <option value="admin">admin</option>
            <option value="viewer">viewer</option>
          </select>
          <input placeholder="tenant_id (opc)" value={form.tenant_id}
            onChange={(e) => setForm({ ...form, tenant_id: e.target.value })} />
          <button type="submit">Crear</button>
        </form>
      </div>
      <table>
        <thead><tr><th>ID</th><th>Usuario</th><th>Rol</th><th>Tenant</th><th></th></tr></thead>
        <tbody>
          {items.map((u) => (
            <tr key={u.id}>
              <td>{u.id}</td><td>{u.username}</td><td>{u.role}</td><td>{u.tenant_id}</td>
              <td><button onClick={() => void remove(u.id)}>Eliminar</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
