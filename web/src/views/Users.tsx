import { useEffect, useState } from 'react';
import { apiFetch } from '../api';

interface User { id: number; username: string; role: string; tenant_id: string; }

export default function Users() {
  const [items, setItems] = useState<User[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    apiFetch<User[]>('/admin/users')
      .then(setItems)
      .catch((e) => setError((e as Error).message));
  }, []);

  return (
    <div>
      <h1>Usuarios</h1>
      {error && <p className="error">{error}</p>}
      <table>
        <thead><tr><th>ID</th><th>Usuario</th><th>Rol</th><th>Tenant</th></tr></thead>
        <tbody>
          {items.map((u) => (
            <tr key={u.id}><td>{u.id}</td><td>{u.username}</td><td>{u.role}</td>
              <td>{u.tenant_id}</td></tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
