import { useState } from 'react';
import { login } from '../api';

export default function Login() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    try {
      await login(username, password);
      location.hash = '#/dashboard';
      location.reload();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  return (
    <div className="login">
      <form onSubmit={submit}>
        <h1>smppgw — Admin</h1>
        <input value={username} onChange={(e) => setUsername(e.target.value)}
          placeholder="usuario" autoFocus />
        <input type="password" value={password} onChange={(e) => setPassword(e.target.value)}
          placeholder="contrasena" />
        {error && <p className="error">{error}</p>}
        <button type="submit">Entrar</button>
      </form>
    </div>
  );
}
