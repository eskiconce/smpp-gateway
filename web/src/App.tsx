import { useEffect, useState } from 'react';
import { getToken, clearToken } from './api';
import Login from './views/Login';
import Dashboard from './views/Dashboard';
import Messages from './views/Messages';
import Tenants from './views/Tenants';
import Connectors from './views/Connectors';
import Groups from './views/Groups';
import Rules from './views/Rules';
import Rates from './views/Rates';
import Webhooks from './views/Webhooks';
import Users from './views/Users';

const NAV = [
  { hash: '#/dashboard', label: 'Dashboard' },
  { hash: '#/messages', label: 'Mensajes' },
  { hash: '#/tenants', label: 'Tenants' },
  { hash: '#/connectors', label: 'Conectores' },
  { hash: '#/groups', label: 'Grupos' },
  { hash: '#/rules', label: 'Reglas de ruteo' },
  { hash: '#/rates', label: 'Tarifas' },
  { hash: '#/webhooks', label: 'Webhooks' },
  { hash: '#/users', label: 'Usuarios' },
];

export function currentRoute(): string {
  const h = location.hash || '';
  return h.startsWith('#/') ? h : '#/dashboard';
}

export default function App() {
  const [route, setRoute] = useState(currentRoute());

  useEffect(() => {
    const onHash = () => setRoute(currentRoute());
    window.addEventListener('hashchange', onHash);
    return () => window.removeEventListener('hashchange', onHash);
  }, []);

  if (!getToken()) return <Login />;

  let view;
  switch (route) {
    case '#/dashboard': view = <Dashboard />; break;
    case '#/messages': view = <Messages />; break;
    case '#/tenants': view = <Tenants />; break;
    case '#/connectors': view = <Connectors />; break;
    case '#/groups': view = <Groups />; break;
    case '#/rules': view = <Rules />; break;
    case '#/rates': view = <Rates />; break;
    case '#/webhooks': view = <Webhooks />; break;
    case '#/users': view = <Users />; break;
    default: view = <Dashboard />;
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <h1>smppgw</h1>
        <nav>
          {NAV.map((n) => (
            <a key={n.hash} href={n.hash} className={route === n.hash ? 'active' : ''}>
              {n.label}
            </a>
          ))}
          <a href="#/login" onClick={() => { clearToken(); }}>Salir</a>
        </nav>
      </aside>
      <main className="content">{view}</main>
    </div>
  );
}
