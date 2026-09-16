# Manual de Usuario — SMPP Gateway

> **Versión:** 1.0.0  
> **Última actualización:** 16 de septiembre de 2026

---

## 1. Introducción

SMPP Gateway (`smppgw`) es una pasarela SMPP 3.4 en Go diseñada como motor de mensajería corta (SMS) con cola persistente, ruteo por conector, estados de entrega (DLR), billing prepaid y panel de administración web.

El sistema opera con un único binario `smppgw` con dos roles:

| Rol | Función |
|-----|---------|
| `server` | API HTTP + pipeline de procesamiento + ESME SMPP entrante |
| `connector` | Sesión SMPP outbound hacia el SMSC (múltiples instancias) |

### Características principales

- **Envío vía API HTTP** con autenticación por API key por tenant
- **Envío vía SMPP** (ESME bind entrante con autenticación por tenant)
- **Routing avanzado** con reglas de prioridad, grupos con peso y fallback
- **Billing prepaid** con reserva, débito y crédito por prefijo
- **DLR completo** con correlación, webhooks y reconciler automático
- **Panel admin web** con dashboard, métricas en tiempo real y CRUD completo
- **Cola persistente** en Redis Streams con consumo por prioridad
- **Múltiples conectores SMPP** con reconexión automática y throttle

---

## 2. Requisitos del sistema

### Hardware mínimo

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 2 GB | 4+ GB |
| Disco | 20 GB | 50+ SSD |

### Software

| Componente | Versión requerida |
|------------|-------------------|
| Go | 1.22+ |
| PostgreSQL | 14+ |
| Redis | 7+ |
| Node.js | 18+ (solo para compilar el frontend) |
| nginx | 1.18+ (para proxy reverso en producción) |
| make | cualquier versión |

### Dependencias del sistema

```bash
# Ubuntu/Debian
apt install -y build-essential nginx postgresql redis-server

# Herramienta de migraciones
go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

---

## 3. Instalación

### 3.1 Docker (PostgreSQL + Redis)

El proyecto incluye un `docker-compose.yml` para levantar las dependencias:

```bash
cd /root/smpp-gateway
docker compose -f deploy/docker-compose.yml up -d
```

Esto crea:

- **PostgreSQL** en `127.0.0.1:5432` (usuario: `smpp`, contraseña: `smpp`, base: `smpp_gw`)
- **Redis** en `127.0.0.1:6379`

Verificar que ambos servicios estén corriendo:

```bash
docker compose -f deploy/docker-compose.yml ps
```

### 3.2 Instalación manual

Si prefieres instalar PostgreSQL y Redis directamente en el sistema:

```bash
# PostgreSQL
apt install -y postgresql
sudo -u postgres createuser -P smpp
sudo -u postgres createdb -O smpp smpp_gw

# Redis
apt install -y redis-server
systemctl enable --now redis-server
```

### 3.3 Compilación del binario

```bash
cd /root/smpp-gateway

# Compilar binario
make build
# Resultado: bin/smppgw

# Instalar en /usr/local/bin (requiere sudo)
sudo cp bin/smppgw /usr/local/bin/
sudo chmod +x /usr/local/bin/smppgw

# Compilar e instalar el frontend
make build-ui
make install-ui
# Resultado: archivos en /var/www/smppgw
```

### 3.4 Migraciones de base de datos

```bash
export SMG_DB_URL="postgres://smpp:smpp@localhost:5432/smpp_gw?sslmode=disable"

# Aplicar todas las migraciones
make migrate-up
```

Las migraciones crean las siguientes tablas:

| Tabla | Propósito |
|-------|-----------|
| `messages` | Mensajes SMS con estado, intentos y costos |
| `tenants` | Clientes con saldo, modo y API key |
| `connectors` | Conectores SMPP (host, puerto, credenciales) |
| `groups` | Grupos de conectores para balanceo de carga |
| `group_members` | Miembros de grupo con peso |
| `routing_rules` | Reglas de ruteo con filtros y prioridad |
| `rate_tables` | Tablas de tarifas por tenant |
| `rate_entries` | Entradas de tarifa por prefijo |
| `webhooks` | Webhooks para notificación de DLR |
| `users` | Usuarios del panel admin con roles |

Para deshacer la última migración:

```bash
make migrate-down
```

### 3.5 Servicio systemd

El proyecto incluye unidades systemd para despliegue en producción:

**Servidor:**

```bash
sudo cp deploy/smppgw-server.service /etc/systemd/system/
```

Crear archivo de entorno:

```bash
sudo mkdir -p /etc/smppgw
sudo tee /etc/smppgw/server.env << 'EOF'
SMG_ROLE=server
SMG_HTTP_ADDR=:8080
SMG_SMPP_ADDR=:2775
SMG_DB_URL=postgres://smpp:smpp@localhost:5432/smpp_gw?sslmode=disable
SMG_REDIS_URL=redis://localhost:6379/0
SMG_JWT_SECRET=mi-secreto-jwt-muy-largo-y-seguro
SMG_JWT_TTL=24h
SMG_ADMIN_USER=admin
SMG_ADMIN_PASSWORD=cambiar-est-password
SMG_DLR_TTL=168h
SMG_RECONCILE_TIMEOUT=10m
SMG_RECONCILE_INTERVAL=1m
SMG_WEBHOOK_TIMEOUT=5s
SMG_METRICS_INTERVAL=2s
EOF
```

**Conectores (instancia template):**

```bash
sudo cp deploy/smppgw-connector@.service /etc/systemd/system/
sudo mkdir -p /etc/systemd/system/smppgw-connector@.service.d/
sudo cp deploy/connector-override.conf /etc/systemd/system/smppgw-connector@.service.d/override.conf
```

Crear archivo de entorno para conectores:

```bash
sudo tee /etc/smppgw/connector.env << 'EOF'
SMG_ROLE=connector
SMG_CONNECTOR_ID=1
SMG_DB_URL=postgres://smpp:smpp@localhost:5432/smpp_gw?sslmode=disable
SMG_REDIS_URL=redis://localhost:6379/0
SMG_DLR_TTL=168h
SMG_WEBHOOK_TIMEOUT=5s
EOF
```

Habilitar e iniciar:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now smppgw-server

# Conectores: instancia 1, 2, etc.
sudo systemctl enable --now smppgw-connector@1
sudo systemctl enable --now smppgw-connector@2
```

### 3.6 Nginx con SSL

Copiar la configuración de nginx:

```bash
sudo cp deploy/nginx-smppgw.conf /etc/nginx/sites-available/smppgw
sudo ln -sf /etc/nginx/sites-available/smppgw /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

Para HTTPS, agregar certificado Let's Encrypt o certificado manual:

```bash
# Ejemplo con certbot
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d smppgw.tudominio.com
```

### 3.7 Variables de entorno

| Variable | Por defecto | Descripción |
|----------|-------------|-------------|
| `SMG_ROLE` | — (requerida) | Rol del proceso: `server` o `connector` |
| `SMG_CONNECTOR_ID` | requerida para `connector` | ID numérico del conector (1, 2, ...) |
| `SMG_HTTP_ADDR` | `:8080` | Dirección y puerto de la API HTTP |
| `SMG_SMPP_ADDR` | `:2775` | Dirección y puerto del servidor SMPP |
| `SMG_DB_URL` | `postgres://smpp:smpp@localhost:5432/smpp?sslmode=disable` | URL de conexión PostgreSQL |
| `SMG_REDIS_URL` | `redis://localhost:6379/0` | URL de conexión Redis |
| `SMG_DLR_TTL` | `168h` (7 días) | Tiempo de vida de correlaciones DLR en Redis |
| `SMG_RECONCILE_TIMEOUT` | `10m` | Timeout del reconciler DLR |
| `SMG_RECONCILE_INTERVAL` | `1m` | Intervalo del reconciler DLR |
| `SMG_WEBHOOK_TIMEOUT` | `5s` | Timeout para envío de webhooks |
| `SMG_JWT_SECRET` | requerida para `server` | Secreto para firmar tokens JWT |
| `SMG_JWT_TTL` | `24h` | Tiempo de vida de tokens JWT |
| `SMG_ADMIN_USER` | `admin` | Usuario del admin bootstrap |
| `SMG_ADMIN_PASSWORD` | vacío | Password del admin bootstrap (si está vacío y no hay usuarios, no crea admin) |
| `SMG_METRICS_INTERVAL` | `2s` | Intervalo de actualización de métricas SSE |

---

## 4. Inicio y parada

### Desarrollo

```bash
# Terminal 1: servidor
make run-server

# Terminal 2: conector
make run-connector
```

### Producción (systemd)

```bash
# Iniciar servidor
sudo systemctl start smppgw-server

# Iniciar conectores
sudo systemctl start smppgw-connector@1
sudo systemctl start smppgw-connector@2

# Ver estado
sudo systemctl status smppgw-server
sudo systemctl status smppgw-connector@1

# Ver logs
sudo journalctl -u smppgw-server -f
sudo journalctl -u smppgw-connector@1 -f

# Parar
sudo systemctl stop smppgw-server
sudo systemctl stop smppgw-connector@1

# Reiniciar todo tras actualización
make deploy
```

---

## 5. Primeros pasos

### 5.1 Crear usuario admin

El usuario admin se crea automáticamente al iniciar el servidor por primera vez si se define `SMG_ADMIN_PASSWORD`:

```
SMG_ADMIN_USER=admin
SMG_ADMIN_PASSWORD=tu-password-seguro
```

Si ya existen usuarios, no se sobrescribe el admin existente.

### 5.2 Login en el panel web

1. Abrir `http://tu-servidor` en el navegador
2. Ingresar usuario (`admin`) y contraseña
3. Serás redirigido al Dashboard

### 5.3 Login vía API

```bash
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "admin", "password": "tu-password"}'
```

Respuesta:

```json
{
  "token": "eyJhbGciOi...",
  "username": "admin",
  "role": "superadmin"
}
```

### 5.4 Crear un tenant

```bash
curl -X POST http://localhost:8080/api/v1/admin/tenants \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "mi-empresa",
    "name": "Mi Empresa",
    "mode": "prepaid",
    "api_key": "clave-secreta-para-envio",
    "status": "active"
  }'
```

### 5.5 Enviar primer SMS

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H 'Authorization: Bearer clave-secreta-para-envio' \
  -H 'Content-Type: application/json' \
  -d '{"to": "56912345678", "text": "Hola desde SMPP Gateway"}'
```

---

## 6. Panel de Administración

El panel se accede en `http://tu-servidor` (o `https://` si está configurado con SSL).

### 6.1 Dashboard

Métricas en tiempo real que se actualizan cada 2 segundos:

| KPI | Descripción |
|-----|-------------|
| Enviados hoy | Total de mensajes enviados en el día actual |
| Últimos 5 min | Mensajes procesados en los últimos 5 minutos |
| Entregados | Mensajes con estado `delivered` |
| No entregados | Mensajes con estado `undeliv` |

**Tablas del dashboard:**

- **Estado:** Cantidad de mensajes por estado (delivered, undeliv, expired, rejected, buffered, accepted, failed, pending)
- **Por conector:** Mensajes enviados por cada conector en el día

### 6.2 Mensajes

Búsqueda y listado de mensajes enviados.

**Filtros disponibles:**

| Campo | Tipo | Descripción |
|-------|------|-------------|
| msisdn | texto | Filtrar por número de teléfono destino |
| estado | texto | Filtrar por estado del mensaje |
| limit | select | Cantidad de resultados por página (10, 25, 50) |

**Columnas de la tabla:**

| Columna | Descripción |
|---------|-------------|
| ID | Identificador único del mensaje |
| Tenant | Tenant propietario del mensaje |
| MSISDN | Número destino |
| Estado | Estado actual del mensaje |
| Seg | Cantidad de segmentos |
| Conector | ID del conector utilizado |
| SmscMsgid | ID devuelto por el SMSC |
| Fecha | Fecha de creación |

### 6.3 Tenants

Gestión de clientes (tenants).

**Crear tenant:**

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| id (slug) | sí | Identificador único del tenant (ej: `mi-empresa`) |
| nombre | sí | Nombre descriptivo |
| modo | sí | `prepaid` o `postpaid` |
| api_key | no | Clave para autenticar envíos vía API |

**Tabla de tenants:**

| Columna | Descripción |
|---------|-------------|
| ID | Slug del tenant |
| Nombre | Nombre descriptivo |
| Estado | `active` o `inactive` |
| Saldo | Balance actual |
| Modo | `prepaid` o `postpaid` |
| Crédito | Campo para abonar saldo (ingresar monto y hacer clic en "Abonar") |
| Api key | Clave de autenticación |

### 6.4 Conectores

Gestión de conectores SMPP para envío al SMSC.

**Crear/editar conector:**

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| nombre | sí | Nombre descriptivo del conector |
| tipo | sí | `smpp` o `http` |
| host | sí | Dirección IP o hostname del SMSC |
| port | no | Puerto SMPP (por defecto: 2775) |
| system_id | no | System ID para autenticación SMPP |
| password | no | Password SMPP |
| msg/s | no | Límite de mensajes por segundo (0 = sin límite) |

**Acciones disponibles:**

- **Editar:** Modificar configuración del conector
- **Test:** Probar conexión al SMSC (bind request)
- **Eliminar:** Borrar el conector

### 6.5 Grupos

Grupos de conectores para balanceo de carga y fallback.

**Crear grupo:**

| Campo | Descripción |
|-------|-------------|
| nombre del grupo | Nombre descriptivo |

**Asignar miembros:**

Para cada conector disponible, asignar un peso:

- **peso > 0:** El conector participa en el grupo con ese peso relativo
- **peso = 0:** El conector está fuera del grupo

El router distribuye mensajes proporcionalmente al peso de cada conector dentro del grupo.

### 6.6 Reglas de Ruteo

Define cómo se enrutan los mensajes según filtros.

**Crear regla:**

| Campo | Descripción |
|-------|-------------|
| prioridad | Número de prioridad (menor = mayor prioridad) |
| tenant_id | Filtrar por tenant específico (vacío = todos) |
| from | Filtrar por dirección origen |
| prefix | Filtrar por prefijo del MSISDN destino |
| regex | Expresión regular para filtrar destino |
| connector_id | Conector destino (mutuamente exclusivo con group_id) |
| group_id | Grupo destino (mutuamente exclusivo con connector_id) |

**Reordenar prioridades:** Arrastrar y soltar las filas en la tabla para cambiar el orden de prioridad. Las reglas se evalúan de mayor a menor prioridad.

### 6.7 Tarifas

Sistema de tarifas por prefijo para billing.

**Flujo de trabajo:**

1. Seleccionar un tenant
2. Crear una tabla de tarifas (nombre + activa)
3. Agregar entradas por prefijo con precio y conector asociado

**Crear entrada de tarifa:**

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| prefix msisdn | sí | Prefijo del número destino (ej: `569`) |
| precio | sí | Costo por mensaje a ese prefijo |
| connector_id | no | Conector asociado a esta tarifa |

### 6.8 Webhooks

Notificación HTTP automática cuando cambia el estado de un mensaje (DLR).

**Crear webhook:**

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| url | sí | URL de destino para la notificación |
| auth_token | no | Token de autenticación (se envía como `Authorization: Bearer <token>`) |
| events | no | Eventos a notificar separados por coma (ej: `DLR`) |
| tenant_id | no | Filtrar por tenant específico |
| activo | no | Checkbox para habilitar/deshabilitar |

### 6.9 Usuarios

Gestión de usuarios del panel de administración.

**Crear usuario:**

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| username | sí | Nombre de usuario |
| password | sí | Contraseña |
| role | sí | `superadmin`, `admin` o `viewer` |
| tenant_id | no | Asociar a un tenant específico |

**Roles:**

| Rol | Permisos |
|-----|----------|
| `superadmin` | Todo: CRUD de usuarios, tenants, conectores, reglas, tarifas, webhooks |
| `admin` | CRUD de tenants, conectores, reglas, tarifas, webhooks, ver mensajes |
| `viewer` | Solo lectura: dashboard, mensajes, listados |

---

## 7. API HTTP

Todas las rutas comienzan con `/api/v1`. Las rutas de administración requieren autenticación JWT en el header `Authorization: Bearer <token>`. El envío de mensajes requiere la API key del tenant en el header `Authorization: Bearer <api_key>`.

### 7.1 Login

```
POST /api/v1/auth/login
```

**Headers:** `Content-Type: application/json`

**Request body:**

```json
{
  "username": "admin",
  "password": "tu-password"
}
```

**Respuesta 200:**

```json
{
  "token": "eyJhbGciOi...",
  "username": "admin",
  "role": "superadmin"
}
```

**Curl:**

```bash
curl -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username": "admin", "password": "tu-password"}'
```

### 7.2 Enviar mensaje

```
POST /api/v1/messages
```

**Headers:** `Authorization: Bearer <api_key_del_tenant>`, `Content-Type: application/json`

**Request body:**

```json
{
  "to": "56912345678",
  "text": "Tu mensaje SMS",
  "routing_tag": "premium"
}
```

| Campo | Requerido | Descripción |
|-------|-----------|-------------|
| to | sí | MSISDN destino |
| text | sí | Texto del mensaje |
| routing_tag | no | Tag para matching de reglas de ruteo |

**Respuesta 200:**

```json
{
  "message_id": "a1b2c3d4e5",
  "segments": 1
}
```

**Errores:**

| HTTP | Mensaje | Causa |
|------|---------|-------|
| 401 | api key requerida / api key invalida | API key no proporcionada o no válida |
| 400 | to y text requeridos | Faltan campos obligatorios |
| 402 | saldo insuficiente | Tenant sin saldo disponible |
| 404 | sin tarifa para el destino | No hay tarifa configurada para el prefijo |

**Curl:**

```bash
curl -X POST http://localhost:8080/api/v1/messages \
  -H 'Authorization: Bearer clave-secreta-para-envio' \
  -H 'Content-Type: application/json' \
  -d '{"to": "56912345678", "text": "Hola desde la API"}'
```

### 7.3 Obtener mensaje

```
GET /api/v1/messages/{id}
```

**Respuesta 200:**

```json
{
  "message_id": "a1b2c3d4e5",
  "tenant_id": "mi-empresa",
  "msisdn": "56912345678",
  "state": "delivered",
  "smsc_msgid": "SMSC123456",
  "try_count": 1,
  "segments": 1,
  "amount": 0.005,
  "created_at": "2026-09-16T10:30:00Z",
  "updated_at": "2026-09-16T10:30:05Z"
}
```

**Curl:**

```bash
curl http://localhost:8080/api/v1/messages/a1b2c3d4e5
```

### 7.4 Health check

```
GET /healthz
```

**Respuesta 200:** `ok`

**Curl:**

```bash
curl http://localhost:8080/healthz
```

### 7.5 Métricas (snapshot)

```
GET /api/v1/metrics
```

**Headers:** `Authorization: Bearer <token>`

**Respuesta 200:**

```json
{
  "by_state": {
    "delivered": 1520,
    "undeliv": 45,
    "expired": 12,
    "pending": 3
  },
  "by_connector": [
    {"connector_id": 1, "count": 800},
    {"connector_id": 2, "count": 770}
  ],
  "today": 1580,
  "last_5min": 42,
  "generated_at": "2026-09-16T14:00:00Z"
}
```

**Curl:**

```bash
curl -H 'Authorization: Bearer <token>' http://localhost:8080/api/v1/metrics
```

### 7.6 Métricas SSE (streaming)

```
GET /api/v1/metrics/stream
```

Stream Server-Sent Events que emite métricas cada `SMG_METRICS_INTERVAL` (por defecto 2 segundos).

**Curl:**

```bash
curl -H 'Authorization: Bearer <token>' \
  -H 'Accept: text/event-stream' \
  http://localhost:8080/api/v1/metrics/stream
```

### 7.7 Listar mensajes (admin)

```
GET /api/v1/admin/messages?msisdn=569&state=delivered&limit=25&offset=0
```

**Headers:** `Authorization: Bearer <token>`

**Parámetros de query:**

| Parámetro | Tipo | Descripción |
|-----------|------|-------------|
| msisdn | string | Filtrar por destino |
| state | string | Filtrar por estado |
| limit | int | Resultados por página (default 25) |
| offset | int | Offset para paginación |

**Respuesta 200:**

```json
{
  "total": 1580,
  "items": [
    {
      "id": "a1b2c3d4e5",
      "tenant_id": "mi-empresa",
      "source_addr": "api",
      "msisdn": "56912345678",
      "text": "Hola",
      "segments": 1,
      "connector_id": 1,
      "state": "delivered",
      "try_count": 1,
      "smsc_msgid": "SMSC123",
      "amount": 0.005,
      "created_at": "2026-09-16T10:30:00Z"
    }
  ]
}
```

**Curl:**

```bash
curl -H 'Authorization: Bearer <token>' \
  'http://localhost:8080/api/v1/admin/messages?limit=10&offset=0'
```

### 7.8 CRUD Tenants

**Listar:**

```
GET /api/v1/admin/tenants
```

**Crear:**

```
POST /api/v1/admin/tenants
```

Request body:

```json
{
  "id": "mi-empresa",
  "name": "Mi Empresa",
  "status": "active",
  "balance": 100.00,
  "mode": "prepaid",
  "api_key": "clave-secreta"
}
```

**Obtener uno:**

```
GET /api/v1/admin/tenants/{id}
```

**Abonar crédito:**

```
POST /api/v1/admin/tenants/{id}/credit
```

Request body:

```json
{
  "amount": 50.00
}
```

Respuesta:

```json
{
  "balance": 150.00
}
```

**Listar transacciones:**

```
GET /api/v1/admin/tenants/{id}/transactions?limit=50
```

**Curl ejemplo completo:**

```bash
# Listar tenants
curl -H 'Authorization: Bearer <token>' http://localhost:8080/api/v1/admin/tenants

# Crear tenant
curl -X POST http://localhost:8080/api/v1/admin/tenants \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"id":"nuevo","name":"Nuevo Tenant","mode":"prepaid","api_key":"k123"}'

# Abonar crédito
curl -X POST http://localhost:8080/api/v1/admin/tenants/nuevo/credit \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"amount": 100}'
```

### 7.9 CRUD Conectores

**Listar:**

```
GET /api/v1/admin/connectors
```

**Crear:**

```
POST /api/v1/admin/connectors
```

Request body:

```json
{
  "name": "SMSC Principal",
  "type": "smpp",
  "host": "smsc.example.com",
  "port": 2775,
  "system_id": "miaccount",
  "password": "secreto",
  "bind_mode": "transceiver",
  "concurrency": 10,
  "max_msg_per_sec": 100,
  "enquire_link_interval": 30,
  "tls": false,
  "enabled": true
}
```

**Actualizar:**

```
PUT /api/v1/admin/connectors/{id}
```

**Eliminar:**

```
DELETE /api/v1/admin/connectors/{id}
```

**Test de conexión:**

```
POST /api/v1/admin/connectors/{id}/test
```

Respuesta:

```json
{
  "ok": true,
  "detail": "bind OK: sistema conectado"
}
```

**Curl ejemplo completo:**

```bash
# Crear conector
curl -X POST http://localhost:8080/api/v1/admin/connectors \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"name":"SMSC1","type":"smpp","host":"smsc.example.com","port":2775,"system_id":"test","password":"test"}'

# Test conexión
curl -X POST http://localhost:8080/api/v1/admin/connectors/1/test \
  -H 'Authorization: Bearer <token>'
```

### 7.10 CRUD Grupos

**Listar:**

```
GET /api/v1/admin/groups
```

**Crear:**

```
POST /api/v1/admin/groups
```

Request body:

```json
{
  "name": "Producción"
}
```

**Eliminar:**

```
DELETE /api/v1/admin/groups/{id}
```

**Asignar miembros:**

```
PUT /api/v1/admin/groups/{id}/members
```

Request body:

```json
{
  "members": [
    {"connector_id": 1, "weight": 60},
    {"connector_id": 2, "weight": 40}
  ]
}
```

**Curl ejemplo completo:**

```bash
# Crear grupo
curl -X POST http://localhost:8080/api/v1/admin/groups \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Produccion"}'

# Asignar miembros con pesos
curl -X PUT http://localhost:8080/api/v1/admin/groups/1/members \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"members":[{"connector_id":1,"weight":60},{"connector_id":2,"weight":40}]}'
```

### 7.11 CRUD Reglas de Ruteo

**Listar:**

```
GET /api/v1/admin/routing-rules
```

**Crear:**

```
POST /api/v1/admin/routing-rules
```

Request body:

```json
{
  "priority": 1,
  "tenant_id": "mi-empresa",
  "from": "api",
  "prefix": "569",
  "regex": "^569[0-9]{8}$",
  "routing_tag": "premium",
  "connector_id": 1,
  "group_id": 0
}
```

**Eliminar:**

```
DELETE /api/v1/admin/routing-rules/{id}
```

**Cambiar prioridad:**

```
PUT /api/v1/admin/routing-rules/{id}/priority
```

Request body:

```json
{
  "priority": 3
}
```

**Curl ejemplo completo:**

```bash
# Crear regla
curl -X POST http://localhost:8080/api/v1/admin/routing-rules \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"priority":1,"prefix":"569","connector_id":1}'

# Cambiar prioridad
curl -X PUT http://localhost:8080/api/v1/admin/routing-rules/1/priority \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"priority":2}'
```

### 7.12 CRUD Tarifas

**Listar tablas de un tenant:**

```
GET /api/v1/admin/rate-tables
```

**Headers:** `X-Tenant-ID: mi-empresa`

**Crear tabla:**

```
POST /api/v1/admin/rate-tables
```

Request body:

```json
{
  "name": "Tarifa General",
  "active": true
}
```

**Listar entradas de una tabla:**

```
GET /api/v1/admin/rate-tables/{id}/entries
```

**Crear entrada:**

```
POST /api/v1/admin/rate-tables/{id}/entries
```

Request body:

```json
{
  "prefix": "569",
  "price": 0.005,
  "connector_id": 1
}
```

**Eliminar entrada:**

```
DELETE /api/v1/admin/rate-entries/{id}
```

**Curl ejemplo completo:**

```bash
# Crear tabla de tarifas
curl -X POST http://localhost:8080/api/v1/admin/rate-tables \
  -H 'Authorization: Bearer <token>' \
  -H 'X-Tenant-ID: mi-empresa' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Tarifa General","active":true}'

# Agregar entrada
curl -X POST http://localhost:8080/api/v1/admin/rate-tables/1/entries \
  -H 'Authorization: Bearer <token>' \
  -H 'X-Tenant-ID: mi-empresa' \
  -H 'Content-Type: application/json' \
  -d '{"prefix":"569","price":0.005}'
```

### 7.13 CRUD Webhooks

**Listar:**

```
GET /api/v1/admin/webhooks
```

**Crear:**

```
POST /api/v1/admin/webhooks
```

Request body:

```json
{
  "url": "https://miapp.com/webhook/dlr",
  "auth_token": "token-secreto",
  "events": ["DLR"],
  "active": true,
  "tenant_id": "mi-empresa"
}
```

**Actualizar:**

```
PUT /api/v1/admin/webhooks/{id}
```

**Eliminar:**

```
DELETE /api/v1/admin/webhooks/{id}
```

**Curl ejemplo completo:**

```bash
# Crear webhook
curl -X POST http://localhost:8080/api/v1/admin/webhooks \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://miapp.com/dlr","events":["DLR"],"active":true}'

# Listar webhooks
curl -H 'Authorization: Bearer <token>' http://localhost:8080/api/v1/admin/webhooks
```

### 7.14 CRUD Usuarios

**Listar:**

```
GET /api/v1/admin/users
```

**Crear:**

```
POST /api/v1/admin/users
```

Request body:

```json
{
  "username": "operador1",
  "password": "password123",
  "role": "admin",
  "tenant_id": "mi-empresa"
}
```

**Eliminar:**

```
DELETE /api/v1/admin/users/{id}
```

**Curl ejemplo completo:**

```bash
# Crear usuario
curl -X POST http://localhost:8080/api/v1/admin/users \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{"username":"operador1","password":"pass123","role":"admin"}'

# Listar usuarios
curl -H 'Authorization: Bearer <token>' http://localhost:8080/api/v1/admin/users
```

---

## 8. Envío de SMS (flujo completo)

### Flujo HTTP → SMSC

```
1. Cliente envía POST /api/v1/messages con API key
   ↓
2. API autentica tenant por API key
   ↓
3. Pipeline valida: saldo suficiente? tarifa existe?
   ↓
4. Billing reserva monto en tabla de saldos
   ↓
5. Router evalúa reglas de ruteo → selecciona conector o grupo
   ↓
6. Si grupo: distribuye según pesos de miembros
   ↓
7. Mensaje se encola en Redis Streams (cola del conector)
   ↓
8. Worker del conector consume de la cola
   ↓
9. Sesión SMPP envía submit_sm al SMSC
   ↓
10. SMSC responde con message_id
   ↓
11. Estado del mensaje se actualiza a "accepted"
   ↓
12. SMSC envía deliver_sm con DLR → correlación → webhook
```

### Flujo SMPP entrante (ESME)

```
1. Cliente ESME hace bind transceiver al servidor SMPP
   ↓
2. Servidor autentica por system_id y password del tenant
   ↓
3. Cliente envía submit_sm
   ↓
4. Pipeline procesa igual que HTTP (validación → billing → ruteo → encolado)
   ↓
5. Envío al SMSC vía conector outbound
```

### Estados de un mensaje

| Estado | Descripción |
|--------|-------------|
| `pending` | Recién creado, en cola |
| `buffered` | Enviado al SMSC, esperando confirmación |
| `accepted` | SMSC aceptó el mensaje |
| `delivered` | Mensaje entregado al destino final |
| `undeliv` | No entregado |
| `expired` | Expiró sin entregarse |
| `rejected` | Rechazado por el SMSC |
| `failed` | Error en el envío |

---

## 9. DLR (correlación, estados, webhooks)

### Correlación

Cuando el SMSC devuelve un DLR (deliver_sm), el sistema correlaciona el `smsc_msgid` con el `message_id` interno usando:

- **Redis:** Caché efímera con TTL configurable (`SMG_DLR_TTL`, por defecto 7 días)
- **PostgreSQL:** Tabla `messages` como fuente persistente

### Estados DLR del SMSC

| Código SMSC | Estado interno |
|-------------|----------------|
| ACCEPTED | `accepted` |
| DELIVERED | `delivered` |
| UNDELIV | `undeliv` |
| EXPIRED | `expired` |
| REJECTED | `rejected` |

### Webhooks

Cuando cambia el estado, el sistema notifica a los webhooks configurados:

**Payload enviado al webhook:**

```json
{
  "event": "DLR",
  "message_id": "a1b2c3d4e5",
  "tenant_id": "mi-empresa",
  "smsc_msgid": "SMSC123456",
  "msisdn": "56912345678",
  "state": "delivered",
  "source_channel": "api"
}
```

### Reconciler

El reconciler ejecuta cada `SMG_RECONCILE_INTERVAL` (default 1m) y transita mensajes en estado `accepted` a `expired` si han superado `SMG_RECONCILE_TIMEOUT` (default 10m) sin recibir DLR confirmatorio.

---

## 10. Billing (tarifas, reserva, débito, saldo)

### Modelo prepaid

1. **Reserva:** Al enviar un mensaje, se reserva el monto según la tarifa del prefijo
2. **Débito:** Cuando el SMSC confirma accept, se debita definitivamente
3. **Crédito:** Un admin puede abonar saldo a un tenant

### Errores de billing

| Error | HTTP | Descripción |
|-------|------|-------------|
| `ErrInsufficientBalance` | 402 | Saldo insuficiente para enviar |
| `ErrNoRate` | 422 | No existe tarifa para el prefijo del destino |

### Configurar tarifas

1. Crear tabla de tarifas para el tenant
2. Agregar entradas por prefijo con precio
3. Opcionalmente, asociar cada entrada a un conector específico

---

## 11. SMPP (conexión conector, parámetros, reconexión)

### Parámetros de sesión SMPP

| Parámetro | Descripción |
|-----------|-------------|
| Host | Dirección del SMSC |
| Port | Puerto (default 2775) |
| System ID | Identificador de cuenta |
| Password | Contraseña SMPP |
| Bind mode | `transceiver` (bind_transceiver) |
| Msg/s | Límite de mensajes por segundo |
| Concurrency | Conexiones simultáneas |
| Enquire link | Intervalo de keepalive (segundos) |
| TLS | Habilitar conexión segura |

### Reconexión automática

Si la conexión con el SMSC se pierde, el conector reconecta automáticamente con backoff exponencial. Los mensajes en cola no se pierden.

### Test de conexión

Desde el panel admin o la API, se puede probar la conexión a un conector específico:

```bash
curl -X POST http://localhost:8080/api/v1/admin/connectors/1/test \
  -H 'Authorization: Bearer <token>'
```

---

## 12. Troubleshooting

### El servidor no inicia

```bash
# Verificar logs
sudo journalctl -u smppgw-server -n 50

# Causa común: SMG_JWT_SECRET no definido
# Solución: agregar SMG_JWT_SECRET al archivo server.env
```

### Error de conexión a PostgreSQL

```bash
# Verificar que PostgreSQL esté corriendo
docker compose -f deploy/docker-compose.yml ps
# o
systemctl status postgresql

# Verificar la URL de conexión
echo $SMG_DB_URL
```

### Error de conexión a Redis

```bash
# Verificar que Redis esté corriendo
redis-cli ping
# Debe responder: PONG

# Verificar la URL
echo $SMG_REDIS_URL
```

### Mensajes en estado "pending" sin avanzar

- Verificar que el conector esté corriendo: `systemctl status smppgw-connector@1`
- Verificar que el conector tenga sesión SMPP activa con el SMSC
- Verificar logs del conector: `journalctl -u smppgw-connector@1 -f`

### Error "saldo insuficiente"

- El tenant no tiene saldo suficiente
- Abonar crédito desde el panel admin (sección Tenants) o vía API

### Error "sin tarifa para el destino"

- No existe entrada de tarifa para el prefijo del MSISDN destino
- Crear entrada de tarifa en el panel (sección Tarifas) o vía API

### El panel admin no carga

```bash
# Verificar que nginx esté configurado
sudo nginx -t

# Verificar que los archivos del frontend estén instalados
ls -la /var/www/smppgw/

# Verificar permisos
sudo chown -R smpp:smpp /var/www/smppgw/
```

### DLR no se reciben

- Verificar que el webhook esté configurado y activo
- Verificar conectividad desde el servidor al webhook URL
- Verificar logs del reconciler: `journalctl -u smppgw-server | grep reconcil`

---

## 13. Apéndice

### Códigos de estado SMPP

| Código | Nombre | Descripción |
|--------|--------|-------------|
| 0x00000000 | ESME_ROK | Éxito |
| 0x00000001 | ESME_RINVMSGLEN | Longitud de mensaje inválida |
| 0x00000002 | ESME_RINVCMDLEN | Longitud de comando inválida |
| 0x00000003 | ESME_RINVCMDID | ID de comando inválido |
| 0x00000004 | ESME_RINVBNDSTS | Estado de bind inválido |
| 0x00000005 | ESME_RALYBND | Ya en estado bound |
| 0x00000006 | ESME_RINVPRTYP | Tipo de protocolo inválido |
| 0x00000007 | ESME_RINVNUMDESTS | Número de destinos inválido |
| 0x00000008 | ESME_RINVDLNAME | Nombre de distribution list inválido |
| 0x00000009 | ESME_RINVDESTFLAG | Flag de destino inválido |
| 0x0000000A | ESME_RINVSUBREP | Inválida presentación de submit |
| 0x0000000B | ESME_RINVSENDERREP | Inválido sender/receiver |
| 0x0000000C | ESME_RINVMSGID | Message ID inválido |
| 0x0000000D | ESME_RBINDFAIL | Fallo en bind |
| 0x0000000E | ESME_RINVPWD | Password inválida |
| 0x0000000F | ESME_RINVSYSID | System ID inválido |
| 0x00000010 | ESME_RCANCELFAIL | Fallo al cancelar mensaje |
| 0x00000011 | ESME_RMSGREPLACEFUL | Mensaje a reemplazar está lleno |
| 0x00000013 | ESME_RREFLEXALERT | Alerta de reflejo activada |
| 0x00000014 | ESME_RNOORIGINATOR | Sin origen original |
| 0x00000033 | ESME_RINVSRCTON | TON de origen inválido |
| 0x00000034 | ESME_RINVSRCNPI | NPI de origen inválido |
| 0x00000040 | ESME_RINVDSTADR | Dirección destino inválida |
| 0x00000041 | ESME_RINVSMLEN | Longitud SMS inválida |
| 0x00000042 | ESME_RINVCURPARS | Parámetros actuales inválidos |
| 0x00000043 | ESME_RINVURGS | Urgencia inválida |
| 0x00000044 | ESME_RINVDIFPARS | Parámetros diferentes inválidos |
| 0x00000045 | ESME_RINVNUMBTLVS | Número de niveles inválido |
| 0x00000046 | ESME_RINVDIFPARINST | Parámetros de instancia diferentes |
| 0x000000C0 | ESME_RPROHIBITED | Envío prohibido |
| 0x000000C1 | ESME_RINVDLFULL | Distribution list lleno |
| 0x000000C2 | ESME_RINVUSRID | User ID inválido |
| 0x000000C3 | ESME_RAUTHERR | Error de autenticación |
| 0x000000C4 | ESME_RVDLALRDYFUL | DL ya está lleno |
| 0x000000C5 | ESME_RINVLVL | Nivel inválido |
| 0x000000C6 | ESME_RINVCUST | Cliente inválido |
| 0x000000C7 | ESME_ROK_WITH_MC_specifics | Éxito con specifics del MC |
| 0x000000C8 | ESME_RINVCNTMCUSER | Cliente/usuario inválido para MC |
| 0x000000C9 | ESME_RAPPNOTFOUND | Aplicación no encontrada |
| 0x000000CA | ESME_RAPPNOPW | Aplicación no requiere password |
| 0x000000CB | ESME_RAPPIDBUSY | Aplicación ID ocupado |
| 0x000000CC | ESME_RAPPCONFIGERR | Error de configuración de aplicación |
| 0x00000100 | ESME_RTHROTTLED | Throttle (frecuencia excesiva) |
| 0x00000200 | ESME_RINVSCHED | Programación inválida |
| 0x00000201 | ESME_RINVEXPIRY | Expiración inválida |
| 0x00000202 | ESME_RINVDNDISTFLAG | Flag de distribución inválido |
| 0x00000203 | ESME_RINVNUMMCAD | Número inválido de MC adds |
| 0x00000204 | ESME_RINVSRECID | Rec ID inválido |
| 0x00000205 | ESMS_RINVMSGBODY | Cuerpo de mensaje inválido |
| 0x00000206 | ESME_RINVBROADCRSTYP | Broadcast result type inválido |
| 0x00000207 | ESME_RINVMULTIPART | Mensaje multipart inválido |
| 0x00000208 | ESME_RPRODUNOTUSED | Production no usada |
| 0x00000209 | ESME_RINVMAXMSGRLEN | Longitud máxima de mensaje inválida |
| 0x0000020A | ESME_RINVMSGSRC | Fuente de mensaje inválida |
| 0x0000020B | ESME_RINVMSGSRCQOS | QoS de fuente de mensaje inválido |
| 0x0000020C | ESME_RINVMSGWAIT | Msg wait inválido |
| 0x0000020D | ESME_RINVSMSMSG | SMS message inválido |
| 0x0000020E | ESME_RINVSMSACKREQ | SMS ack request inválido |
| 0x0000020F | ESME_RINVSMSTYP | SMS type inválido |
| 0x00000210 | ESME_RINVRECPRESENTATION | Recipient presentation inválida |
| 0x00000211 | ESME_RINVRECSUBADDR | Recipient subaddress inválida |
| 0x00000212 | ESME_RINVSMMTYP | SM message type inválido |
| 0x00000213 | ESME_RINVCONCATMSGID | Concat msg ref number inválido |
| 0x00000214 | ESME_RINVCONCATPARAM | Parámetros de concatenación inválidos |
| 0x00000215 | ESME_RINVOPEXP | Operation elapsed time inválido |
| 0x00000216 | ESME_RINVPREFPAY | Preferred payment inválido |
| 0x00000217 | ESME_RINVDSTDQOA | DstQoS inválido |
| 0x00000218 | ESME_RINVREPDQ | Reply timing inválido |
| 0x00000219 | ESME_RINVTF | Time formatting inválido |
| 0x0000021A | ESME_RINVDQoS | Delivery QoS inválido |
| 0x0000021B | ESME_RINVNOTIFREP | Notification report inválido |
| 0x0000021C | ESME_RINVOPOVLP | Operation overlap inválido |
| 0x0000021D | ESME_RINVMEDSRC | Message delivery source inválido |
| 0x0000021E | ESME_RINVMEDDEST | Message delivery destination inválido |
| 0x0000021F | ESME_RINVMEDTMP | Message delivery template inválido |
| 0x00000220 | ESME_RINVMEDAUTH | Message delivery authentication inválida |
| 0x00000221 | ESME_RINVMEDACT | Message delivery action inválida |
| 0x00000222 | ESME_RINVMEDHOW | Message delivery how inválido |
| 0x00000223 | ESME_RINVMEDSVCID | Message delivery service ID inválido |
| 0x00000224 | ESME_RINVMEDSERV | Message delivery service inválido |
| 0x00000225 | ESME_RINVMEDBILLING | Message delivery billing inválido |
| 0x00000226 | ESME_RINVMEDPORT | Message delivery port inválido |
| 0x00000227 | ESME_RINVMEDMSINFO | Message delivery MS info inválido |
| 0x00000228 | ESME_RINVPROCLVL | Prohibido (nivel de servicio) |
| 0x00000229 | ESME_RINVMMBBDATA | MMBD data inválido |
| 0x0000022A | ESME_RINVMMBDSERV | MMBD service inválido |
| 0x0000022B | ESME_RINVMMBDPROTO | MMBD protocol inválido |
| 0x0000022C | ESME_RINVMMBDBEARER | MMBD bearer inválido |
| 0x0000022D | ESME_RINVMMBDSMSENTRY | MMBD SMS entry inválido |
| 0x00000300 | ESME_RINVSEQNUM | Sequence number inválido |
| 0x00000301 | ESME_RINVPERMS | Permisos inválidos |
| 0x00000302 | ESME_RINVSGISTRING | Admin/GI string inválido |
| 0x00000303 | ESME_RINVSYSFEATURES | System features inválido |
| 0x00000304 | ESME_RINVVCARD | vCard inválido |
| 0x00000305 | ESME_RINVVCALENDAR | vCalendar inválido |
| 0x00000306 | ESME_RINVEXRDATAREC | Extra data record inválido |
| 0x00000307 | ESME_RINVSROAMINGINFO | Roaming info inválido |
| 0x00000308 | ESME_RINVSMSGFLAGS | Message flags inválido |
| 0x00000309 | ESME_RINVSMSTAT | Message status inválido |
| 0x0000030A | ESME_RINVRECEIPTQ | Receipt queued request inválido |
| 0x0000030B | ESME_RINVSMMSTATUS | SM-MMS status inválido |
| 0x0000030C | ESME_RINVINVCERT | Certificado inválido |
| 0x0000030D | ESME_RINVINVCERTTYPE | Tipo de certificado inválido |
| 0x0000030E | ESME_RINVINVSVC | Servicio inválido |
| 0x0000030F | ESME_RINVCLTID | Client ID inválido |
| 0x00000310 | ESME_RINVJUNK | Datos basura inválidos |
| 0x00000311 | ESME_RINVTOLVLP | Timing option level inválido |
| 0x00000312 | ESME_RINVTORGPAR | Timing parameter inválido |
| 0x00000313 | ESME_RINVTRELWIND | Timing release window inválido |
| 0x00000314 | ESME_RINVSUBPARAM | Submit parameter inválido |
| 0x00000315 | ESME_RINVOTID | Operation transaction ID inválido |
| 0x00000316 | ESME_RINVSRVPNREQ | Service provisioning not required |
| 0x00000317 | ESME_RINVSMSDLVRRCPT | SMs DLVR receipt inválido |
| 0x00000318 | ESME_RINVCLTINFREQ | Client info request inválido |
| 0x00000319 | ESME_RINVRESTART | Restart inválido |
| 0x0000031A | ESME_RINVCANCELREQ | Cancel request inválido |
| 0x0000031B | ESME_RINVREPLACEREQ | Replace request inválido |
| 0x0000031C | ESME_RINVPOLLREQ | Poll request inválido |
| 0x0000031D | ESME_RINVPOLLREPSCHED | Poll reply schedule inválido |
| 0x0000031E | ESME_RINVQUERYREPLX | Query reply length inválido |
| 0x0000031F | ESME_RINVREPLYPARAMS | Reply parameters inválidos |
| 0x00000320 | ESME_RINVXMSGSRC | Extra message source inválido |
| 0x00000321 | ESME_RINVSRCHCRGINFO | Charging info inválido |
| 0x00000322 | ESME_RINVSRCQOSINFO | Source QoS info inválido |
| 0x00000323 | ESME_RINVSRCPORT | Source port inválido |
| 0x00000324 | ESME_RINVSRCHEAAD | Source header inválido |
| 0x00000325 | ESME_RINVSRCFILE | Source file inválido |
| 0x00000326 | ESME_RINVSRCFILENAM | Source filename inválido |
| 0x00000327 | ESME_RINVSRCDATA | Source data inválido |
| 0x00000328 | ESME_RINVSRCTEXT | Source text inválido |
| 0x00000329 | ESME_RINVSRCRPLYPARAM | Source reply params inválido |
| 0x0000032A | ESME_RINVSRCOPREQ | Source operation request inválido |
| 0x0000032B | ESME_RINVDESTCRGINFO | Destination charging info inválido |
| 0x0000032C | ESME_RINVDSTQOSINFO | Destination QoS info inválido |
| 0x0000032D | ESME_RINVDSTPORT | Destination port inválido |
| 0x0000032E | ESME_RINVDSTHEAD | Destination header inválido |
| 0x0000032F | ESME_RINVDSTFILE | Destination file inválido |
| 0x00000330 | ESME_RINVDSTFILENAME | Destination filename inválido |
| 0x00000331 | ESME_RINVDSTDATA | Destination data inválido |
| 0x00000332 | ESME_RINVDSTTEXT | Destination text inválido |
| 0x00000333 | ESME_RINVDSTREPLYPARAM | Destination reply params inválido |
| 0x00000334 | ESME_RINVDSTOPREQ | Destination operation request inválido |
| 0x00000335 | ESME_RINVEXPDTIME | Expiration time inválido |
| 0x00000336 | ESME_RINVINST | Operation instance inválido |
| 0x00000337 | ESME_RINVOPRPARAM | Operation parameter inválido |
| 0x00000338 | ESME_RINVOPREXT | Operation extension inválido |
| 0x00000339 | ESME_RINVTERTIME | Delivery time inválido |
| 0x0000033A | ESME_RINVTRCTYP | Trigger type inválido |
| 0x0000033B | ESME_RINVREPLYPARAM | Reply parameter inválido |
| 0x0000033C | ESME_RINVMULTISHORT | Multiple short messages inválido |
| 0x0000033D | ESME_RINVSRCPORT | Source port (duplicate) |
| 0x0000033E | ESME_RINVSRCHEAAD | Source header (duplicate) |
| 0x0000033F | ESME_RINVSRCFILE | Source file (duplicate) |
| 0x00000340 | ESME_RINVSRCFILENAME | Source filename (duplicate) |
| 0x00000341 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000342 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000343 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000344 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000345 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000346 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000347 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000348 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000349 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000034F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000350 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000351 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000352 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000353 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000354 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000355 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000356 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000357 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000358 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000359 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000035F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000360 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000361 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000362 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000363 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000364 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000365 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000366 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000367 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000368 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000369 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000036F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000370 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000371 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000372 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000373 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000374 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000375 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000376 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000377 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000378 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000379 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000037F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000380 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000381 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000382 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000383 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000384 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000385 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000386 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000387 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000388 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000389 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000038F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000390 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000391 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000392 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000393 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000394 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000395 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000396 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000397 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000398 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000399 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039A | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039B | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039C | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039D | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039E | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x0000039F | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003A9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AD | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003AF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003B9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BD | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003BF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003C9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CD | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003CF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003D9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DD | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003DF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003E9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003EA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003EB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003EC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003ED | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003EE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003EF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F0 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F1 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F2 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F3 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F4 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F5 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F6 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F7 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F8 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003F9 | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FA | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FB | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FC | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FD | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FE | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x000003FF | ESME_RINVSRCFILENAM | Source filename (duplicate) |
| 0x00000400 | ESME_RUNKNOWNERR | Error desconocido |
| 0x00000401 | ESME_RADAPTDATAINSUF | Adaptador con datos insuficientes |
| 0x00000402 | ESME_RADAPTREQDECL | Solicitud de adaptador declinada |
| 0x00000403 | ESME_RADAPTREQERR | Error en solicitud de adaptador |
| 0x00000404 | ESME_RADAPTTYPUNK | Tipo de adaptador desconocido |

### Estados de mensaje

| Estado | Descripción | Flujo típico |
|--------|-------------|--------------|
| `pending` | Creado, esperando procesamiento | Recién encolado |
| `buffered` | Enviado al SMSC, esperando respuesta | submit_sm enviado |
| `accepted` | SMSC aceptó, pendiente de entrega | SMSC respondió OK |
| `delivered` | Entregado al dispositivo final | DLR positivo recibido |
| `undeliv` | No pudo ser entregado | DLR negativo |
| `expired` | Expiró sin entrega (reconciler) | Timeout sin DLR |
| `rejected` | Rechazado por SMSC | SMSC rechazó |
| `failed` | Error de envío | Error de sesión SMPP |

### Glosario

| Término | Definición |
|---------|------------|
| SMPP | Short Message Peer-to-Peer, protocolo estándar para intercambio de SMS |
| SMSC | Short Message Service Center, centro de mensajes del operador |
| ESME | External Short Message Entity, aplicación que envía/recibe SMS |
| DLR | Delivery Report, reporte de entrega de un mensaje SMS |
| MSISDN | Mobile Station International Subscriber Directory Number (número de teléfono) |
| TON | Type of Number, tipo de dirección (nacional, internacional, etc.) |
| NPI | Numbering Plan Identification, plan de numeración |
| PDU | Protocol Data Unit, unidad de datos del protocolo SMPP |
| bind | Establecimiento de sesión entre ESME y SMSC |
| submit_sm | PDU para enviar un mensaje SMS desde ESME al SMSC |
| deliver_sm | PDU para entregar un mensaje o DLR desde SMSC al ESME |
| enquire_link | Mensaje de keepalive en la sesión SMPP |
| TLV | Tag-Length-Value, parámetros opcionales en SMPP |
| Throttle | Mecanismo de control de frecuencia de envío |
| Prepaid | Modelo de facturación con saldo adelantado |
| Postpaid | Modelo de facturación con pago posterior |
| Pipeline | Cadena de procesamiento: validación → ruteo → billing → encolado |
| Conector | Instancia de sesión SMPP hacia un SMSC específico |
| Grupo | Colección de conectores para balanceo de carga |
| Routing | Proceso de selección de conector según reglas y filtros |
| Reconciler | Proceso que transita mensajes stale de `accepted` a `expired` |
| Webhook | Notificación HTTP automática ante eventos (DLR) |
| JWT | JSON Web Token, mecanismo de autenticación para el panel admin |
| RBAC | Role-Based Access Control, control de acceso basado en roles |
