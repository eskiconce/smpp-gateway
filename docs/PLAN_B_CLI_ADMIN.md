# Plan B — CLI de Administración (Plan de Implementación)

**Objetivo**: Crear una CLI (`smppgw-admin`) que permita administrar la plataforma completamente por consola, como respaldo del panel web.

**Justificación**: Si el frontend web falla (nginx caído, problema de SSL, error de build), el operador necesita una forma de:
- Ver/enviar mensajes
- Gestionar tenants, conectores, reglas
- Consultar métricas y logs
- Crear usuarios
- Verificar estado de servicios

---

## Arquitectura

```
smppgw-admin (CLI)
  │
  ├── Conecta a la API HTTP existente (misma :8080)
  │   └── Usa los mismos endpoints REST que el frontend
  │
  ├── No necesita acceso directo a BD/Redis
  │   └── Todo pasa por la API (single source of truth)
  │
  └── Binario estático (CGO_ENABLED=0)
      └── Se distribuye junto a smppgw
```

**Decisión clave**: La CLI es un **cliente HTTP** de la API existente, NO un accesso directo a BD. Esto:
- Mantiene una sola lógica de negocio en el backend
- No rompe abstracciones de seguridad (mismos permisos JWT)
- Es más fácil de mantener

---

## Estructura de comandos

```
smppgw-admin
├── auth
│   ├── login              # Login, guarda token en ~/.smppgw-admin/config.json
│   ├── logout             # Limpia token
│   └── whoami             # Muestra usuario actual y rol
│
├── messages
│   ├── list               # Listar mensajes con filtros
│   │   --tenant ID          Filtrar por tenant
│   │   --from DATE           Desde fecha
│   │   --to DATE             Hasta fecha
│   │   --state STATE         buffered|accepted|delivered|expired|undeliv|rejected|failed
│   │   --msisdn NUMBER       Filtrar por destino
│   │   --limit N             Límite de resultados (default 50)
│   │   --offset N            Offset para paginación
│   │   --format table|json   Formato de salida (default table)
│   │
│   ├── get ID             # Ver detalle de un mensaje específico
│   │
│   ├── send               # Enviar SMS
│   │   --tenant ID          Tenant que envía
│   │   --to NUMBER          Destino (MSISDN)
│   │   --text "mensaje"     Texto del mensaje
│   │   --from "sender"      Sender (opcional)
│   │   --tag "tag"          Routing tag (opcional)
│   │
│   └── stats              # Estadísticas de mensajes
│       --tenant ID          Filtrar por tenant
│       --from DATE          Desde
│       --to DATE            Hasta
│
├── tenants
│   ├── list               # Listar todos los tenants
│   │   --format table|json
│   │
│   ├── get ID             # Detalle de tenant
│   │
│   ├── create             # Crear tenant
│   │   --name "nombre"       Nombre del tenant
│   │   --mode prepaid|postpaid  Modo de facturación
│   │   --balance N           Saldo inicial (solo prepaid)
│   │   --rate-limit N        Límite de mensajes por segundo
│   │   --msg-rate N          Mensajes por minuto
│   │
│   ├── update ID          # Actualizar tenant
│   │   --name "nuevo"
│   │   --mode prepaid|postpaid
│   │   --balance N
│   │   --status active|inactive
│   │   --rate-limit N
│   │
│   └── delete ID          # Eliminar tenant (con confirmación)
│
├── connectors
│   ├── list               # Listar conectores
│   │   --format table|json
│   │
│   ├── get ID             # Detalle de conector
│   │
│   ├── create             # Crear conector
│   │   --name "nombre"
│   │   --host HOST
│   │   --port PORT
│   │   --system-id "id"
│   │   --password "pass"
│   │   --max-binds N       Máximo de binds simultáneos
│   │
│   ├── update ID
│   │   --name --host --port --system-id --password --max-binds
│   │
│   └── delete ID
│
├── groups
│   ├── list
│   ├── get ID
│   ├── create
│   │   --name "nombre"
│   ├── add-member GROUP_ID CONNECTOR_ID
│   │   --weight N
│   ├── remove-member GROUP_ID CONNECTOR_ID
│   └── delete ID
│
├── rules
│   ├── list
│   │   --tenant ID
│   ├── get ID
│   ├── create
│   │   --tenant ID
│   │   --name "nombre"
│   │   --priority N
│   │   --prefix "569"
│   │   --from "sender"
│   │   --from-regex "^569"
│   │   --routing-tag "tag"
│   │   --connector-id ID
│   │   --group-id ID
│   │   --default true|false
│   ├── update ID
│   └── delete ID
│
├── rates
│   ├── list               # Listar tablas de tarifas
│   ├── get TABLE_ID       # Detalle con entradas
│   ├── create-table
│   │   --name "nombre"
│   ├── add-entry TABLE_ID
│   │   --prefix "569"
│   │   --price N
│   │   --currency "CLP"
│   │   --segment-size 160
│   └── delete-table ID
│
├── users
│   ├── list
│   ├── get ID
│   ├── create
│   │   --username "user"
│   │   --password "pass"
│   │   --role superadmin|admin|operator
│   ├── update ID
│   │   --password "newpass"
│   │   --role ROLE
│   └── delete ID
│
├── webhooks
│   ├── list
│   ├── get ID
│   ├── create
│   │   --tenant ID
│   │   --url "https://..."
│   │   --events "delivered,failed"
│   └── delete ID
│
├── status
│   ├── health             # Verificar salud del gateway
│   ├── metrics            # Métricas en tiempo real
│   ├── sessions           # Sesiones SMPP activas
│   └── queues             # Estado de colas Redis
│
└── logs
    ├── tail               # Últimas líneas de log
    │   --level info|warn|error
    │   --follow           Modo tail -f
    └── search "keyword"   Buscar en logs
```

---

## Ejemplos de uso

```bash
# Login
$ smppgw-admin auth login --user admin --password admin123 --url https://192.168.1.246
Token guardado en ~/.smppgw-admin/config.json

# Ver estado
$ smppgw-admin status health
Gateway: OK | API: :8080 | Uptime: 2h 15m | Msgs: 1,234

# Enviar SMS
$ smppgw-admin messages send \
  --tenant 1 \
  --to 56912345678 \
  --text "Hola mundo" \
  --from "PACIFICO"
Mensaje enviado: ID=abc-123 segments=1

# Listar mensajes fallidos
$ smppgw-admin messages list --state undeliv --limit 10
ID        | TO          | STATE  | CREATED       | SMSID
def-456   | 56987654321 | undeliv| 2026-09-16 10:30 | smsc-789

# Crear tenant
$ smppgw-admin tenants create \
  --name "Cliente Demo" \
  --mode prepaid \
  --balance 10000
Tenant creado: ID=3

# Verificar colas
$ smppgw-admin status queues
Queue: con:1:high  → 5 items
Queue: con:1:low   → 0 items
Queue: con:2:high  → 0 items

# Seguir logs en tiempo real
$ smppgw-admin logs tail --level warn --follow
```

---

## Implementación

### Dependencias (solo 1 extra)

```go
// cmd/smppgw-admin/main.go
import (
    "net/http"
    "encoding/json"
    "flag"
    "fmt"
    "os"
    "path/filepath"
)
```

**NO usar cobra/urfave** — mantenerlo simple con `flag` stdlib. Un solo binario sin dependencias externas (ya que la API es simple).

### Estructura de archivos

```
cmd/
├── smppgw/
│   └── main.go          # existente (server/connector)
└── smppgw-admin/
    └── main.go          # NUEVO — CLI completo en un solo archivo

internal/
└── admin/               # NUEVO — helper HTTP client
    └── client.go        # Funciones: login, request, format
```

### Config

`~/.smppgw-admin/config.json`:
```json
{
  "url": "https://192.168.1.246",
  "token": "eyJhbGciOi...",
  "user": "admin"
}
```

### Makefile targets

```makefile
build-admin:           # Compilar CLI
	go build -o bin/smppgw-admin ./cmd/smppgw-admin

install-admin:         # Copiar a /usr/local/bin
	cp bin/smppgw-admin /usr/local/bin/
```

### systemadmin manual

Agregar sección al `MANUAL_DE_USO.md`:

```markdown
## 14. CLI de Administración (Plan B)

Cuando el panel web no está disponible, use la CLI:

### Instalación
go install ./cmd/smppgw-admin
# o desde binario compilado
cp bin/smppgw-admin /usr/local/bin/

### Autenticación
smppgw-admin auth login --user admin --password admin123 --url https://IP

### Comandos disponibles
[lista completa]
```

---

## Prioridad de implementación

| Fase | Comandos | Esfuerzo |
|------|----------|----------|
| **Fase 1** | `auth/login`, `status/health`, `messages/list`, `messages/send` | 1-2h |
| **Fase 2** | `tenants/*`, `connectors/*` CRUD | 1-2h |
| **Fase 3** | `groups/*`, `rules/*`, `rates/*` CRUD | 1-2h |
| **Fase 4** | `users/*`, `webhooks/*`, `logs/*`, `status/queues` | 1h |

**Total estimado**: 4-7 horas de implementación

---

## Verificación

```bash
# 1. Compilar
cd /root/smpp-gateway && go build -o bin/smppgw-admin ./cmd/smppgw-admin

# 2. Login
./bin/smppgw-admin auth login --user admin --password admin123 --url https://192.168.1.246

# 3. Verificar health
./bin/smppgw-admin status health

# 4. Enviar SMS de prueba
./bin/smppgw-admin messages send --tenant 1 --to 56912345678 --text "Prueba CLI"

# 5. Listar mensajes
./bin/smppgw-admin messages list --limit 5

# 6. Crear tenant
./bin/smppgw-admin tenants create --name "CLI Test" --mode prepaid --balance 1000

# 7. Verificar en web que todo coincide
```
