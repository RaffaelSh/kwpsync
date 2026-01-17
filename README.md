# KWP ⇄ Supabase Sync API (Express)

## Struktur
- `server.js` – Express-API mit zwei Endpoints
  - `POST /sync/pull`  → KWP (MSSQL) → Supabase (Upsert `projekt`)
  - `POST /sync/push`  → Supabase/CRM → KWP (MERGE in `dbo.Projekt`)
- `realtime-sync.js` – Realtime-Subscriber (CRM → KWP), kein eingehender Port nötig (Queue-basiert)
- `.env.example` – Platzhalter für Zugangsdaten
- `package.json` – Abhängigkeiten: `express`, `mssql`, `@supabase/supabase-js`, `dotenv`

## Einrichtung (Windows oder macOS)
1. Node 20+ installieren.
2. `.env` aus `.env.example` kopieren und füllen:
   ```
   MSSQL_SERVER=192.168.254.22\KWP
   MSSQL_DB=BNWINS
   MSSQL_USER=sa
   MSSQL_PASS=***
   SUPA_URL=http://49.12.236.245:8000
   SUPA_SERVICE_KEY=service_role_key
   KWP_QUEUE_SCHEMA=public
   KWP_QUEUE_TABLE=kwp_project_queue
   KWP_TEMPLATE_PROJNR=
   KWP_TEMPLATE_ADRNR=
   KWP_QUEUE_POLL_MS=30000
   KWP_QUEUE_POLL_LIMIT=50
   PORT=4000
   ```
3. Abhängigkeiten installieren:
   ```
   npm install
   ```

## API (Pull/Push)
- Start:
  ```
  npm start
  ```
- Endpoints:
  - `POST /sync/pull`
  - `POST /sync/push`

## Realtime (CRM → KWP, ohne eingehenden Port)
Wir nutzen eine Queue-Tabelle in Supabase. CRM schreibt dort nur ein JSON-Payload hinein.

1. Queue-Tabelle anlegen:
   ```sql
   create table if not exists public.kwp_project_queue (
     id uuid primary key default gen_random_uuid(),
     created_at timestamptz not null default now(),
     status text not null default 'pending',
     payload jsonb not null,
     attempt_count int not null default 0,
     processed_at timestamptz,
     error text
   );
   ```
2. Realtime für die Queue aktivieren:
   ```sql
   alter table public.kwp_project_queue replica identity full;
   alter publication supabase_realtime add table public.kwp_project_queue;
   ```
3. Starten:
   ```
   npm run realtime
   ```
4. CRM-Insert (Beispiel):
   ```sql
   insert into public.kwp_project_queue (payload)
   values (
     '{
       "projnr": "HIVE2026000001",
       "projbezeichnung": "Heizungsmodernisierung Einfamilienhaus",
       "abtnr": 1,
       "sachbearb": "NG",
       "auftragStatus": 1,
       "adresse": {
         "adrNrGes": "HIVE2026000001",
         "name": "Mustermann GmbH",
       "vorname": "Max",
       "strasse": "Musterstrasse 1",
       "plz": "46509",
       "ort": "Musterstadt",
       "rechnungsmail": "rechnung@example.com",
       "kontakte": {
         "telefon": "02843-123456",
         "fax": "02843-654321",
         "mail": "rechnung@example.com"
       }
     },
     "rechnungAdresse": { "sameAsAdresse": true },
     "bauherrAdresse": { "sameAsAdresse": true }
   }'::jsonb
 );
   ```
5. Der Worker verarbeitet den Eintrag, schreibt nach MSSQL und setzt `status` auf `done` oder `error`.
   Zusätzlich gibt es ein Polling (alle 30s), falls Realtime/Websocket nicht erreichbar ist.
   Der Worker klont eine bestehende Adresse als Template. Optional `KWP_TEMPLATE_ADRNR` setzen.

## Tabelle in Supabase (optional: KWP → Supabase Pull)
Falls du weiterhin die Pull-Synchronisation nutzen willst:
```sql
create table if not exists public.projekt (
  projnr text primary key,
  projbezeichnung text,
  statusse text,
  projadr text,
  rechadr text,
  bauhradr text,
  abtnr integer,
  sachbearb text,
  auftragssumme numeric(18,2),
  beginn timestamptz,
  projinfos text,
  rechinfos text,
  bauhrinfos text,
  vorname text,
  name text,
  strasse text,
  ort text,
  plz text,
  rechnungsmail text
);
```

## Betrieb / "Instant"-Nutzung
- CRM schreibt in Supabase `kwp_project_queue`.
- Realtime-Prozess schreibt sofort nach MSSQL.
- KWP → Supabase bleibt via `POST /sync/pull` (z.B. per Scheduler) oder optional ebenfalls per Job.

## Dienst auf Windows (optional mit PM2)
```powershell
npm install -g pm2
pm2 start server.js --name kwp-sync-api
pm2 start realtime-sync.js --name kwp-sync-realtime
pm2 save
pm2 startup windows
```

## Sicherheit
- Service Key nur im Backend/.env, nicht im Browser.
- MSSQL-Verbindung in sicherem Netz; fuer TLS `encrypt: true` + gueltiges Zertifikat nutzen.
- Firewall: Port 4000 nur intern freigeben.
