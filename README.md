# KWP ⇄ Supabase Sync API (Express)

## Struktur
- `server.js` – Express-API mit zwei Endpoints
  - `POST /sync/pull`  → KWP (MSSQL) → Supabase (Upsert `projekt`)
  - `POST /sync/push`  → Supabase/CRM → KWP (MERGE in `dbo.Projekt`)
- `realtime-sync.js` – Realtime-Subscriber (CRM → KWP), kein eingehender Port nötig
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
1. Realtime für Tabelle aktivieren:
   ```sql
   alter table public.projekt replica identity full;
   alter publication supabase_realtime add table public.projekt;
   ```
2. Starten:
   ```
   npm run realtime
   ```
3. Der Prozess lauscht auf Änderungen in `public.projekt` und schreibt nach MSSQL.

## Tabelle in Supabase
Falls noch nicht vorhanden:
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
- CRM schreibt in Supabase `projekt`.
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
