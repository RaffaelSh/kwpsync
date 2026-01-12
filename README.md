# KWP ⇄ Supabase Sync API (Express)

## Struktur
- `server.js` – Express-API mit zwei Endpoints
  - `POST /sync/pull`  → KWP (MSSQL) → Supabase (Upsert `projekt`)
  - `POST /sync/push`  → Supabase/CRM → KWP (MERGE in `dbo.Projekt`)
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
   SUPA_URL=https://<your-supabase>.supabase.co
   SUPA_SERVICE_KEY=service_role_key
   PORT=4000
   ```
3. Abhängigkeiten installieren:
   ```
   npm install
   ```
4. Starten:
   ```
   npm start
   ```

## Endpoints
- `POST /sync/pull`
  - Holt alle Projekte aus MSSQL und upsertet sie in Supabase-Tabelle `projekt` (onConflict `projnr`).
- `POST /sync/push`
  - Erwartet JSON `{ "rows": [ {proj...} ] }` und MERGEt in MSSQL `dbo.Projekt` nach `ProjNr`.
  - Beispiel-Payload:
    ```json
    {
      "rows": [
        {
          "projnr": "P-10001",
          "projbezeichnung": "Neues Bad",
          "projadr": 1234,
          "rechadr": 1235,
          "bauhradr": 1236,
          "abtnr": 2,
          "sachbearb": "NG",
          "auftragssumme": 15000,
          "beginn": "2024-12-01T00:00:00Z"
        }
      ]
    }
    ```

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
- CRM ruft nach Speichern sofort `POST /sync/push` auf → KWP wird direkt geschrieben.
- Wenn KWP-Änderungen sofort in Supabase landen sollen, einen Trigger/Job nutzen, der `POST /sync/pull` aufruft (oder kurzer Task-Scheduler-Intervall).

## Dienst auf Windows (optional mit PM2)
```powershell
npm install -g pm2
pm2 start server.js --name kwp-sync-api
pm2 save
pm2 startup windows
```

## Sicherheit
- Service Key nur im Backend/.env, nicht im Browser.
- MSSQL-Verbindung in sicherem Netz; für TLS `encrypt: true` + gültiges Zertifikat nutzen.
- Firewall: Port 4000 nur intern freigeben.
