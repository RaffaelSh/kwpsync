require('dotenv').config();
const express = require('express');
const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');

// --- Config ---
const supa = createClient(process.env.SUPA_URL, process.env.SUPA_SERVICE_KEY);
const poolPromise = new sql.ConnectionPool({
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options: { encrypt: false, trustServerCertificate: true },
}).connect();

// --- Helpers ---
function mapStatus(r) {
  const a = r.AAuftragStatus, b = r.BAuftragStatus, c = r.AuftragStatus;
  if (a === 12 && b === 99 && c === 8) return 'Auftrag eingegangen';
  if (a === 5  && b === 99 && c === 1) return 'Bauvorh. wird nicht ausgeführt';
  if (a === 6  && b === 99 && c === 2) return 'Bauvorh. neu ausgeschrieben';
  if (a === 7  && b === 99 && c === 3) return 'Bauvorhmit Ersatzangebot';
  if (a === 3  && b === 99 && (c === 1 || c === 9)) return 'Auftrag abgeschlossen';
  if (a === 0  && b === 99 && c === 0) return 'Auftrag noch nicht vergeben';
  if (a === 12 && b === 99 && c === 7) return 'Auftrag zugesagt';
  if (a === 99 && b === 1  && c === 8) return 'Kostensammler';
  if (a === 4  && b === 99 && c === 1) return 'Auftrag nicht erhalten';
  if (a === 8  && b === 99 && c === 4) return 'Auftragsvergabe zurückgestellt';
  if (a === 9  && b === 99 && c === 5) return 'Auftrag nicht erhalten, zu teuer';
  if (a === 10 && b === 99 && c === 6) return 'Auftrag nicht erhalten, sonstige Gründe';
  return 'Auftrag nicht vergeben';
}

const toISO = (v) => (v == null || v === '' ? null : new Date(v).toISOString());

// --- Pull: MSSQL -> Supabase ---
async function syncToSupabase() {
  const pool = await poolPromise;
  const res = await pool.request().query(`
    SELECT
      p.ProjNr, p.ProjBezeichnung, p.ProjAdr, p.RechAdr, p.BauHrAdr,
      p.AbtNr, p.SachBearb, p.AuftragsSumme, p.Beginn,
      p.AAuftragStatus, p.BAuftragStatus, p.AuftragStatus,
      CONCAT(pa.Name, ' ', pa.Vorname, ', ', pa.Strasse, ', ', po.PLZ, ' ', po.Ort) AS ProjInfos,
      CONCAT(ra.Name, ' ', ra.Vorname, ', ', ra.Strasse, ', ', ro.PLZ, ' ', ro.Ort) AS RechInfos,
      CONCAT(ba.Name, ' ', ba.Vorname, ', ', ba.Strasse, ', ', bo.PLZ, ' ', bo.Ort) AS BauHrInfos,
      pa.Vorname, pa.Name, pa.Strasse, po.Ort, po.PLZ, pa.RechnungsMail
    FROM dbo.Projekt p
    LEFT JOIN adrAdressen pa ON p.ProjAdr = pa.AdrNrGes
    LEFT JOIN adrOrte     po ON pa.Ort   = po.OrtID
    LEFT JOIN adrAdressen ra ON p.RechAdr = ra.AdrNrGes
    LEFT JOIN adrOrte     ro ON ra.Ort   = ro.OrtID
    LEFT JOIN adrAdressen ba ON p.BauHrAdr = ba.AdrNrGes
    LEFT JOIN adrOrte     bo ON ba.Ort     = bo.OrtID;
  `);

  const rows = res.recordset
    .filter((r) => r.ProjNr)
    .map((r) => ({
      projnr: String(r.ProjNr),
      projbezeichnung: r.ProjBezeichnung ?? null,
      statusse: mapStatus(r),
      projadr: r.ProjAdr ?? null,
      rechadr: r.RechAdr ?? null,
      bauhradr: r.BauHrAdr ?? null,
      abtnr: r.AbtNr ?? null,
      sachbearb: r.SachBearb ?? null,
      auftragssumme: r.AuftragsSumme ?? null,
      beginn: toISO(r.Beginn),
      projinfos: r.ProjInfos ?? null,
      rechinfos: r.RechInfos ?? null,
      bauhrinfos: r.BauHrInfos ?? null,
      vorname: r.Vorname ?? null,
      name: r.Name ?? null,
      strasse: r.Strasse ?? null,
      ort: r.Ort ?? null,
      plz: r.PLZ ?? null,
      rechnungsmail: r.RechnungsMail ?? null,
    }));

  const batch = 500;
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    const { error } = await supa.from('projekt').upsert(chunk, { onConflict: 'projnr' });
    if (error) throw error;
  }
  return rows.length;
}

// --- Push: Supabase/CRM -> MSSQL ---
async function upsertToMSSQL(items) {
  if (!items.length) return 0;
  const pool = await poolPromise;

  const table = new sql.Table('Projekt');
  table.columns.add('ProjNr', sql.NVarChar(50), { nullable: false });
  table.columns.add('ProjBezeichnung', sql.NVarChar(255), { nullable: true });
  table.columns.add('ProjAdr', sql.Int, { nullable: true });
  table.columns.add('RechAdr', sql.Int, { nullable: true });
  table.columns.add('BauHrAdr', sql.Int, { nullable: true });
  table.columns.add('AbtNr', sql.Int, { nullable: true });
  table.columns.add('SachBearb', sql.NVarChar(50), { nullable: true });
  table.columns.add('AuftragsSumme', sql.Numeric(18, 2), { nullable: true });
  table.columns.add('Beginn', sql.DateTimeOffset, { nullable: true });

  for (const r of items) {
    table.rows.add(
      r.projnr,
      r.projbezeichnung ?? null,
      r.projadr ?? null,
      r.rechadr ?? null,
      r.bauhradr ?? null,
      r.abtnr ?? null,
      r.sachbearb ?? null,
      r.auftragssumme ?? null,
      r.beginn ? new Date(r.beginn) : null
    );
  }

  const tmp = '#tmp_proj';
  await pool.request().batch(`
    IF OBJECT_ID('tempdb..${tmp}') IS NOT NULL DROP TABLE ${tmp};
    CREATE TABLE ${tmp} (
      ProjNr NVARCHAR(50) PRIMARY KEY,
      ProjBezeichnung NVARCHAR(255) NULL,
      ProjAdr INT NULL,
      RechAdr INT NULL,
      BauHrAdr INT NULL,
      AbtNr INT NULL,
      SachBearb NVARCHAR(50) NULL,
      AuftragsSumme NUMERIC(18,2) NULL,
      Beginn DATETIMEOFFSET NULL
    );
  `);

  await pool.request().bulk(table, { keepNulls: true, table: tmp });
  await pool.request().batch(`
    MERGE dbo.Projekt AS t
    USING ${tmp} AS s ON t.ProjNr = s.ProjNr
    WHEN MATCHED THEN UPDATE SET
      ProjBezeichnung = s.ProjBezeichnung,
      ProjAdr = s.ProjAdr,
      RechAdr = s.RechAdr,
      BauHrAdr = s.BauHrAdr,
      AbtNr = s.AbtNr,
      SachBearb = s.SachBearb,
      AuftragsSumme = s.AuftragsSumme,
      Beginn = s.Beginn
    WHEN NOT MATCHED THEN INSERT (
      ProjNr, ProjBezeichnung, ProjAdr, RechAdr, BauHrAdr,
      AbtNr, SachBearb, AuftragsSumme, Beginn
    ) VALUES (
      s.ProjNr, s.ProjBezeichnung, s.ProjAdr, s.RechAdr, s.BauHrAdr,
      s.AbtNr, s.SachBearb, s.AuftragsSumme, s.Beginn
    );
    DROP TABLE ${tmp};
  `);
  return items.length;
}

// --- API ---
const app = express();
app.use(express.json({ limit: '5mb' }));

app.post('/sync/pull', async (_req, res) => {
  try { res.json({ ok: true, count: await syncToSupabase() }); }
  catch (e) { console.error(e); res.status(500).json({ ok: false, error: e.message }); }
});

app.post('/sync/push', async (req, res) => {
  try { res.json({ ok: true, count: await upsertToMSSQL(req.body.rows || []) }); }
  catch (e) { console.error(e); res.status(500).json({ ok: false, error: e.message }); }
});

const port = process.env.PORT || 4000;
app.listen(port, () => console.log(`kwp-sync-api listening on :${port}`));
