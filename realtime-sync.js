require('dotenv').config();
const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');

const supa = createClient(process.env.SUPA_URL, process.env.SUPA_SERVICE_KEY, {
  realtime: {
    params: {
      eventsPerSecond: 10,
    },
  },
});

const poolPromise = new sql.ConnectionPool({
  server: process.env.MSSQL_SERVER,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options: { encrypt: false, trustServerCertificate: true },
}).connect();

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

const queue = [];
let processing = false;

function normalizeRow(row) {
  if (!row || !row.projnr) return null;
  return {
    projnr: String(row.projnr),
    projbezeichnung: row.projbezeichnung ?? null,
    projadr: row.projadr ?? null,
    rechadr: row.rechadr ?? null,
    bauhradr: row.bauhradr ?? null,
    abtnr: row.abtnr ?? null,
    sachbearb: row.sachbearb ?? null,
    auftragssumme: row.auftragssumme ?? null,
    beginn: row.beginn ?? null,
  };
}

async function processQueue() {
  if (processing || queue.length === 0) return;
  processing = true;
  try {
    const batch = queue.splice(0, queue.length);
    await upsertToMSSQL(batch);
    console.log(`Realtime sync: ${batch.length} row(s) upserted.`);
  } catch (err) {
    console.error('Realtime sync error:', err);
  } finally {
    processing = false;
  }
}

console.log('Realtime sync starting...');

supa
  .channel('realtime:public:projekt')
  .on('postgres_changes', { event: '*', schema: 'public', table: 'projekt' }, (payload) => {
    if (payload.eventType === 'DELETE') {
      console.log('Realtime delete ignored:', payload.old?.projnr || payload.old?.ProjNr);
      return;
    }
    const row = normalizeRow(payload.new);
    if (!row) return;
    queue.push(row);
    processQueue();
  })
  .subscribe((status) => {
    console.log('Realtime status:', status);
  });
