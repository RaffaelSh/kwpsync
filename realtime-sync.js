require('dotenv').config();
const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');

const QUEUE_SCHEMA = process.env.KWP_QUEUE_SCHEMA || 'public';
const QUEUE_TABLE = process.env.KWP_QUEUE_TABLE || 'kwp_project_queue';
const TEMPLATE_PROJNR = (process.env.KWP_TEMPLATE_PROJNR || '').trim() || null;
const POLL_INTERVAL_MS = Number.parseInt(process.env.KWP_QUEUE_POLL_MS || '30000', 10);
const POLL_LIMIT = Number.parseInt(process.env.KWP_QUEUE_POLL_LIMIT || '50', 10);

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

const fitString = (v, maxLen) => {
  if (v == null || v === '') return null;
  const s = String(v);
  return maxLen ? s.slice(0, maxLen) : s;
};

const toFloat = (v) => {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function parsePayload(row) {
  if (!row) return null;
  const payload = row.payload ?? row;
  if (!payload) return null;
  if (typeof payload === 'string') {
    try {
      return JSON.parse(payload);
    } catch {
      return null;
    }
  }
  return payload;
}

function buildAdrKey(base, suffix) {
  const clean = String(base || '').replace(/\s+/g, '');
  const suffixPart = suffix ? `_${suffix}` : '';
  const maxBaseLen = 24 - suffixPart.length;
  return fitString(clean, maxBaseLen) + suffixPart;
}

function normalizeAddress(raw, fallbackKey) {
  if (!raw || typeof raw !== 'object') return null;
  return {
    adrNrGes: fitString(raw.adrNrGes || fallbackKey, 24),
    name: fitString(raw.name, 100),
    vorname: fitString(raw.vorname, 100),
    strasse: fitString(raw.strasse, 80),
    plz: fitString(raw.plz, 16),
    ort: fitString(raw.ort, 80),
    rechnungsmail: fitString(raw.rechnungsmail, 510),
    land: fitString(raw.land || 'DE', 3),
  };
}

async function ensureOrt(trx, address) {
  if (!address.plz || !address.ort) {
    throw new Error('Adresse muss PLZ und Ort enthalten.');
  }
  const request = new sql.Request(trx);
  request.input('Plz', sql.NVarChar(16), address.plz);
  request.input('Ort', sql.NVarChar(80), address.ort);
  const existing = await request.query(
    'SELECT OrtID FROM dbo.adrOrte WHERE PLZ = @Plz AND Ort = @Ort'
  );
  if (existing.recordset.length) {
    return existing.recordset[0].OrtID;
  }
  const nextReq = new sql.Request(trx);
  const nextIdRes = await nextReq.query('SELECT ISNULL(MAX(OrtID), 0) + 1 AS NextId FROM dbo.adrOrte');
  const nextId = nextIdRes.recordset[0].NextId;
  const insertReq = new sql.Request(trx);
  insertReq.input('OrtID', sql.Int, nextId);
  insertReq.input('Land', sql.NVarChar(3), address.land || 'DE');
  insertReq.input('Plz', sql.NVarChar(16), address.plz);
  insertReq.input('Plzn', sql.NVarChar(16), address.plz);
  insertReq.input('Ort', sql.NVarChar(80), address.ort);
  insertReq.input('OrtTyp', sql.Int, 0);
  await insertReq.query(
    'INSERT INTO dbo.adrOrte (OrtID, Land, PLZ, PLZN, Ort, OrtTyp) VALUES (@OrtID, @Land, @Plz, @Plzn, @Ort, @OrtTyp)'
  );
  return nextId;
}

async function ensureAdresse(trx, address) {
  if (!address?.adrNrGes) {
    throw new Error('AdrNrGes fehlt.');
  }
  const ortId = await ensureOrt(trx, address);
  const checkReq = new sql.Request(trx);
  checkReq.input('AdrNrGes', sql.NVarChar(24), address.adrNrGes);
  const exists = await checkReq.query('SELECT AdrNrGes FROM dbo.adrAdressen WHERE AdrNrGes = @AdrNrGes');
  if (exists.recordset.length) {
    return address.adrNrGes;
  }
  const insertReq = new sql.Request(trx);
  insertReq.input('AdrNrGes', sql.NVarChar(24), address.adrNrGes);
  insertReq.input('Name', sql.NVarChar(100), address.name);
  insertReq.input('Vorname', sql.NVarChar(100), address.vorname);
  insertReq.input('Strasse', sql.NVarChar(80), address.strasse);
  insertReq.input('Ort', sql.Int, ortId);
  insertReq.input('RechnungsMail', sql.NVarChar(510), address.rechnungsmail);
  insertReq.input('MahnSperre', sql.Bit, 0);
  insertReq.input('MwStPflicht', sql.Bit, 1);
  await insertReq.query(
    'INSERT INTO dbo.adrAdressen (AdrNrGes, Name, Vorname, Strasse, Ort, RechnungsMail, MahnSperre, MwStPflicht) VALUES (@AdrNrGes, @Name, @Vorname, @Strasse, @Ort, @RechnungsMail, @MahnSperre, @MwStPflicht)'
  );
  return address.adrNrGes;
}

async function insertProjektFromTemplate(trx, payload) {
  const projnr = fitString(payload.projnr, 30);
  if (!projnr) throw new Error('projnr fehlt.');

  const baseKey = payload.adresse?.adrNrGes || buildAdrKey(projnr, 'A');
  const baseAddress = normalizeAddress(payload.adresse, baseKey);
  if (!baseAddress?.name || !baseAddress?.strasse || !baseAddress?.plz || !baseAddress?.ort) {
    throw new Error('adresse (name, strasse, plz, ort) fehlt.');
  }

  const rechnungSame = payload.rechnungAdresse?.sameAsAdresse !== false;
  const bauherrSame = payload.bauherrAdresse?.sameAsAdresse !== false;

  const rechnungAddress = rechnungSame
    ? baseAddress
    : normalizeAddress(payload.rechnungAdresse, buildAdrKey(baseKey, 'R'));
  const bauherrAddress = bauherrSame
    ? baseAddress
    : normalizeAddress(payload.bauherrAdresse, buildAdrKey(baseKey, 'B'));

  const poolRequest = new sql.Request(trx);
  poolRequest.input('ProjNr', sql.NVarChar(30), projnr);
  const exists = await poolRequest.query('SELECT 1 FROM dbo.Projekt WHERE ProjNr = @ProjNr');
  if (exists.recordset.length) {
    return { status: 'exists', projnr };
  }

  const projAdr = await ensureAdresse(trx, baseAddress);
  const rechAdr = await ensureAdresse(trx, rechnungAddress || baseAddress);
  const bauAdr = await ensureAdresse(trx, bauherrAddress || baseAddress);

  const now = new Date();
  const sachbearb = fitString(payload.sachbearb, 40);
  const abtnr = toFloat(payload.abtnr);
  const auftragStatus = payload.auftragStatus != null ? Number(payload.auftragStatus) : null;

  const insertReq = new sql.Request(trx);
  insertReq.input('ProjNr', sql.NVarChar(30), projnr);
  insertReq.input('ProjBezeichnung', sql.NVarChar(sql.MAX), payload.projbezeichnung ?? null);
  insertReq.input('ProjAdr', sql.NVarChar(24), projAdr);
  insertReq.input('RechAdr', sql.NVarChar(24), rechAdr);
  insertReq.input('BauHrAdr', sql.NVarChar(24), bauAdr);
  insertReq.input('ArchAdr', sql.NVarChar(24), null);
  insertReq.input('Now', sql.SmallDateTime, now);
  insertReq.input('AbtNr', sql.Float, abtnr);
  insertReq.input('SachBearb', sql.NVarChar(40), sachbearb);
  insertReq.input('AuftragStatus', sql.SmallInt, auftragStatus);
  insertReq.input('CreateUser', sql.NVarChar(16), fitString(payload.createuser || sachbearb, 16));
  insertReq.input('EditUser', sql.NVarChar(16), fitString(payload.edituser || sachbearb, 16));
  if (TEMPLATE_PROJNR) {
    insertReq.input('TemplateProjNr', sql.NVarChar(30), TEMPLATE_PROJNR);
  }

  const templateWhere = TEMPLATE_PROJNR
    ? 'WHERE ProjNr = @TemplateProjNr'
    : 'ORDER BY Createdate DESC';

  const insertSql = `
    INSERT INTO dbo.Projekt (
      ProjNr, ProjAnlage, ProjAdr, RechAdr, ArchAdr, BauHrAdr,
      KoArt, RohstPr, LohnVorg, VerarbVoreinst, Sperrvermerk,
      Beginn, FertigStellung, AuftragStatus, CheckDate, AbtNr, Waehrung, AuftragsNr, SachBearb,
      CheckOut, eCheck, doPosition, doAufmass, doBstLager, doPosMat, upsize_ts,
      FestPreis, FestPreisMaterial, FestPreisLohn, Info1, Info2, Info3, MittelLohn, SubProjekt,
      Auftraggeber, Kategorie, AuftragsDatum, AuftragsSumme, AAuftragStatus, BAuftragStatus,
      Vertrieb, Passwort, SymbolIndex, Direktlieferung,
      Benutzer1, Benutzer2, Benutzer3, Benutzer4, Benutzer5, Benutzer6, Benutzer7, Benutzer8, Benutzer9, Benutzer10,
      Submissionsdatum, Fertigstellungsgrad, Version, Nachlass, NachlassProzent, NachlassPauschal,
      Createuser, Createdate, Edituser, Editdate,
      Gemeinkosten, Ansprechpartner, GrundlageSE, ProzentSE, Qualifikation, ProjBezeichnung, KonvertierungsFlag,
      fkAnsprechpartnerIDProjAdr, fkAnsprechpartnerIDRechAdr, fkAnsprechpartnerIDBauhAdr, fkAnsprechpartnerIDArchAdr,
      AnlagenNr, IgnoreProjektampel, GrundlageSEAZ, ProzentSEAZ, SteuerSchl, Handelsspannenkalkulation,
      EndberechnungenIstNettosumme, KalkulationEinstellungen
    )
    SELECT TOP 1
      @ProjNr, @Now, @ProjAdr, @RechAdr, @ArchAdr, @BauHrAdr,
      KoArt, RohstPr, LohnVorg, VerarbVoreinst, Sperrvermerk,
      Beginn, FertigStellung, COALESCE(@AuftragStatus, AuftragStatus), @Now, COALESCE(@AbtNr, AbtNr), Waehrung, AuftragsNr,
      COALESCE(@SachBearb, SachBearb),
      CheckOut, eCheck, doPosition, doAufmass, doBstLager, doPosMat, upsize_ts,
      FestPreis, FestPreisMaterial, FestPreisLohn, Info1, Info2, Info3, MittelLohn, SubProjekt,
      Auftraggeber, Kategorie, AuftragsDatum, AuftragsSumme, AAuftragStatus, BAuftragStatus,
      Vertrieb, Passwort, SymbolIndex, Direktlieferung,
      Benutzer1, Benutzer2, Benutzer3, Benutzer4, Benutzer5, Benutzer6, Benutzer7, Benutzer8, Benutzer9, Benutzer10,
      Submissionsdatum, Fertigstellungsgrad, Version, Nachlass, NachlassProzent, NachlassPauschal,
      COALESCE(@CreateUser, @SachBearb, Createuser), @Now, COALESCE(@EditUser, @SachBearb, Edituser), @Now,
      Gemeinkosten, Ansprechpartner, GrundlageSE, ProzentSE, Qualifikation, @ProjBezeichnung, KonvertierungsFlag,
      -1, -1, -1, -1,
      AnlagenNr, IgnoreProjektampel, GrundlageSEAZ, ProzentSEAZ, SteuerSchl, Handelsspannenkalkulation,
      EndberechnungenIstNettosumme, KalkulationEinstellungen
    FROM dbo.Projekt
    ${templateWhere};
  `;

  const result = await insertReq.query(insertSql);
  if (result.rowsAffected[0] !== 1) {
    throw new Error('Template-Projekt nicht gefunden oder Insert fehlgeschlagen.');
  }
  return { status: 'inserted', projnr };
}

const queue = [];
let processing = false;
const enqueuedIds = new Set();

async function updateQueueRow(id, values) {
  if (!id) return;
  const { error } = await supa
    .schema(QUEUE_SCHEMA)
    .from(QUEUE_TABLE)
    .update(values)
    .eq('id', id);
  if (error) {
    console.error('Queue status update failed:', error.message);
  }
}

async function handleQueueItem(row) {
  const payload = parsePayload(row);
  const id = row.id || payload?.id;
  const attemptCount = (row.attempt_count || 0) + 1;

  await updateQueueRow(id, {
    status: 'processing',
    attempt_count: attemptCount,
  });

  const pool = await poolPromise;
  const trx = new sql.Transaction(pool);
  try {
    await trx.begin();
    const result = await insertProjektFromTemplate(trx, payload);
    await trx.commit();
    await updateQueueRow(id, {
      status: 'done',
      processed_at: new Date().toISOString(),
      error: result.status === 'exists' ? 'ProjNr exists, skipped insert.' : null,
    });
    console.log(`Queue item ${id}: ${result.status} (${result.projnr})`);
  } catch (err) {
    try {
      await trx.rollback();
    } catch (_) {
      // ignore rollback errors
    }
    await updateQueueRow(id, {
      status: 'error',
      processed_at: new Date().toISOString(),
      error: fitString(err.message || String(err), 2000),
    });
    console.error('Queue processing error:', err);
  } finally {
    if (id) enqueuedIds.delete(id);
  }
}

function enqueue(row) {
  if (!row?.id) return;
  if (enqueuedIds.has(row.id)) return;
  enqueuedIds.add(row.id);
  queue.push(row);
  processQueue();
}

async function processQueue() {
  if (processing) return;
  processing = true;
  try {
    while (queue.length) {
      const row = queue.shift();
      await handleQueueItem(row);
    }
  } finally {
    processing = false;
  }
}

async function fetchPendingQueue() {
  const { data, error } = await supa
    .schema(QUEUE_SCHEMA)
    .from(QUEUE_TABLE)
    .select('*')
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(POLL_LIMIT);
  if (error) {
    console.error('Pending fetch error:', error.message);
    return;
  }
  for (const row of data || []) {
    enqueue(row);
  }
}

function startPolling() {
  if (!Number.isFinite(POLL_INTERVAL_MS) || POLL_INTERVAL_MS <= 0) return;
  setInterval(fetchPendingQueue, POLL_INTERVAL_MS);
}

console.log(`Realtime queue starting for ${QUEUE_SCHEMA}.${QUEUE_TABLE}...`);
fetchPendingQueue();
startPolling();

supa
  .channel(`realtime:${QUEUE_SCHEMA}:${QUEUE_TABLE}`)
  .on('postgres_changes', { event: 'INSERT', schema: QUEUE_SCHEMA, table: QUEUE_TABLE }, (payload) => {
    if (!payload?.new) return;
    if (payload.new.status && payload.new.status !== 'pending') return;
    enqueue(payload.new);
  })
  .on('postgres_changes', { event: 'UPDATE', schema: QUEUE_SCHEMA, table: QUEUE_TABLE }, (payload) => {
    if (!payload?.new) return;
    if (payload.new.status !== 'pending') return;
    enqueue(payload.new);
  })
  .subscribe((status) => {
    console.log('Realtime status:', status);
  });
