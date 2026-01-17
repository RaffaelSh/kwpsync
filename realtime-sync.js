require('dotenv').config();
const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');

const QUEUE_SCHEMA = process.env.KWP_QUEUE_SCHEMA || 'public';
const QUEUE_TABLE = process.env.KWP_QUEUE_TABLE || 'kwp_project_queue';
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

const fitString = (v, maxLen, label) => {
  if (v == null || v === '') return null;
  const s = String(v);
  if (maxLen && s.length > maxLen) {
    throw new Error(`${label || 'Wert'} ist zu lang (max ${maxLen}).`);
  }
  return s;
};

const truncateString = (v, maxLen) => {
  if (v == null || v === '') return null;
  const s = String(v);
  return maxLen ? s.slice(0, maxLen) : s;
};

const toNumber = (v, label) => {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) {
    throw new Error(`${label || 'Wert'} ist keine Zahl.`);
  }
  return n;
};

let adrAdressenColumnsCache = null;
let projektColumnsCache = null;

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

const ADR_NR_MAX = 24;
const ADR_LEGAL_FORMS = new Set([
  'GMBH',
  'MBH',
  'KG',
  'UG',
  'AG',
  'EK',
  'GMBHCO',
  'CO',
]);

function normalizeAdrBase(value) {
  const raw = String(value || '');
  const cleaned = raw
    .replace(/&/g, ' UND ')
    .replace(/[Ää]/g, 'AE')
    .replace(/[Öö]/g, 'OE')
    .replace(/[Üü]/g, 'UE')
    .replace(/ß/g, 'SS')
    .replace(/[^A-Za-z0-9]+/g, ' ')
    .trim()
    .toUpperCase();
  if (!cleaned) return '';
  const tokens = cleaned.split(/\s+/).filter(Boolean);
  const filtered = tokens.filter((token) => !ADR_LEGAL_FORMS.has(token));
  return (filtered.length ? filtered : tokens).join('_');
}

function buildAdrBase(address) {
  if (!address || typeof address !== 'object') return '';
  const primary = address.name || address.strasse || address.vorname || '';
  return normalizeAdrBase(primary);
}

async function generateAdrNrGes(trx, address, typeTag) {
  const baseRaw = buildAdrBase(address);
  const suffix = `_${typeTag}`;
  const maxDigits = 4;
  const maxBaseLen = ADR_NR_MAX - suffix.length - maxDigits;
  const base = truncateString(baseRaw || 'ADRESSE', maxBaseLen);
  const prefix = `${base}${suffix}`;
  const req = new sql.Request(trx);
  req.input('Prefix', sql.NVarChar(48), `${prefix}%`);
  const res = await req.query('SELECT AdrNrGes FROM dbo.adrAdressen WHERE AdrNrGes LIKE @Prefix');
  let maxNum = 0;
  for (const row of res.recordset || []) {
    const value = row.AdrNrGes || '';
    if (!value.startsWith(prefix)) continue;
    const match = value.match(/(\d+)$/);
    if (!match) continue;
    const num = Number.parseInt(match[1], 10);
    if (Number.isFinite(num) && num > maxNum) maxNum = num;
  }
  const nextNum = maxNum + 1;
  let candidate = `${prefix}${nextNum}`;
  if (candidate.length > ADR_NR_MAX) {
    const allowedBaseLen = ADR_NR_MAX - suffix.length - String(nextNum).length;
    const trimmedBase = truncateString(base, allowedBaseLen);
    candidate = `${trimmedBase}${suffix}${nextNum}`;
  }
  return candidate;
}

async function ensureAdrNrGes(trx, raw, typeTag) {
  if (!raw || typeof raw !== 'object') return;
  if (Object.prototype.hasOwnProperty.call(raw, 'AdrNrGes')) return;
  if (Object.prototype.hasOwnProperty.call(raw, 'adrNrGes')) return;
  raw.AdrNrGes = await generateAdrNrGes(trx, raw, typeTag);
}

async function ensureOrt(trx, address) {
  if (!address.plz || !address.ort) {
    throw new Error('Adresse muss PLZ und Ort enthalten.');
  }
  const plzn = address.plzn || address.plz;
  const ortTyp = Number.isFinite(address.ortTyp) ? address.ortTyp : 0;
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
  insertReq.input('Plzn', sql.NVarChar(16), plzn);
  insertReq.input('Ort', sql.NVarChar(80), address.ort);
  insertReq.input('OrtTyp', sql.Int, ortTyp);
  await insertReq.query(
    'INSERT INTO dbo.adrOrte (OrtID, Land, PLZ, PLZN, Ort, OrtTyp) VALUES (@OrtID, @Land, @Plz, @Plzn, @Ort, @OrtTyp)'
  );
  return nextId;
}

async function getTableColumns(trx, tableName) {
  const cache = tableName === 'dbo.adrAdressen' ? adrAdressenColumnsCache : projektColumnsCache;
  if (cache) return cache;
  const req = new sql.Request(trx);
  const res = await req.query(`
    SELECT
      c.name AS column_name,
      t.name AS data_type,
      c.max_length,
      c.precision,
      c.scale,
      c.is_nullable,
      c.is_identity,
      c.is_computed,
      dc.definition AS default_definition
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
    WHERE c.object_id = OBJECT_ID('${tableName}')
    ORDER BY c.column_id
  `);
  const meta = res.recordset;
  if (tableName === 'dbo.adrAdressen') {
    adrAdressenColumnsCache = meta;
  } else if (tableName === 'dbo.Projekt') {
    projektColumnsCache = meta;
  }
  return meta;
}

function buildColumnMaps(columns) {
  const byLower = new Map();
  const metaByName = new Map();
  for (const col of columns) {
    const name = col.column_name;
    byLower.set(name.toLowerCase(), name);
    metaByName.set(name, col);
  }
  return { byLower, metaByName };
}

function isInsertableColumn(meta) {
  if (!meta) return false;
  if (meta.is_identity || meta.is_computed) return false;
  if (meta.data_type === 'timestamp') return false;
  if (meta.column_name === 'Offline_Sync_Id') return false;
  return true;
}

function getRequiredColumns(columns) {
  return columns
    .filter((col) => isInsertableColumn(col))
    .filter((col) => !col.is_nullable && !col.default_definition)
    .map((col) => col.column_name);
}

function normalizeValueForColumn(value, meta) {
  if (value == null) return null;
  const type = meta.data_type.toLowerCase();
  if (type === 'nvarchar' || type === 'varchar' || type === 'nchar' || type === 'char') {
    const maxLen = meta.max_length === -1 ? null : type.startsWith('n') ? meta.max_length / 2 : meta.max_length;
    return fitString(value, maxLen, meta.column_name);
  }
  if (type === 'int' || type === 'smallint' || type === 'tinyint' || type === 'bigint') {
    return toNumber(value, meta.column_name);
  }
  if (type === 'float' || type === 'real' || type === 'decimal' || type === 'money') {
    return toNumber(value, meta.column_name);
  }
  if (type === 'bit') {
    if (value === true || value === false) return value ? 1 : 0;
    const n = toNumber(value, meta.column_name);
    return n ? 1 : 0;
  }
  return value;
}

function buildSqlType(meta) {
  const type = meta.data_type.toLowerCase();
  if (type === 'nvarchar') {
    return meta.max_length === -1 ? sql.NVarChar(sql.MAX) : sql.NVarChar(meta.max_length / 2);
  }
  if (type === 'varchar') {
    return meta.max_length === -1 ? sql.VarChar(sql.MAX) : sql.VarChar(meta.max_length);
  }
  if (type === 'nchar') return sql.NChar(meta.max_length / 2);
  if (type === 'char') return sql.Char(meta.max_length);
  if (type === 'int') return sql.Int;
  if (type === 'smallint') return sql.SmallInt;
  if (type === 'tinyint') return sql.TinyInt;
  if (type === 'bigint') return sql.BigInt;
  if (type === 'float') return sql.Float;
  if (type === 'real') return sql.Real;
  if (type === 'decimal') return sql.Decimal(meta.precision || 18, meta.scale ?? 0);
  if (type === 'money') return sql.Money;
  if (type === 'datetime') return sql.DateTime;
  if (type === 'smalldatetime') return sql.SmallDateTime;
  if (type === 'binary') return sql.Binary(meta.max_length);
  if (type === 'varbinary') return sql.VarBinary(meta.max_length);
  return sql.NVarChar(sql.MAX);
}

function splitProjectPayload(payload) {
  const reserved = new Set(['adresse', 'rechnungAdresse', 'bauherrAdresse', 'projekt']);
  const rootProject = {};
  for (const [key, value] of Object.entries(payload || {})) {
    if (reserved.has(key)) continue;
    rootProject[key] = value;
  }
  if (payload?.projekt && Object.keys(rootProject).length) {
    throw new Error('Nutze entweder "projekt" ODER Root-Felder, nicht beides.');
  }
  return payload?.projekt && typeof payload.projekt === 'object' ? payload.projekt : rootProject;
}

function mapPayloadToColumns(raw, columns, options = {}) {
  const { allowKeys = [], label = 'payload' } = options;
  const { byLower, metaByName } = buildColumnMaps(columns);
  const data = {};
  for (const [key, value] of Object.entries(raw || {})) {
    if (allowKeys.includes(key)) continue;
    const columnName = byLower.get(key.toLowerCase());
    if (!columnName) {
      throw new Error(`Unbekanntes Feld in ${label}: ${key}`);
    }
    const meta = metaByName.get(columnName);
    if (!isInsertableColumn(meta)) {
      throw new Error(`Feld nicht schreibbar in ${label}: ${columnName}`);
    }
    data[columnName] = normalizeValueForColumn(value, meta);
  }
  return { data, metaByName };
}

function extractAddressExtras(raw) {
  if (!raw || typeof raw !== 'object') return {};
  const extras = {};
  if (Object.prototype.hasOwnProperty.call(raw, 'sameAsAdresse')) {
    extras.sameAsAdresse = raw.sameAsAdresse !== false;
  }
  if (Object.prototype.hasOwnProperty.call(raw, 'plz')) extras.plz = fitString(raw.plz, 16, 'plz');
  if (Object.prototype.hasOwnProperty.call(raw, 'ort')) extras.ort = fitString(raw.ort, 80, 'ort');
  if (Object.prototype.hasOwnProperty.call(raw, 'land')) extras.land = fitString(raw.land, 3, 'land');
  if (Object.prototype.hasOwnProperty.call(raw, 'plzn')) extras.plzn = fitString(raw.plzn, 16, 'plzn');
  if (Object.prototype.hasOwnProperty.call(raw, 'ortTyp')) extras.ortTyp = toNumber(raw.ortTyp, 'ortTyp');
  if (Object.prototype.hasOwnProperty.call(raw, 'ortId')) extras.ortId = toNumber(raw.ortId, 'ortId');
  return extras;
}

async function ensureAdresse(trx, adrMeta, raw, baseLabel) {
  if (!raw || typeof raw !== 'object') {
    throw new Error(`${baseLabel} fehlt.`);
  }
  const extras = extractAddressExtras(raw);
  const { data, metaByName } = mapPayloadToColumns(raw, adrMeta, {
    allowKeys: ['sameAsAdresse', 'plz', 'ort', 'land', 'plzn', 'ortTyp', 'ortId'],
    label: baseLabel,
  });
  if (!data.AdrNrGes) {
    throw new Error(`${baseLabel}.AdrNrGes fehlt.`);
  }
  if (String(data.AdrNrGes).length > ADR_NR_MAX) {
    throw new Error(`${baseLabel}.AdrNrGes ist zu lang (max ${ADR_NR_MAX}).`);
  }
  const checkReq = new sql.Request(trx);
  checkReq.input('AdrNrGes', sql.NVarChar(48), data.AdrNrGes);
  const exists = await checkReq.query('SELECT AdrNrGes FROM dbo.adrAdressen WHERE AdrNrGes = @AdrNrGes');
  if (exists.recordset.length) {
    return { adrNrGes: data.AdrNrGes, extras };
  }

  const requiredCols = getRequiredColumns(adrMeta);
  for (const col of requiredCols) {
    if (data[col] == null) {
      throw new Error(`${baseLabel}.${col} fehlt.`);
    }
  }
  if (!data.Name) {
    throw new Error(`${baseLabel}.Name fehlt.`);
  }
  if (!data.Strasse) {
    throw new Error(`${baseLabel}.Strasse fehlt.`);
  }

  let ortId = data.Ort ?? extras.ortId;
  if (ortId == null) {
    if (!extras.plz || !extras.ort) {
      throw new Error(`${baseLabel} braucht plz + ort oder ortId.`);
    }
    ortId = await ensureOrt(trx, {
      plz: extras.plz,
      ort: extras.ort,
      land: extras.land,
      plzn: extras.plzn,
      ortTyp: extras.ortTyp,
    });
  }
  if (data.Ort == null) {
    data.Ort = ortId;
  }

  const columns = Object.keys(data);
  const insertReq = new sql.Request(trx);
  for (const col of columns) {
    const meta = metaByName.get(col);
    insertReq.input(col, buildSqlType(meta), data[col]);
  }
  const columnList = columns.map((col) => `[${col}]`).join(', ');
  const valueList = columns.map((col) => `@${col}`).join(', ');
  const insertSql = `INSERT INTO dbo.adrAdressen (${columnList}) VALUES (${valueList});`;
  await insertReq.query(insertSql);
  return { adrNrGes: data.AdrNrGes, extras };
}

async function insertProjektDirect(trx, payload) {
  const projektMeta = await getTableColumns(trx, 'dbo.Projekt');
  const adrMeta = await getTableColumns(trx, 'dbo.adrAdressen');
  const projectRaw = splitProjectPayload(payload);
  const { data: projectData, metaByName: projectMetaMap } = mapPayloadToColumns(projectRaw, projektMeta, {
    label: 'projekt',
  });

  const projnr = projectData.ProjNr || projectData.ProjNr === 0 ? projectData.ProjNr : null;
  if (!projnr) {
    throw new Error('projnr fehlt.');
  }

  const baseRaw = payload?.adresse;
  if (!baseRaw) {
    throw new Error('adresse fehlt.');
  }

  await ensureAdrNrGes(trx, baseRaw, 'PROJADR');

  const hasRechnung = payload?.rechnungAdresse && typeof payload.rechnungAdresse === 'object';
  const hasBauherr = payload?.bauherrAdresse && typeof payload.bauherrAdresse === 'object';
  const rechnungSame = !hasRechnung ? true : payload.rechnungAdresse.sameAsAdresse === true;
  const bauherrSame = !hasBauherr ? true : payload.bauherrAdresse.sameAsAdresse === true;

  const rechRaw = rechnungSame ? baseRaw : payload?.rechnungAdresse;
  const bauRaw = bauherrSame ? baseRaw : payload?.bauherrAdresse;

  if (!rechnungSame) {
    await ensureAdrNrGes(trx, rechRaw, 'RECHADR');
  }
  if (!bauherrSame) {
    await ensureAdrNrGes(trx, bauRaw, 'BAUHRADR');
  }

  const projAdrResult = await ensureAdresse(trx, adrMeta, baseRaw, 'adresse');
  const rechAdrResult = await ensureAdresse(trx, adrMeta, rechRaw, 'rechnungAdresse');
  const bauAdrResult = await ensureAdresse(trx, adrMeta, bauRaw, 'bauherrAdresse');

  const projAdr = projAdrResult.adrNrGes;
  const rechAdr = rechAdrResult.adrNrGes;
  const bauAdr = bauAdrResult.adrNrGes;

  if (projectData.ProjAdr && projectData.ProjAdr !== projAdr) {
    throw new Error('ProjAdr passt nicht zur adresse.AdrNrGes.');
  }
  if (projectData.RechAdr && projectData.RechAdr !== rechAdr) {
    throw new Error('RechAdr passt nicht zur rechnungAdresse.AdrNrGes.');
  }
  if (projectData.BauHrAdr && projectData.BauHrAdr !== bauAdr) {
    throw new Error('BauHrAdr passt nicht zur bauherrAdresse.AdrNrGes.');
  }

  projectData.ProjAdr = projAdr;
  projectData.RechAdr = rechAdr;
  projectData.BauHrAdr = bauAdr;

  if (!Object.prototype.hasOwnProperty.call(projectData, 'Createdate')) {
    projectData.Createdate = new Date();
  }
  if (!Object.prototype.hasOwnProperty.call(projectData, 'Editdate')) {
    projectData.Editdate = new Date();
  }
  if (!Object.prototype.hasOwnProperty.call(projectData, 'ProjAnlage')) {
    projectData.ProjAnlage = new Date();
  }
  if (!Object.prototype.hasOwnProperty.call(projectData, 'AuftragsDatum')) {
    projectData.AuftragsDatum = new Date();
  }

  const requiredProj = getRequiredColumns(projektMeta);
  for (const col of requiredProj) {
    if (projectData[col] == null) {
      throw new Error(`projekt.${col} fehlt.`);
    }
  }

  const checkReq = new sql.Request(trx);
  checkReq.input('ProjNr', sql.NVarChar(30), projectData.ProjNr);
  const exists = await checkReq.query('SELECT 1 FROM dbo.Projekt WHERE ProjNr = @ProjNr');
  if (exists.recordset.length) {
    return { status: 'exists', projnr: projectData.ProjNr };
  }

  const columns = Object.keys(projectData);
  const insertReq = new sql.Request(trx);
  for (const col of columns) {
    const meta = projectMetaMap.get(col);
    insertReq.input(col, buildSqlType(meta), projectData[col]);
  }
  const columnList = columns.map((col) => `[${col}]`).join(', ');
  const valueList = columns.map((col) => `@${col}`).join(', ');
  const insertSql = `INSERT INTO dbo.Projekt (${columnList}) VALUES (${valueList});`;
  await insertReq.query(insertSql);
  return { status: 'inserted', projnr: projectData.ProjNr };
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
    const result = await insertProjektDirect(trx, payload);
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
      error: truncateString(err.message || String(err), 2000),
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
