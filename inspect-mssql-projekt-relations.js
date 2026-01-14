require('dotenv').config();
const sql = require('mssql');

const REQUIRED_ENV = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

function getArgValue(name) {
  const flag = `--${name}`;
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return null;
  const value = process.argv[idx + 1];
  if (!value || value.startsWith('--')) return '';
  return value;
}

const rawServer = process.env.MSSQL_SERVER;
const normalizedServer = rawServer.replace(/\\\\+/g, '\\');
const portValue = process.env.MSSQL_PORT;
const parsedPort = portValue ? Number.parseInt(portValue, 10) : null;
if (portValue && (!Number.isFinite(parsedPort) || parsedPort <= 0)) {
  console.error(`Invalid MSSQL_PORT value: ${portValue}`);
  process.exit(1);
}
let server = rawServer;
const options = { encrypt: false, trustServerCertificate: true };
if (normalizedServer.includes('\\')) {
  const [host, instanceName] = normalizedServer.split('\\');
  server = host;
  if (instanceName && !parsedPort) {
    options.instanceName = instanceName;
  }
}

const config = {
  server,
  ...(parsedPort ? { port: parsedPort } : {}),
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options,
};

const outputFormat = String(
  getArgValue('format') ||
    getArgValue('output') ||
    process.env.OUTPUT_FORMAT ||
    'table'
).toLowerCase();
const sampleLimitRaw = getArgValue('sample') || process.env.SAMPLE_LIMIT || '3';
const sampleLimit = Number.parseInt(sampleLimitRaw, 10);
const shouldSample = Number.isFinite(sampleLimit) && sampleLimit > 0;

const addressPatterns = [
  'adr',
  'adresse',
  'address',
  'addr',
  'strasse',
  'str',
  'plz',
  'ort',
  'post',
  'email',
  'mail',
  'vorname',
  'nachname',
  'name',
  'ansprech',
  'kontakt',
  'firma',
  'company',
];

function bracket(name) {
  return `[${String(name).replace(/]/g, ']]')}]`;
}

function groupBy(list, keyFn) {
  const map = new Map();
  for (const item of list) {
    const key = keyFn(item);
    const existing = map.get(key);
    if (existing) {
      existing.push(item);
    } else {
      map.set(key, [item]);
    }
  }
  return map;
}

async function run() {
  const pool = await sql.connect(config);

  const projectColumnsRes = await pool.request().query(`
    SELECT
      c.name AS column_name,
      t.name AS data_type,
      c.max_length,
      c.is_nullable
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID('dbo.Projekt')
    ORDER BY c.column_id;
  `);
  const projectColumns = projectColumnsRes.recordset || [];

  const fkOutRes = await pool.request().query(`
    SELECT
      fk.name AS fk_name,
      sch1.name AS parent_schema,
      tab1.name AS parent_table,
      col1.name AS parent_column,
      sch2.name AS ref_schema,
      tab2.name AS ref_table,
      col2.name AS ref_column
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    JOIN sys.tables tab1 ON fk.parent_object_id = tab1.object_id
    JOIN sys.schemas sch1 ON tab1.schema_id = sch1.schema_id
    JOIN sys.tables tab2 ON fk.referenced_object_id = tab2.object_id
    JOIN sys.schemas sch2 ON tab2.schema_id = sch2.schema_id
    JOIN sys.columns col1
      ON fkc.parent_object_id = col1.object_id AND fkc.parent_column_id = col1.column_id
    JOIN sys.columns col2
      ON fkc.referenced_object_id = col2.object_id AND fkc.referenced_column_id = col2.column_id
    WHERE fk.parent_object_id = OBJECT_ID('dbo.Projekt')
    ORDER BY fk.name, fkc.constraint_column_id;
  `);

  const fkInRes = await pool.request().query(`
    SELECT
      fk.name AS fk_name,
      sch1.name AS parent_schema,
      tab1.name AS parent_table,
      col1.name AS parent_column,
      sch2.name AS ref_schema,
      tab2.name AS ref_table,
      col2.name AS ref_column
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    JOIN sys.tables tab1 ON fk.parent_object_id = tab1.object_id
    JOIN sys.schemas sch1 ON tab1.schema_id = sch1.schema_id
    JOIN sys.tables tab2 ON fk.referenced_object_id = tab2.object_id
    JOIN sys.schemas sch2 ON tab2.schema_id = sch2.schema_id
    JOIN sys.columns col1
      ON fkc.parent_object_id = col1.object_id AND fkc.parent_column_id = col1.column_id
    JOIN sys.columns col2
      ON fkc.referenced_object_id = col2.object_id AND fkc.referenced_column_id = col2.column_id
    WHERE fk.referenced_object_id = OBJECT_ID('dbo.Projekt')
    ORDER BY fk.name, fkc.constraint_column_id;
  `);

  const likeClauses = addressPatterns
    .map((_, idx) => `LOWER(c.name) LIKE @p${idx}`)
    .join(' OR ');
  const addressReq = pool.request();
  addressPatterns.forEach((pattern, idx) => {
    addressReq.input(`p${idx}`, sql.NVarChar, `%${pattern}%`);
  });
  const addressRes = await addressReq.query(`
    SELECT
      c.object_id,
      s.name AS schema_name,
      t.name AS table_name,
      c.name AS column_name,
      ty.name AS data_type,
      c.max_length
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    WHERE ${likeClauses}
    ORDER BY s.name, t.name, c.column_id;
  `);

  const pkRes = await pool.request().query(`
    SELECT kc.parent_object_id AS object_id, ic.key_ordinal, c.name AS column_name
    FROM sys.key_constraints kc
    JOIN sys.index_columns ic
      ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
    JOIN sys.columns c
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE kc.type = 'PK'
    ORDER BY kc.parent_object_id, ic.key_ordinal;
  `);

  const pkMap = new Map();
  for (const row of pkRes.recordset || []) {
    const list = pkMap.get(row.object_id) || [];
    list.push(row.column_name);
    pkMap.set(row.object_id, list);
  }

  const candidateMap = new Map();
  for (const row of addressRes.recordset || []) {
    const entry = candidateMap.get(row.object_id) || {
      schema: row.schema_name,
      table: row.table_name,
      objectId: row.object_id,
      columns: [],
      primaryKey: pkMap.get(row.object_id) || [],
    };
    entry.columns.push({
      name: row.column_name,
      dataType: row.data_type,
      maxLength: row.max_length,
    });
    candidateMap.set(row.object_id, entry);
  }

  const candidates = Array.from(candidateMap.values());

  const refCandidateNames = [
    'adr',
    'id',
    'nr',
    'ansprech',
    'auftraggeber',
    'kontakt',
  ];
  const projectRefCandidates = projectColumns.filter((col) => {
    const name = String(col.column_name || '').toLowerCase();
    return refCandidateNames.some((needle) => name.includes(needle));
  });

  let sampleRows = [];
  if (shouldSample) {
    const preferredColumns = [
      'ProjNr',
      'ProjBezeichnung',
      'ProjAdr',
      'RechAdr',
      'BauHrAdr',
      'Ansprechpartner',
      'Auftraggeber',
      'AuftragStatus',
      'fkAnsprechpartnerIDProjAdr',
      'fkAnsprechpartnerIDRechAdr',
      'fkAnsprechpartnerIDBauhAdr',
      'fkAnsprechpartnerIDArchAdr',
    ];
    const columnsByLower = new Map(
      projectColumns.map((col) => [String(col.column_name).toLowerCase(), col.column_name])
    );
    const sampleColumns = preferredColumns
      .map((col) => columnsByLower.get(col.toLowerCase()))
      .filter(Boolean);
    const columnList = sampleColumns.length
      ? sampleColumns
      : projectColumns.slice(0, 10).map((col) => col.column_name);
    const orderColumn = columnsByLower.get('projnr') || columnList[0];
    const sampleRes = await pool.request().query(`
      SELECT TOP (${sampleLimit})
        ${columnList.map((col) => bracket(col)).join(', ')}
      FROM dbo.Projekt
      ORDER BY ${bracket(orderColumn)} DESC;
    `);
    sampleRows = sampleRes.recordset || [];
  }

  const result = {
    projektColumns: projectColumns,
    projektReferenceCandidates: projectRefCandidates,
    foreignKeys: {
      outgoing: fkOutRes.recordset || [],
      incoming: fkInRes.recordset || [],
    },
    candidateTables: candidates,
    sample: sampleRows,
  };

  if (outputFormat === 'json') {
    console.log(JSON.stringify(result, null, 2));
  } else if (outputFormat === 'text') {
    console.log('## dbo.Projekt columns');
    projectColumns.forEach((col) => {
      console.log(`- ${col.column_name} (${col.data_type}) null=${col.is_nullable}`);
    });
    console.log('\n## dbo.Projekt reference candidates');
    projectRefCandidates.forEach((col) => {
      console.log(`- ${col.column_name} (${col.data_type})`);
    });
    console.log('\n## Foreign keys from dbo.Projekt');
    const fkOutGroups = groupBy(result.foreignKeys.outgoing, (row) => row.fk_name);
    for (const [name, rows] of fkOutGroups.entries()) {
      rows.forEach((row) => {
        console.log(
          `- ${name}: ${row.parent_schema}.${row.parent_table}.${row.parent_column} -> ${row.ref_schema}.${row.ref_table}.${row.ref_column}`
        );
      });
    }
    console.log('\n## Foreign keys to dbo.Projekt');
    const fkInGroups = groupBy(result.foreignKeys.incoming, (row) => row.fk_name);
    for (const [name, rows] of fkInGroups.entries()) {
      rows.forEach((row) => {
        console.log(
          `- ${name}: ${row.parent_schema}.${row.parent_table}.${row.parent_column} -> ${row.ref_schema}.${row.ref_table}.${row.ref_column}`
        );
      });
    }
    console.log('\n## Candidate address/contact tables');
    candidates.forEach((table) => {
      console.log(`- ${table.schema}.${table.table} PK=[${table.primaryKey.join(', ')}]`);
      table.columns.forEach((col) => {
        console.log(`  - ${col.name} (${col.dataType}${col.maxLength ? `:${col.maxLength}` : ''})`);
      });
    });
    if (shouldSample) {
      console.log('\n## dbo.Projekt sample rows');
      console.log(JSON.stringify(sampleRows, null, 2));
    }
  } else {
    console.log('\nMSSQL dbo.Projekt columns');
    console.table(projectColumns);
    console.log('\nMSSQL dbo.Projekt reference candidates');
    console.table(projectRefCandidates);
    console.log('\nMSSQL foreign keys from dbo.Projekt');
    console.table(result.foreignKeys.outgoing);
    console.log('\nMSSQL foreign keys to dbo.Projekt');
    console.table(result.foreignKeys.incoming);
    console.log('\nMSSQL candidate address/contact tables');
    console.table(
      candidates.flatMap((table) =>
        table.columns.map((col) => ({
          table: `${table.schema}.${table.table}`,
          primaryKey: table.primaryKey.join(', '),
          column: col.name,
          dataType: col.dataType,
          maxLength: col.maxLength,
        }))
      )
    );
    if (shouldSample) {
      console.log(`\nMSSQL dbo.Projekt sample rows (top ${sampleLimit})`);
      console.table(sampleRows);
    }
  }

  await pool.close();
}

run().catch((err) => {
  console.error('MSSQL relation inspect failed:', err?.message || err);
  process.exit(1);
});
