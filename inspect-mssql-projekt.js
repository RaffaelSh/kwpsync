require('dotenv').config();
const sql = require('mssql');

const REQUIRED_ENV = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

const rawServer = process.env.MSSQL_SERVER;
let server = rawServer;
const options = { encrypt: false, trustServerCertificate: true };
if (rawServer.includes('\\')) {
  const [host, instanceName] = rawServer.split('\\');
  server = host;
  if (instanceName) {
    options.instanceName = instanceName;
  }
}

const config = {
  server,
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options,
};

const numericTypes = new Set([
  'int',
  'bigint',
  'smallint',
  'tinyint',
  'decimal',
  'numeric',
  'float',
  'real',
  'money',
  'smallmoney',
]);

const dateTypes = new Set([
  'date',
  'datetime',
  'datetime2',
  'smalldatetime',
  'datetimeoffset',
  'time',
]);

function bracket(name) {
  return `[${String(name).replace(/]/g, ']]')}]`;
}

async function run() {
  const pool = await sql.connect(config);

  const columnsRes = await pool.request().query(`
    SELECT
      COLUMN_NAME,
      DATA_TYPE,
      IS_NULLABLE,
      CHARACTER_MAXIMUM_LENGTH,
      NUMERIC_PRECISION,
      NUMERIC_SCALE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Projekt'
    ORDER BY ORDINAL_POSITION;
  `);

  const columns = columnsRes.recordset || [];
  if (!columns.length) {
    console.error('No columns found for dbo.Projekt');
    await pool.close();
    return;
  }

  console.log('\nMSSQL dbo.Projekt schema');
  console.table(columns);

  const countRes = await pool.request().query('SELECT COUNT(*) AS total FROM dbo.Projekt;');
  const total = countRes.recordset?.[0]?.total ?? 0;
  console.log(`Rows total: ${total}`);

  const selectParts = ['COUNT(*) AS total'];
  for (const col of columns) {
    const name = col.COLUMN_NAME;
    const dataType = String(col.DATA_TYPE).toLowerCase();
    selectParts.push(`SUM(CASE WHEN ${bracket(name)} IS NULL THEN 1 ELSE 0 END) AS ${bracket(name + '__nulls')}`);
    selectParts.push(`MIN(LEN(TRY_CAST(${bracket(name)} AS NVARCHAR(MAX)))) AS ${bracket(name + '__minlen')}`);
    selectParts.push(`MAX(LEN(TRY_CAST(${bracket(name)} AS NVARCHAR(MAX)))) AS ${bracket(name + '__maxlen')}`);
    if (numericTypes.has(dataType) || dateTypes.has(dataType)) {
      selectParts.push(`MIN(${bracket(name)}) AS ${bracket(name + '__min')}`);
      selectParts.push(`MAX(${bracket(name)}) AS ${bracket(name + '__max')}`);
    }
  }

  const statsQuery = `SELECT ${selectParts.join(',\n')} FROM dbo.Projekt;`;
  const statsRes = await pool.request().query(statsQuery);
  const stats = statsRes.recordset?.[0] ?? {};

  const output = columns.map((col) => {
    const name = col.COLUMN_NAME;
    return {
      column: name,
      type: col.DATA_TYPE,
      nullable: col.IS_NULLABLE,
      maxLength: col.CHARACTER_MAXIMUM_LENGTH ?? null,
      precision: col.NUMERIC_PRECISION ?? null,
      scale: col.NUMERIC_SCALE ?? null,
      nulls: stats[`${name}__nulls`],
      minLen: stats[`${name}__minlen`],
      maxLen: stats[`${name}__maxlen`],
      min: stats[`${name}__min`] ?? null,
      max: stats[`${name}__max`] ?? null,
    };
  });

  console.log('\nMSSQL dbo.Projekt stats');
  console.table(output);

  const sampleRes = await pool.request().query(`
    SELECT TOP (5)
      ${columns.map((col) => bracket(col.COLUMN_NAME)).join(', ')}
    FROM dbo.Projekt
    ORDER BY ${bracket('projnr')} DESC;
  `);

  console.log('\nMSSQL dbo.Projekt sample (top 5 by projnr desc)');
  console.table(sampleRes.recordset || []);

  await pool.close();
}

run().catch((err) => {
  console.error('MSSQL inspect failed:', err?.message || err);
  process.exit(1);
});
