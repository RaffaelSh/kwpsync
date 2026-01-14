require('dotenv').config();
const sql = require('mssql');

const REQUIRED_ENV = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
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

const bitTypes = new Set(['bit']);

const textTypes = new Set([
  'nvarchar',
  'varchar',
  'nchar',
  'char',
  'text',
  'ntext',
  'sysname',
  'xml',
]);

const binaryTypes = new Set([
  'binary',
  'varbinary',
  'image',
  'timestamp',
  'rowversion',
]);

function bracket(name) {
  return `[${String(name).replace(/]/g, ']]')}]`;
}

async function run() {
  const pool = await sql.connect(config);
  const outputFormat = String(process.env.OUTPUT_FORMAT || 'table').toLowerCase();

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

  const result = {
    schema: columns,
    total: null,
    stats: null,
    sample: null,
  };

  if (outputFormat !== 'json') {
    console.log('\nMSSQL dbo.Projekt schema');
    console.table(columns);
  }

  const countRes = await pool.request().query('SELECT COUNT(*) AS total FROM dbo.Projekt;');
  const total = countRes.recordset?.[0]?.total ?? 0;
  result.total = total;
  if (outputFormat !== 'json') {
    console.log(`Rows total: ${total}`);
  }

  const selectParts = ['COUNT(*) AS total'];
  for (const col of columns) {
    const name = col.COLUMN_NAME;
    const dataType = String(col.DATA_TYPE).toLowerCase();
    selectParts.push(`SUM(CASE WHEN ${bracket(name)} IS NULL THEN 1 ELSE 0 END) AS ${bracket(name + '__nulls')}`);
    if (textTypes.has(dataType)) {
      selectParts.push(`MIN(LEN(CAST(${bracket(name)} AS NVARCHAR(MAX)))) AS ${bracket(name + '__minlen')}`);
      selectParts.push(`MAX(LEN(CAST(${bracket(name)} AS NVARCHAR(MAX)))) AS ${bracket(name + '__maxlen')}`);
    } else if (binaryTypes.has(dataType)) {
      selectParts.push(`MIN(DATALENGTH(${bracket(name)})) AS ${bracket(name + '__minlen')}`);
      selectParts.push(`MAX(DATALENGTH(${bracket(name)})) AS ${bracket(name + '__maxlen')}`);
    } else {
      selectParts.push(`CAST(NULL AS INT) AS ${bracket(name + '__minlen')}`);
      selectParts.push(`CAST(NULL AS INT) AS ${bracket(name + '__maxlen')}`);
    }
    if (bitTypes.has(dataType)) {
      selectParts.push(`MIN(CAST(${bracket(name)} AS TINYINT)) AS ${bracket(name + '__min')}`);
      selectParts.push(`MAX(CAST(${bracket(name)} AS TINYINT)) AS ${bracket(name + '__max')}`);
    } else if (numericTypes.has(dataType) || dateTypes.has(dataType)) {
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

  result.stats = output;
  if (outputFormat !== 'json') {
    console.log('\nMSSQL dbo.Projekt stats');
    console.table(output);
  }

  const sampleRes = await pool.request().query(`
    SELECT TOP (5)
      ${columns.map((col) => bracket(col.COLUMN_NAME)).join(', ')}
    FROM dbo.Projekt
    ORDER BY ${bracket('projnr')} DESC;
  `);

  result.sample = sampleRes.recordset || [];
  if (outputFormat !== 'json') {
    console.log('\nMSSQL dbo.Projekt sample (top 5 by projnr desc)');
    console.table(result.sample);
  }

  if (outputFormat === 'json') {
    console.log(JSON.stringify(result, null, 2));
  }

  await pool.close();
}

run().catch((err) => {
  console.error('MSSQL inspect failed:', err?.message || err);
  process.exit(1);
});
