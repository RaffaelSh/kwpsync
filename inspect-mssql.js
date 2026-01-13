require('dotenv').config();
const sql = require('mssql');

const required = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = required.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

async function run() {
  const pool = await sql.connect({
    server: process.env.MSSQL_SERVER,
    database: process.env.MSSQL_DB,
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASS,
    options: { encrypt: false, trustServerCertificate: true },
  });

  const name = 'dbo.Projekt';
  const obj = await pool.request()
    .input('name', sql.NVarChar, name)
    .query(`
      SELECT o.type_desc, o.object_id
      FROM sys.objects o
      WHERE o.object_id = OBJECT_ID(@name);
    `);

  if (!obj.recordset.length) {
    console.error('Object not found:', name);
    await pool.close();
    process.exit(1);
  }

  const { type_desc: typeDesc, object_id: objectId } = obj.recordset[0];
  console.log(`Type: ${typeDesc}`);

  const cols = await pool.request().query(`
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

  console.log('Columns:');
  for (const c of cols.recordset) {
    const len = c.CHARACTER_MAXIMUM_LENGTH != null ? `(${c.CHARACTER_MAXIMUM_LENGTH})` : '';
    const prec =
      c.NUMERIC_PRECISION != null ? `(${c.NUMERIC_PRECISION},${c.NUMERIC_SCALE ?? 0})` : '';
    const size = len || prec;
    console.log(
      `- ${c.COLUMN_NAME} ${c.DATA_TYPE}${size} ${c.IS_NULLABLE === 'YES' ? 'NULL' : 'NOT NULL'}`
    );
  }

  if (typeDesc === 'VIEW') {
    const def = await pool.request()
      .input('id', sql.Int, objectId)
      .query('SELECT definition FROM sys.sql_modules WHERE object_id = @id;');
    const viewDef = def.recordset?.[0]?.definition;
    if (viewDef) {
      console.log('\nView definition:');
      console.log(viewDef.trim());
    }
  }

  await pool.close();
}

run().catch((err) => {
  console.error('Error:', err.message || err);
  process.exit(1);
});
