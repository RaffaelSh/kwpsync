require('dotenv').config();
const sql = require('mssql');

const required = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = required.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

const projNr = process.argv[2] || process.env.PROJNR;
if (!projNr) {
  console.error('Usage: node test-project.js <ProjNr>  (or set PROJNR in .env)');
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

  const res = await pool.request()
    .input('projNr', sql.NVarChar, projNr)
    .query(`
      SELECT
        ProjNr,
        ProjBezeichnung,
        ProjAdr,
        RechAdr,
        BauHrAdr,
        AbtNr,
        AuftragsSumme,
        Beginn,
        CreateDate,
        EditDate
      FROM dbo.Projekt
      WHERE ProjNr = @projNr;
    `);

  if (!res.recordset.length) {
    console.log(`No row found for ProjNr=${projNr}`);
  } else {
    console.log(JSON.stringify(res.recordset[0], null, 2));
  }

  await pool.close();
}

run().catch((err) => {
  console.error('Error:', err.message || err);
  process.exit(1);
});
