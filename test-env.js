require('dotenv').config();
const sql = require('mssql');
const { createClient } = require('@supabase/supabase-js');

const required = [
  'MSSQL_SERVER',
  'MSSQL_DB',
  'MSSQL_USER',
  'MSSQL_PASS',
  'SUPA_URL',
  'SUPA_SERVICE_KEY',
];

const missing = required.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

function isValidUrl(url) {
  return /^https?:\/\//i.test(url);
}

async function testMssql() {
  const pool = await sql.connect({
    server: process.env.MSSQL_SERVER,
    database: process.env.MSSQL_DB,
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASS,
    options: { encrypt: false, trustServerCertificate: true },
  });

  const res = await pool.request().query('SELECT 1 AS ok');
  await pool.close();
  return res?.recordset?.[0]?.ok === 1;
}

async function testSupabase() {
  if (!isValidUrl(process.env.SUPA_URL)) {
    throw new Error('SUPA_URL must start with http:// or https://');
  }

  const supa = createClient(process.env.SUPA_URL, process.env.SUPA_SERVICE_KEY, {
    auth: { persistSession: false },
  });

  const { data, error } = await supa.from('projekt').select('projnr').limit(1);
  if (error) throw error;
  return Array.isArray(data);
}

async function run() {
  let failed = false;

  try {
    const ok = await testMssql();
    console.log(`MSSQL OK: ${ok}`);
  } catch (err) {
    failed = true;
    console.error('MSSQL ERROR:', err.message || err);
  }

  try {
    const ok = await testSupabase();
    console.log(`Supabase OK: ${ok}`);
  } catch (err) {
    failed = true;
    console.error('Supabase ERROR:', err.message || err);
  }

  if (failed) process.exit(1);
}

run();
