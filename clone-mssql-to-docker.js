require('dotenv').config();
const sql = require('mssql');
const { execSync } = require('child_process');

const REQUIRED_ENV = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS', 'MSSQL_DOCKER_SA_PASSWORD'];
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

let sourceServer = rawServer;
const sourceOptions = { encrypt: false, trustServerCertificate: true };
if (normalizedServer.includes('\\')) {
  const [host, instanceName] = normalizedServer.split('\\');
  sourceServer = host;
  if (instanceName && !parsedPort) {
    sourceOptions.instanceName = instanceName;
  }
}

const sourceConfig = {
  server: sourceServer,
  ...(parsedPort ? { port: parsedPort } : {}),
  database: process.env.MSSQL_DB,
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options: sourceOptions,
};

const targetPort = Number.parseInt(process.env.MSSQL_DOCKER_PORT || '1433', 10);
const targetConfig = {
  server: process.env.MSSQL_TARGET_SERVER || '127.0.0.1',
  port: Number.isFinite(targetPort) ? targetPort : 1433,
  database: process.env.MSSQL_TARGET_DB || `${process.env.MSSQL_DB}_CLONE`,
  user: process.env.MSSQL_TARGET_USER || 'sa',
  password: process.env.MSSQL_DOCKER_SA_PASSWORD,
  options: { encrypt: false, trustServerCertificate: true },
};

const targetDb = targetConfig.database;
const dropTarget = String(process.env.MSSQL_TARGET_DROP ?? '1').toLowerCase() !== '0';
const batchSize = Number.parseInt(process.env.CLONE_BATCH_SIZE || '1000', 10);
const skipData = String(process.env.CLONE_SCHEMA_ONLY || '0').toLowerCase() === '1';
const tableFilter = process.env.CLONE_TABLE_FILTER || '';
const skipExistingTables = String(process.env.CLONE_SKIP_EXISTING_TABLES || '1').toLowerCase() !== '0';
const compareCounts = String(process.env.CLONE_COMPARE_COUNTS || '1').toLowerCase() !== '0';
const truncateExisting = String(process.env.CLONE_TRUNCATE_EXISTING || '0').toLowerCase() === '1';
const dropExistingTables = String(process.env.CLONE_DROP_EXISTING_TABLES || '0').toLowerCase() === '1';

function runDockerStart() {
  const startScript = process.env.MSSQL_DOCKER_START_SCRIPT || './docker-mssql-start.js';
  execSync(`${process.execPath} ${startScript}`, { stdio: 'inherit' });
}

function bracket(name) {
  return `[${String(name).replace(/]/g, ']]')}]`;
}

function formatType(col) {
  const type = col.data_type.toLowerCase();
  if (type === 'nvarchar' || type === 'nchar') {
    if (col.max_length === -1) return `${type}(MAX)`;
    return `${type}(${Math.floor(col.max_length / 2)})`;
  }
  if (type === 'varchar' || type === 'char' || type === 'varbinary' || type === 'binary') {
    if (col.max_length === -1) return `${type}(MAX)`;
    return `${type}(${col.max_length})`;
  }
  if (type === 'decimal' || type === 'numeric') {
    return `${type}(${col.precision},${col.scale ?? 0})`;
  }
  if (type === 'datetime2' || type === 'datetimeoffset' || type === 'time') {
    return `${type}(${col.scale ?? 7})`;
  }
  return type;
}

function getSqlType(col) {
  const type = col.data_type.toLowerCase();
  const len = col.max_length;
  switch (type) {
    case 'bigint': return sql.BigInt;
    case 'int': return sql.Int;
    case 'smallint': return sql.SmallInt;
    case 'tinyint': return sql.TinyInt;
    case 'bit': return sql.Bit;
    case 'decimal':
    case 'numeric': return sql.Decimal(col.precision ?? 18, col.scale ?? 0);
    case 'float': return sql.Float;
    case 'real': return sql.Real;
    case 'money': return sql.Money;
    case 'smallmoney': return sql.SmallMoney;
    case 'uniqueidentifier': return sql.UniqueIdentifier;
    case 'date': return sql.Date;
    case 'datetime': return sql.DateTime;
    case 'smalldatetime': return sql.SmallDateTime;
    case 'datetime2': return sql.DateTime2(col.scale ?? 7);
    case 'datetimeoffset': return sql.DateTimeOffset(col.scale ?? 7);
    case 'time': return sql.Time(col.scale ?? 7);
    case 'varchar': return sql.VarChar(len === -1 ? sql.MAX : len);
    case 'nvarchar': return sql.NVarChar(len === -1 ? sql.MAX : Math.floor(len / 2));
    case 'char': return sql.Char(len === -1 ? sql.MAX : len);
    case 'nchar': return sql.NChar(len === -1 ? sql.MAX : Math.floor(len / 2));
    case 'text': return sql.VarChar(sql.MAX);
    case 'ntext': return sql.NVarChar(sql.MAX);
    case 'xml': return sql.Xml;
    case 'binary': return sql.Binary(len === -1 ? sql.MAX : len);
    case 'varbinary': return sql.VarBinary(len === -1 ? sql.MAX : len);
    case 'image': return sql.VarBinary(sql.MAX);
    default: return sql.NVarChar(sql.MAX);
  }
}

function isRowversion(col) {
  const type = col.data_type.toLowerCase();
  return type === 'timestamp' || type === 'rowversion';
}

async function waitForTarget() {
  const maxAttempts = Number.parseInt(process.env.MSSQL_TARGET_WAIT_TRIES || '30', 10);
  const delayMs = Number.parseInt(process.env.MSSQL_TARGET_WAIT_MS || '2000', 10);
  for (let i = 0; i < maxAttempts; i += 1) {
    try {
      const pool = await new sql.ConnectionPool({ ...targetConfig, database: 'master' }).connect();
      await pool.close();
      return;
    } catch (_err) {
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error('Target MSSQL not reachable after retries.');
}

async function ensureTargetDb() {
  const masterPool = await new sql.ConnectionPool({ ...targetConfig, database: 'master' }).connect();
  if (dropTarget) {
    await masterPool.request().batch(`
      IF DB_ID(N'${targetDb}') IS NOT NULL
      BEGIN
        ALTER DATABASE ${bracket(targetDb)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE ${bracket(targetDb)};
      END
    `);
  }
  await masterPool.request().batch(`
    IF DB_ID(N'${targetDb}') IS NULL
      CREATE DATABASE ${bracket(targetDb)};
  `);
  await masterPool.close();
}

async function fetchTables(pool) {
  const res = await pool.request().query(`
    SELECT s.name AS schema_name, t.name AS table_name, t.object_id
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY s.name, t.name;
  `);
  let tables = res.recordset || [];
  if (tableFilter) {
    const regex = new RegExp(tableFilter, 'i');
    tables = tables.filter((t) => regex.test(`${t.schema_name}.${t.table_name}`));
  }
  return tables;
}

async function fetchColumns(pool, objectId) {
  const res = await pool.request()
    .input('obj', sql.Int, objectId)
    .query(`
      SELECT
        c.column_id,
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        c.is_identity,
        c.is_computed,
        ic.seed_value,
        ic.increment_value,
        cc.definition AS computed_definition,
        cc.is_persisted
      FROM sys.columns c
      JOIN sys.types ty ON c.user_type_id = ty.user_type_id
      LEFT JOIN sys.identity_columns ic
        ON c.object_id = ic.object_id AND c.column_id = ic.column_id
      LEFT JOIN sys.computed_columns cc
        ON c.object_id = cc.object_id AND c.column_id = cc.column_id
      WHERE c.object_id = @obj
      ORDER BY c.column_id;
    `);
  return res.recordset || [];
}

async function createSchema(targetPool, schema) {
  await targetPool.request().batch(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'${schema}')
      EXEC('CREATE SCHEMA ${bracket(schema)}');
  `);
}

async function tableExists(targetPool, schema, table) {
  const res = await targetPool.request()
    .input('schema', sql.NVarChar, schema)
    .input('table', sql.NVarChar, table)
    .query(`
      SELECT 1
      FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = @schema AND t.name = @table;
    `);
  return Boolean(res.recordset?.[0]);
}

async function getTableRowCount(pool, schema, table) {
  const res = await pool.request()
    .input('schema', sql.NVarChar, schema)
    .input('table', sql.NVarChar, table)
    .query(`
      SELECT SUM(p.row_count) AS row_count
      FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      JOIN sys.dm_db_partition_stats p ON t.object_id = p.object_id
      WHERE s.name = @schema AND t.name = @table AND p.index_id IN (0,1);
    `);
  const row = res.recordset?.[0];
  return row?.row_count == null ? null : Number(row.row_count);
}

async function dropTable(targetPool, schema, table) {
  await targetPool.request().batch(`DROP TABLE ${bracket(schema)}.${bracket(table)};`);
}

async function truncateTable(targetPool, schema, table) {
  await targetPool.request().batch(`TRUNCATE TABLE ${bracket(schema)}.${bracket(table)};`);
}

async function createTable(targetPool, schema, table, columns) {
  const columnDefs = columns.map((col) => {
    if (col.is_computed) {
      const persist = col.is_persisted ? ' PERSISTED' : '';
      return `${bracket(col.column_name)} AS ${col.computed_definition}${persist}`;
    }
    const identity = col.is_identity
      ? ` IDENTITY(${col.seed_value ?? 1},${col.increment_value ?? 1})`
      : '';
    const nullable = col.is_nullable ? 'NULL' : 'NOT NULL';
    const type = formatType(col);
    return `${bracket(col.column_name)} ${type}${identity} ${nullable}`;
  });

  await targetPool.request().batch(`
    CREATE TABLE ${bracket(schema)}.${bracket(table)} (
      ${columnDefs.join(',\n      ')}
    );
  `);
}

function createBulkTable(schema, table, columns) {
  const bulkTable = new sql.Table(`${schema}.${table}`);
  bulkTable.create = false;
  for (const col of columns) {
    bulkTable.columns.add(col.column_name, getSqlType(col), { nullable: Boolean(col.is_nullable) });
  }
  return bulkTable;
}

async function copyTableData(sourcePool, targetPool, schema, table, columns) {
  const insertable = columns.filter((col) => !col.is_computed && !isRowversion(col));
  if (!insertable.length) return;

  const hasIdentity = insertable.some((col) => col.is_identity);
  const columnNames = insertable.map((col) => bracket(col.column_name)).join(', ');
  const sourceQuery = `SELECT ${columnNames} FROM ${bracket(schema)}.${bracket(table)};`;

  if (hasIdentity) {
    await targetPool.request().batch(`SET IDENTITY_INSERT ${bracket(schema)}.${bracket(table)} ON;`);
  }

  let bulkTable = createBulkTable(schema, table, insertable);
  let pendingFlush = Promise.resolve();

  await new Promise((resolve, reject) => {
    const request = sourcePool.request();
    request.stream = true;

    request.on('row', (row) => {
      const values = insertable.map((col) => row[col.column_name]);
      bulkTable.rows.add(...values);

      if (bulkTable.rows.length >= batchSize) {
        request.pause();
        const tableToInsert = bulkTable;
        bulkTable = createBulkTable(schema, table, insertable);
        pendingFlush = pendingFlush
          .then(() => targetPool.request().bulk(tableToInsert, { keepNulls: true }))
          .then(() => request.resume())
          .catch((err) => {
            request.cancel();
            reject(err);
          });
      }
    });

    request.on('error', (err) => reject(err));
    request.on('done', async () => {
      try {
        await pendingFlush;
        if (bulkTable.rows.length) {
          await targetPool.request().bulk(bulkTable, { keepNulls: true });
        }
        resolve();
      } catch (err) {
        reject(err);
      }
    });

    request.query(sourceQuery);
  });

  if (hasIdentity) {
    await targetPool.request().batch(`SET IDENTITY_INSERT ${bracket(schema)}.${bracket(table)} OFF;`);
  }
}

async function run() {
  const startDocker = String(process.env.MSSQL_TARGET_START_DOCKER ?? '1').toLowerCase() !== '0';
  if (startDocker) {
    runDockerStart();
  }

  await waitForTarget();
  await ensureTargetDb();

  const sourcePool = await new sql.ConnectionPool(sourceConfig).connect();
  const targetPool = await new sql.ConnectionPool(targetConfig).connect();

  const tables = await fetchTables(sourcePool);
  console.log(`Tables to clone: ${tables.length}`);

  for (const tableInfo of tables) {
    const { schema_name: schema, table_name: table, object_id: objectId } = tableInfo;
    const columns = await fetchColumns(sourcePool, objectId);
    if (!columns.length) continue;

    const exists = await tableExists(targetPool, schema, table);
    if (exists && dropExistingTables) {
      console.log(`Dropping existing ${schema}.${table}...`);
      await dropTable(targetPool, schema, table);
    }

    const stillExists = exists && !dropExistingTables;
    if (!stillExists) {
      console.log(`Creating ${schema}.${table}...`);
      await createSchema(targetPool, schema);
      await createTable(targetPool, schema, table, columns);
    }

    if (!skipData) {
      if (stillExists && compareCounts) {
        const [sourceCount, targetCount] = await Promise.all([
          getTableRowCount(sourcePool, schema, table),
          getTableRowCount(targetPool, schema, table),
        ]);
        if (sourceCount != null && targetCount != null && sourceCount === targetCount) {
          console.log(`Skipping ${schema}.${table} (row counts match: ${sourceCount}).`);
          continue;
        }
      }

      if (stillExists && truncateExisting) {
        console.log(`Truncating ${schema}.${table}...`);
        await truncateTable(targetPool, schema, table);
      }

      console.log(`Copying data ${schema}.${table}...`);
      await copyTableData(sourcePool, targetPool, schema, table, columns);
    }
  }

  await sourcePool.close();
  await targetPool.close();

  console.log(`Clone completed into ${targetDb}.`);
}

run().catch((err) => {
  console.error('Clone failed:', err?.message || err);
  process.exit(1);
});
