require('dotenv').config();
const fs = require('fs');
const path = require('path');
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
  database: 'master',
  user: process.env.MSSQL_USER,
  password: process.env.MSSQL_PASS,
  options,
};

const srcDb = process.env.MSSQL_DB;
const devDb = process.env.MSSQL_DEV_DB || `${srcDb}_DEV`;
const shouldRestore = String(process.env.MSSQL_RESTORE ?? '1').toLowerCase() !== '0';
const allowDrop = String(process.env.MSSQL_DROP_DEV ?? '0').toLowerCase() === '1';
const setReadOnly = String(process.env.MSSQL_READONLY ?? '1').toLowerCase() !== '0';
const backupPathEnv = process.env.MSSQL_BACKUP_PATH;
const backupDirEnv = process.env.MSSQL_BACKUP_DIR;
const copyBackupTo = process.env.MSSQL_COPY_BACKUP_TO;
const dataPathOverride = process.env.MSSQL_DATA_PATH;
const logPathOverride = process.env.MSSQL_LOG_PATH;
const readUser = process.env.MSSQL_READ_USER;
const readPass = process.env.MSSQL_READ_PASS;

function bracket(name) {
  return `[${String(name).replace(/]/g, ']]')}]`;
}

function sqlString(value) {
  return `N'${String(value).replace(/'/g, "''")}'`;
}

function isBakFile(filePath) {
  return path.extname(filePath).toLowerCase() === '.bak';
}

function resolvePath(input) {
  if (!input) return null;
  return path.isAbsolute(input) ? input : path.join(process.cwd(), input);
}

async function detectBackupDir(pool) {
  try {
    const res = await pool.request().query(
      "SELECT CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS NVARCHAR(4000)) AS path;"
    );
    const row = res.recordset?.[0];
    if (row?.path) return row.path;
  } catch (_err) {
    // ignore
  }

  try {
    const res = await pool.request().query(
      "EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\\\\Microsoft\\\\MSSQLServer\\\\MSSQLServer', N'BackupDirectory';"
    );
    const row = res.recordset?.[0];
    return row?.Data || row?.Value || row?.value || row?.path || null;
  } catch (_err) {
    return null;
  }
}

async function resolveBackupPath(pool) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const defaultName = `${srcDb}_${stamp}.bak`;

  if (backupPathEnv) {
    const resolved = resolvePath(backupPathEnv);
    return isBakFile(resolved) ? resolved : path.join(resolved, defaultName);
  }

  if (backupDirEnv) {
    const resolved = resolvePath(backupDirEnv);
    return path.join(resolved, defaultName);
  }

  const detectedDir = await detectBackupDir(pool);
  if (detectedDir) {
    return path.join(detectedDir, defaultName);
  }

  const fallbackDir = path.join(process.cwd(), 'backups');
  fs.mkdirSync(fallbackDir, { recursive: true });
  console.warn('Could not detect SQL Server backup directory, using local ./backups folder.');
  console.warn('If backup fails, set MSSQL_BACKUP_PATH to a SQL Server-writable path.');
  return path.join(fallbackDir, defaultName);
}

function parseFileMove(file, index) {
  const physical = file.PhysicalName || file.physicalName || '';
  const parsed = path.parse(physical);
  const type = String(file.Type || file.type || '').toUpperCase();
  const targetDir = type === 'L' ? (logPathOverride || parsed.dir) : (dataPathOverride || parsed.dir);
  const suffix = type === 'L' ? 'log' : 'data';
  const ext = parsed.ext || (type === 'L' ? '.ldf' : '.mdf');
  const base = `${devDb}_${suffix}_${index + 1}`;
  return path.join(targetDir, `${base}${ext}`);
}

async function run() {
  const pool = await sql.connect(config);
  const backupPath = await resolveBackupPath(pool);

  console.log(`Backup source DB: ${srcDb}`);
  console.log(`Backup path: ${backupPath}`);

  await pool.request().batch(`
    BACKUP DATABASE ${bracket(srcDb)}
    TO DISK = ${sqlString(backupPath)}
    WITH COPY_ONLY, COMPRESSION, INIT, CHECKSUM;
  `);

  await pool.request().query(`RESTORE VERIFYONLY FROM DISK = ${sqlString(backupPath)};`);

  const filelistRes = await pool.request().query(
    `RESTORE FILELISTONLY FROM DISK = ${sqlString(backupPath)};`
  );
  const filelist = filelistRes.recordset || [];
  if (!filelist.length) {
    throw new Error('RESTORE FILELISTONLY returned no files.');
  }

  if (copyBackupTo) {
    const targetPath = path.isAbsolute(copyBackupTo)
      ? copyBackupTo
      : path.join(process.cwd(), copyBackupTo);
    fs.copyFileSync(backupPath, targetPath);
    console.log(`Backup copied to: ${targetPath}`);
  }

  if (shouldRestore) {
    const existsRes = await pool
      .request()
      .input('db', sql.NVarChar, devDb)
      .query('SELECT name FROM sys.databases WHERE name = @db;');
    const exists = Boolean(existsRes.recordset?.[0]?.name);

    if (exists && !allowDrop) {
      throw new Error(`Database ${devDb} already exists. Set MSSQL_DROP_DEV=1 to replace.`);
    }

    if (exists && allowDrop) {
      await pool.request().batch(`
        ALTER DATABASE ${bracket(devDb)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE ${bracket(devDb)};
      `);
    }

    const moves = filelist.map((file, idx) => {
      const logical = file.LogicalName || file.logicalName;
      const target = parseFileMove(file, idx);
      return `MOVE ${sqlString(logical)} TO ${sqlString(target)}`;
    });

    await pool.request().batch(`
      RESTORE DATABASE ${bracket(devDb)}
      FROM DISK = ${sqlString(backupPath)}
      WITH ${moves.join(',\n')}, RECOVERY, STATS = 5;
    `);

    if (setReadOnly) {
      await pool.request().batch(`
        ALTER DATABASE ${bracket(devDb)} SET READ_ONLY WITH ROLLBACK IMMEDIATE;
      `);
    }

    if (readUser && readPass) {
      await pool.request().batch(`
        IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = ${sqlString(readUser)})
          CREATE LOGIN ${bracket(readUser)} WITH PASSWORD = ${sqlString(readPass)};
        USE ${bracket(devDb)};
        IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = ${sqlString(readUser)})
          CREATE USER ${bracket(readUser)} FOR LOGIN ${bracket(readUser)};
        EXEC sp_addrolemember 'db_datareader', ${bracket(readUser)};
      `);
    }
  }

  await pool.close();

  console.log(JSON.stringify({
    ok: true,
    backupPath,
    devDb: shouldRestore ? devDb : null,
    restored: shouldRestore,
    readOnly: shouldRestore ? setReadOnly : false,
  }, null, 2));
}

function formatSqlError(err) {
  if (!err) return 'Unknown error';
  const parts = [];
  const info = err?.originalError?.info;
  if (Array.isArray(info)) {
    for (const item of info) {
      if (item?.message) parts.push(item.message);
    }
  } else if (info?.message) {
    parts.push(info.message);
  }
  if (err?.message) parts.push(err.message);
  if (err?.code) parts.push(`code=${err.code}`);
  if (err?.number) parts.push(`number=${err.number}`);
  return Array.from(new Set(parts)).join(' | ');
}

run().catch((err) => {
  console.error('Backup/restore failed:', formatSqlError(err));
  process.exit(1);
});
