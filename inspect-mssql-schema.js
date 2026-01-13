require('dotenv').config();
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

const required = ['MSSQL_SERVER', 'MSSQL_DB', 'MSSQL_USER', 'MSSQL_PASS'];
const missing = required.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`Missing env vars: ${missing.join(', ')}`);
  process.exit(1);
}

const OUT_DIR = process.env.SCHEMA_OUT_DIR || '.';
const JSON_PATH = path.join(OUT_DIR, 'mssql-schema.json');
const MD_PATH = path.join(OUT_DIR, 'mssql-schema.md');

function formatLength(type, maxLength, precision, scale) {
  const t = type.toLowerCase();
  if (t === 'nvarchar' || t === 'nchar') {
    if (maxLength === -1) return 'MAX';
    return String(Math.floor(maxLength / 2));
  }
  if (t === 'varchar' || t === 'char' || t === 'varbinary' || t === 'binary') {
    if (maxLength === -1) return 'MAX';
    return String(maxLength);
  }
  if (t === 'decimal' || t === 'numeric') {
    if (precision == null) return null;
    return `${precision},${scale ?? 0}`;
  }
  return null;
}

function formatType(col) {
  const len = formatLength(col.data_type, col.max_length, col.precision, col.scale);
  return len ? `${col.data_type}(${len})` : col.data_type;
}

function renderMarkdown(schema) {
  const lines = [];
  lines.push(`# MSSQL Schema Report`);
  lines.push(``);
  lines.push(`Generated: ${schema.generatedAt}`);
  lines.push(`Database: ${schema.database}`);
  lines.push(``);
  lines.push(`## Summary`);
  lines.push(`- Tables: ${schema.tables.length}`);
  lines.push(`- Views: ${schema.views.length}`);
  lines.push(`- Foreign keys: ${schema.foreignKeys.length}`);
  lines.push(`- Triggers: ${schema.triggers.length}`);
  lines.push(``);
  lines.push(`This report lists tables, columns, keys, indexes, triggers, and view definitions.`);
  lines.push(`It is intended for reverse engineering and safe mapping to an external API.`);
  lines.push(``);

  for (const table of schema.tables) {
    lines.push(`## ${table.schema}.${table.name}`);
    lines.push(`- Type: ${table.type}`);
    if (table.primaryKey.length) {
      lines.push(`- Primary key: ${table.primaryKey.join(', ')}`);
    }
    if (table.triggers.length) {
      lines.push(`- Triggers: ${table.triggers.map((t) => t.name).join(', ')}`);
    }
    if (table.indexes.length) {
      lines.push(`- Indexes: ${table.indexes.length}`);
    }
    lines.push(``);
    lines.push(`Columns:`);
    for (const col of table.columns) {
      const tags = [];
      if (col.is_identity) tags.push('IDENTITY');
      if (col.is_computed) tags.push('COMPUTED');
      if (col.default_definition) tags.push(`DEFAULT ${col.default_definition}`);
      const nullable = col.is_nullable ? 'NULL' : 'NOT NULL';
      const tagStr = tags.length ? ` (${tags.join(', ')})` : '';
      lines.push(`- ${col.column_name} ${formatType(col)} ${nullable}${tagStr}`);
    }
    lines.push(``);
    if (table.foreignKeys.length) {
      lines.push(`Foreign keys:`);
      for (const fk of table.foreignKeys) {
        const map = fk.columns
          .map((c) => `${c.column_name} -> ${fk.ref_schema}.${fk.ref_table}.${c.ref_column_name}`)
          .join(', ');
        lines.push(`- ${fk.name}: ${map}`);
      }
      lines.push(``);
    }
    if (table.indexes.length) {
      lines.push(`Indexes:`);
      for (const idx of table.indexes) {
        const cols = idx.columns
          .map((c) => `${c.column_name}${c.is_included ? ' (included)' : ''}`)
          .join(', ');
        lines.push(`- ${idx.name} [${idx.type_desc}] ${idx.is_unique ? 'UNIQUE ' : ''}${cols}`);
      }
      lines.push(``);
    }
    if (table.triggers.length) {
      lines.push(`Triggers (definition):`);
      for (const tr of table.triggers) {
        lines.push(`- ${tr.name} (disabled=${tr.is_disabled}, instead_of=${tr.is_instead_of})`);
        if (tr.definition) {
          lines.push('```sql');
          lines.push(tr.definition.trim());
          lines.push('```');
        }
      }
      lines.push(``);
    }
  }

  if (schema.views.length) {
    lines.push(`## Views`);
    for (const view of schema.views) {
      lines.push(`### ${view.schema}.${view.name}`);
      if (view.definition) {
        lines.push('```sql');
        lines.push(view.definition.trim());
        lines.push('```');
      }
    }
  }

  lines.push(``);
  return lines.join('\n');
}

async function run() {
  const pool = await sql.connect({
    server: process.env.MSSQL_SERVER,
    database: process.env.MSSQL_DB,
    user: process.env.MSSQL_USER,
    password: process.env.MSSQL_PASS,
    options: { encrypt: false, trustServerCertificate: true },
  });

  const dbRes = await pool.request().query('SELECT DB_NAME() AS name;');
  const dbName = dbRes.recordset?.[0]?.name || process.env.MSSQL_DB;

  const tablesRes = await pool.request().query(`
    SELECT t.object_id, s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY s.name, t.name;
  `);

  const viewsRes = await pool.request().query(`
    SELECT v.object_id, s.name AS schema_name, v.name AS view_name, m.definition
    FROM sys.views v
    JOIN sys.schemas s ON v.schema_id = s.schema_id
    LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
    ORDER BY s.name, v.name;
  `);

  const columnsRes = await pool.request().query(`
    SELECT
      c.object_id,
      c.column_id,
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
    LEFT JOIN sys.default_constraints dc
      ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id IN (SELECT object_id FROM sys.tables)
    ORDER BY c.object_id, c.column_id;
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

  const fkRes = await pool.request().query(`
    SELECT
      fk.object_id,
      fk.name AS fk_name,
      sch1.name AS schema_name,
      tab1.name AS table_name,
      sch2.name AS ref_schema_name,
      tab2.name AS ref_table_name,
      fk.is_disabled,
      fk.is_not_for_replication,
      fkc.constraint_column_id,
      col1.name AS column_name,
      col2.name AS ref_column_name
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
    ORDER BY sch1.name, tab1.name, fk.name, fkc.constraint_column_id;
  `);

  const idxRes = await pool.request().query(`
    SELECT
      i.object_id,
      i.index_id,
      i.name AS index_name,
      i.type_desc,
      i.is_unique,
      i.is_primary_key,
      i.is_unique_constraint,
      ic.key_ordinal,
      ic.is_included_column,
      c.name AS column_name
    FROM sys.indexes i
    JOIN sys.index_columns ic
      ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c
      ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.is_hypothetical = 0 AND i.index_id > 0
    ORDER BY i.object_id, i.index_id, ic.key_ordinal, ic.is_included_column;
  `);

  const trgRes = await pool.request().query(`
    SELECT
      tr.object_id,
      tr.name,
      tr.parent_id,
      tr.is_disabled,
      tr.is_instead_of_trigger,
      m.definition
    FROM sys.triggers tr
    LEFT JOIN sys.sql_modules m ON tr.object_id = m.object_id
    WHERE tr.parent_class_desc = 'OBJECT_OR_COLUMN'
    ORDER BY tr.parent_id, tr.name;
  `);

  const tableMap = new Map();
  for (const t of tablesRes.recordset) {
    tableMap.set(t.object_id, {
      object_id: t.object_id,
      schema: t.schema_name,
      name: t.table_name,
      type: 'USER_TABLE',
      columns: [],
      primaryKey: [],
      foreignKeys: [],
      indexes: [],
      triggers: [],
    });
  }

  for (const c of columnsRes.recordset) {
    const table = tableMap.get(c.object_id);
    if (table) table.columns.push(c);
  }

  for (const pk of pkRes.recordset) {
    const table = tableMap.get(pk.object_id);
    if (table) table.primaryKey.push(pk.column_name);
  }

  const fkGroup = new Map();
  for (const fk of fkRes.recordset) {
    const key = `${fk.schema_name}.${fk.table_name}.${fk.fk_name}`;
    if (!fkGroup.has(key)) {
      fkGroup.set(key, {
        name: fk.fk_name,
        schema: fk.schema_name,
        table: fk.table_name,
        ref_schema: fk.ref_schema_name,
        ref_table: fk.ref_table_name,
        is_disabled: fk.is_disabled,
        is_not_for_replication: fk.is_not_for_replication,
        columns: [],
      });
    }
    fkGroup.get(key).columns.push({
      column_name: fk.column_name,
      ref_column_name: fk.ref_column_name,
    });
  }
  for (const fk of fkGroup.values()) {
    const table = [...tableMap.values()].find(
      (t) => t.schema === fk.schema && t.name === fk.table
    );
    if (table) table.foreignKeys.push(fk);
  }

  const idxGroup = new Map();
  for (const idx of idxRes.recordset) {
    const key = `${idx.object_id}:${idx.index_id}`;
    if (!idxGroup.has(key)) {
      idxGroup.set(key, {
        object_id: idx.object_id,
        index_id: idx.index_id,
        name: idx.index_name,
        type_desc: idx.type_desc,
        is_unique: idx.is_unique,
        is_primary_key: idx.is_primary_key,
        is_unique_constraint: idx.is_unique_constraint,
        columns: [],
      });
    }
    idxGroup.get(key).columns.push({
      column_name: idx.column_name,
      is_included: idx.is_included_column,
    });
  }
  for (const idx of idxGroup.values()) {
    const table = tableMap.get(idx.object_id);
    if (table) table.indexes.push(idx);
  }

  for (const tr of trgRes.recordset) {
    const table = tableMap.get(tr.parent_id);
    if (table) {
      table.triggers.push({
        name: tr.name,
        is_disabled: tr.is_disabled,
        is_instead_of: tr.is_instead_of_trigger,
        definition: tr.definition,
      });
    }
  }

  const schema = {
    generatedAt: new Date().toISOString(),
    database: dbName,
    tables: Array.from(tableMap.values()),
    views: viewsRes.recordset.map((v) => ({
      object_id: v.object_id,
      schema: v.schema_name,
      name: v.view_name,
      definition: v.definition,
    })),
    foreignKeys: Array.from(fkGroup.values()),
    triggers: trgRes.recordset.map((t) => ({
      object_id: t.object_id,
      name: t.name,
      parent_id: t.parent_id,
      is_disabled: t.is_disabled,
      is_instead_of: t.is_instead_of_trigger,
    })),
  };

  fs.writeFileSync(JSON_PATH, JSON.stringify(schema, null, 2));
  fs.writeFileSync(MD_PATH, renderMarkdown(schema));

  console.log(`Wrote ${JSON_PATH}`);
  console.log(`Wrote ${MD_PATH}`);

  await pool.close();
}

run().catch((err) => {
  console.error('Error:', err.message || err);
  process.exit(1);
});
