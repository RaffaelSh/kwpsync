require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

const outDir = path.resolve(process.env.ANALYZE_OUT_DIR || path.join(process.cwd(), 'reports', stamp()));
fs.mkdirSync(outDir, { recursive: true });

function runScript(label, scriptPath, args, envOverrides, outputFile) {
  const env = { ...process.env, ...envOverrides };
  const result = spawnSync(process.execPath, [scriptPath, ...args], {
    env,
    encoding: 'utf8',
  });

  if (outputFile) {
    fs.writeFileSync(outputFile, `${result.stdout || ''}${result.stderr || ''}`);
  } else {
    process.stdout.write(result.stdout || '');
    process.stderr.write(result.stderr || '');
  }

  if (result.status !== 0) {
    throw new Error(`${label} failed with exit code ${result.status}`);
  }
}

const repoRoot = __dirname;

try {
  console.log(`Writing reports to: ${outDir}`);

  runScript(
    'inspect-mssql-schema',
    path.join(repoRoot, 'inspect-mssql-schema.js'),
    [],
    { SCHEMA_OUT_DIR: outDir },
    path.join(outDir, 'inspect-mssql-schema.log')
  );

  runScript(
    'inspect-mssql-projekt',
    path.join(repoRoot, 'inspect-mssql-projekt.js'),
    [],
    { OUTPUT_FORMAT: 'json' },
    path.join(outDir, 'inspect-mssql-projekt.json')
  );

  runScript(
    'inspect-mssql-projekt-relations-json',
    path.join(repoRoot, 'inspect-mssql-projekt-relations.js'),
    ['--format', 'json', '--sample', process.env.SAMPLE_LIMIT || '5'],
    {},
    path.join(outDir, 'inspect-mssql-projekt-relations.json')
  );

  runScript(
    'inspect-mssql-projekt-relations-text',
    path.join(repoRoot, 'inspect-mssql-projekt-relations.js'),
    ['--format', 'text', '--sample', process.env.SAMPLE_LIMIT || '5'],
    {},
    path.join(outDir, 'inspect-mssql-projekt-relations.txt')
  );

  console.log('Done. Report files:');
  console.log(`- ${path.join(outDir, 'mssql-schema.json')}`);
  console.log(`- ${path.join(outDir, 'mssql-schema.md')}`);
  console.log(`- ${path.join(outDir, 'inspect-mssql-projekt.json')}`);
  console.log(`- ${path.join(outDir, 'inspect-mssql-projekt-relations.json')}`);
  console.log(`- ${path.join(outDir, 'inspect-mssql-projekt-relations.txt')}`);
} catch (err) {
  console.error('Analysis failed:', err?.message || err);
  process.exit(1);
}
