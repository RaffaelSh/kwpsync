require('dotenv').config();
const { execSync } = require('child_process');

function run(cmd) {
  return execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
}

const container = process.env.MSSQL_DOCKER_CONTAINER || 'kwp-mssql';
const image = process.env.MSSQL_DOCKER_IMAGE || 'mcr.microsoft.com/mssql/server:2022-latest';
const hostPort = process.env.MSSQL_DOCKER_PORT || '1433';
const saPassword = process.env.MSSQL_DOCKER_SA_PASSWORD;
const dataDir = process.env.MSSQL_DOCKER_DATA_DIR;
const volume = process.env.MSSQL_DOCKER_VOLUME || `${container}-data`;
const pid = process.env.MSSQL_DOCKER_PID || 'Developer';

if (!saPassword || saPassword.length < 8) {
  console.error('Missing or weak MSSQL_DOCKER_SA_PASSWORD (min length 8).');
  process.exit(1);
}

try {
  run('docker --version');
} catch (err) {
  console.error('Docker not available. Install Docker on the dev server first.');
  process.exit(1);
}

let exists = false;
try {
  const names = run(`docker ps -a --filter "name=^${container}$" --format "{{.Names}}"`);
  exists = names.split('\n').includes(container);
} catch (_err) {
  exists = false;
}

if (exists) {
  const running = run(`docker ps --filter "name=^${container}$" --format "{{.Names}}"`);
  if (running.split('\n').includes(container)) {
    console.log(`Docker MSSQL container already running: ${container}`);
    process.exit(0);
  }
  run(`docker start ${container}`);
  console.log(`Docker MSSQL container started: ${container}`);
  process.exit(0);
}

const volumeArg = dataDir
  ? `-v "${dataDir}:/var/opt/mssql"`
  : `-v ${volume}:/var/opt/mssql`;

const cmd = [
  'docker run -d',
  `--name ${container}`,
  `-e "ACCEPT_EULA=Y"`,
  `-e "MSSQL_SA_PASSWORD=${saPassword}"`,
  `-e "MSSQL_PID=${pid}"`,
  `-p ${hostPort}:1433`,
  volumeArg,
  image,
].join(' ');

run(cmd);
console.log(`Docker MSSQL container created: ${container}`);
