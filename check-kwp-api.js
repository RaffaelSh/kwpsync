'use strict';

require('dotenv').config();

const http = require('http');
const https = require('https');
const { URL } = require('url');

const baseUrl = (process.env.KWP_API_BASE_URL || 'http://localhost:5000').replace(/\/$/, '');
const mandant = process.env.KWP_API_MANDANT || '';
const username = process.env.KWP_API_USERNAME || '';
const password = process.env.KWP_API_PASSWORD || '';
const clientId = process.env.KWP_API_CLIENT_ID || 'wartungswesen-desktop';
const clientSecret = process.env.KWP_API_CLIENT_SECRET || '';
const scope = process.env.KWP_API_SCOPE || 'kwpapi wartungswesen';

function request(method, path, headers, body) {
  const url = new URL(baseUrl + path);
  const isHttps = url.protocol === 'https:';
  const lib = isHttps ? https : http;

  const options = {
    method,
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    headers: headers || {}
  };

  return new Promise((resolve, reject) => {
    const req = lib.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        resolve({ status: res.statusCode || 0, body: data });
      });
    });
    req.on('error', reject);
    if (body) {
      req.write(body);
    }
    req.end();
  });
}

function withMandant(path) {
  if (!mandant) return path;
  return path + (path.includes('?') ? '&' : '?') + 'mandant=' + encodeURIComponent(mandant);
}

async function checkText(name, path, expectContains) {
  try {
    const res = await request('GET', path, { 'X-KWP-VERSION': '99' });
    const ok = res.status >= 200 && res.status < 300 && res.body.includes(expectContains);
    console.log(`${ok ? 'OK' : 'FAIL'} ${name}: ${res.status} ${res.body.trim()}`);
    return ok;
  } catch (err) {
    console.log(`FAIL ${name}: ${err.message}`);
    return false;
  }
}

async function getText(name, path) {
  try {
    const res = await request('GET', path, { 'X-KWP-VERSION': '99' });
    const ok = res.status >= 200 && res.status < 300;
    console.log(`${ok ? 'OK' : 'FAIL'} ${name}: ${res.status} ${res.body.trim()}`);
    return { ok, body: res.body };
  } catch (err) {
    console.log(`FAIL ${name}: ${err.message}`);
    return { ok: false, body: '' };
  }
}

async function getToken() {
  if (!username || !password || !clientSecret) {
    console.log('SKIP token: set KWP_API_USERNAME, KWP_API_PASSWORD, KWP_API_CLIENT_SECRET');
    return '';
  }
  const form = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: 'password',
    username,
    password,
    scope
  }).toString();

  try {
    const res = await request('POST', '/connect/token', {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(form)
    }, form);

    if (res.status < 200 || res.status >= 300) {
      console.log(`FAIL token: ${res.status} ${res.body.trim()}`);
      return '';
    }
    const data = JSON.parse(res.body);
    if (!data.access_token) {
      console.log('FAIL token: access_token missing');
      return '';
    }
    console.log('OK token: received access_token');
    return data.access_token;
  } catch (err) {
    console.log(`FAIL token: ${err.message}`);
    return '';
  }
}

async function checkAuthorized(name, path, token) {
  if (!token) {
    console.log(`SKIP ${name}: no token`);
    return false;
  }
  try {
    const res = await request('GET', path, {
      'X-KWP-VERSION': '99',
      Authorization: `bearer ${token}`
    });
    const ok = res.status >= 200 && res.status < 300;
    console.log(`${ok ? 'OK' : 'FAIL'} ${name}: ${res.status}`);
    return ok;
  } catch (err) {
    console.log(`FAIL ${name}: ${err.message}`);
    return false;
  }
}

async function main() {
  console.log(`KWP API check: ${baseUrl}`);

  const helloOk = await checkText('api/test/Hello', '/api/test/Hello', 'Hello');
  const hostRes = await getText('api/core/getHostname', withMandant('/api/core/getHostname'));
  const portRes = await getText('api/core/getLocalPort', withMandant('/api/core/getLocalPort'));

  const token = await getToken();
  await checkAuthorized('api/core/GetSetup', withMandant('/api/core/GetSetup'), token);

  if (helloOk && hostRes.ok && portRes.ok) {
    process.exitCode = 0;
  } else {
    process.exitCode = 2;
  }
}

main();
