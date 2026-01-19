'use strict';

const path = require('path');
const express = require('express');
const swaggerUiDist = require('swagger-ui-dist');

const app = express();
const port = process.env.DOCS_PORT || 4010;

const swaggerUiPath = swaggerUiDist.getAbsoluteFSPath();
app.use('/docs-assets', express.static(swaggerUiPath));

app.get(['/docs', '/docs/'], (_req, res) => {
  res.sendFile(path.join(__dirname, 'swagger-ui.html'));
});

app.get('/openapi.json', (_req, res) => {
  res.sendFile(path.join(__dirname, 'openapi.json'));
});

app.listen(port, () => {
  console.log(`swagger-ui listening on :${port}`);
});
