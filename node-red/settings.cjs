'use strict';
const fs = require('node:fs');
module.exports = {
  uiPort: Number(process.env.PORT || 1880),
  uiHost: '0.0.0.0',
  flowFile: '/data/flows.json',
  flowFilePretty: true,
  userDir: '/data',
  httpAdminRoot: '/admin',
  httpNodeRoot: '/',
  // UI is local HTTP by default; optional HTTPS uses the mounted dashboard certificate.
  https: process.env.NODE_RED_HTTPS === 'true' ? {
    key: fs.readFileSync('/certs/dashboard.key'),
    cert: fs.readFileSync('/certs/dashboard.crt'),
    minVersion: 'TLSv1.2'
  } : undefined,
  // Loopback-only host port exposure is mandatory in the compose/run definition.
  // No passwords, certificate values or other credentials are stored in flows.
  credentialSecret: process.env.NODE_RED_CREDENTIAL_SECRET || false,
  contextStorage: { default: { module: 'memory' } },
  functionGlobalContext: { domain: require('/app/src/domain.cjs') },
  functionExternalModules: false,
  functionTimeout: 5,
  externalModules: { autoInstall: false, palette: { allowInstall: false, allowUpload: false }, modules: { allowInstall: false } },
  editorTheme: { page: { title: 'HVAC independent Node-RED pipeline' }, projects: { enabled: false }, palette: { editable: false }, tours: false },
  diagnostics: { enabled: false, ui: false },
  runtimeState: { enabled: false, ui: false },
  logging: { console: { level: 'info', metrics: false, audit: false } },
  exportGlobalContextKeys: false
};
