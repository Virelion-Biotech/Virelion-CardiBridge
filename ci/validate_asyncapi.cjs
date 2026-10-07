'use strict';
const fs = require('node:fs');
const { Parser } = require('@asyncapi/parser');
(async () => {
  const result = await new Parser().parse(fs.readFileSync(process.argv[2], 'utf8'));
  const errors = result.diagnostics.filter(diagnostic => diagnostic.severity === 0);
  if (!result.document || errors.length) {
    console.error(JSON.stringify(errors, null, 2));
    process.exit(1);
  }
  console.log('Official AsyncAPI parser: valid document, zero errors.');
})();
