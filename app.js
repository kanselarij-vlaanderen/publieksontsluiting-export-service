import { app, query, errorHandler } from 'mu';
import { writeToFile } from './lib/graph-helpers';

app.get('/', function( req, res ) {
  res.send('Hello from publieksontsluiting-export-service');
} );

app.post('/export', async function( req, res ) {
  const timestamp = new Date().toISOString().replace(/\D/g,'').substring(0, 14);
  const file = `/data/exports/${timestamp}-publieksontsluiting.ttl`;
  await writeToFile('http://mu.semte.ch/application', file);

  res.status(200).send({
    export: file
  });
});

app.use(errorHandler);
