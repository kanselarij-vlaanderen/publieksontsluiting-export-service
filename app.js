import { app, query, errorHandler } from 'mu';
import { writeToFile } from './lib/graph-helpers';
import { queryKaleidos } from './lib/kaleidos';
import { copyToLocalGraph } from './lib/query-helpers';

app.get('/', function( req, res ) {
  res.send('Hello from publieksontsluiting-export-service');
} );

app.post('/export/:uuid', async function( req, res ) {
  const timestamp = new Date().toISOString().replace(/\D/g,'').substring(0, 14);
  const file = `/data/exports/${timestamp}-publieksontsluiting.ttl`;
  await writeToFile('http://mu.semte.ch/application', file);

  res.status(200).send({
    export: file
  });
});

// TODO remove this dummy function
app.get('/ping-kaleidos', async function( req, res ) {
  const result = await queryKaleidos(`SELECT * WHERE { GRAPH <http://mu.semte.ch/graphs/public> { ?s ?p ?o } } LIMIT 10`);
  res.status(200).send(result);
} );


// TODO remove this dummy function
app.get('/copy-kaleidos', async function( req, res ) {
  const constructQuery = `
    CONSTRUCT { ?s ?p ?o }
    WHERE {
         GRAPH <http://mu.semte.ch/graphs/public> {
             ?s a <http://mu.semte.ch/vocabularies/ext/ThemaCode> ; ?p ?o .
          }
     }`;
  await copyToLocalGraph(constructQuery, 'http://mu.semte.ch/graphs/copy-kaleidos');
  res.status(204).send({});
} );

app.use(errorHandler);
