import { app, query, errorHandler } from 'mu';
import { writeToFile } from './lib/graph-helpers';
import { queryKaleidos } from './lib/kaleidos';
import { copyToLocalGraph } from './lib/query-helpers';
import {
  parseResult,
  getMeetingUriFromKaleidos,
  constructMeetingInfo,
  constructProcedurestapInfo,
  getProcedurestappenInfoFromTmp,
  constructNieuwsbriefInfo
} from './queries';

app.get('/', function( req, res ) {
  res.send('Hello from publieksontsluiting-export-service');
} );

app.post('/export/:uuid', async function( req, res ) {
  const timestamp = new Date().toISOString().replace(/\D/g,'').substring(0, 14);
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;
  const kaleidosGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;
  const exportGraph = `http://mu.semte.ch/graphs/export/${timestamp}`;
  const uuid = req.params.uuid;

  const result = await getMeetingUriFromKaleidos(kaleidosGraph, uuid);
  try {
    const meetingUri = parseResult(result)[0].s;

    const meetingInfo = constructMeetingInfo(kaleidosGraph, uuid)
    await copyToLocalGraph(meetingInfo, exportGraph);

    const procedurestapInfo = constructProcedurestapInfo(kaleidosGraph, meetingUri)
    await copyToLocalGraph(procedurestapInfo, tmpGraph);

    const resultProcedurestappenInfo = await getProcedurestappenInfoFromTmp(tmpGraph);
    const procedurestappenInfo = parseResult(resultProcedurestappenInfo);

    procedurestappenInfo.forEach(procedurestapInfo => {
      const nieuwsbriefInfo = constructNieuwsbriefInfo(kaleidosGraph, procedurestapInfo)
      copyToLocalGraph(nieuwsbriefInfo, exportGraph);
    });

    const file = `/data/exports/${timestamp}-publieksontsluiting.ttl`;
    await writeToFile(graph, file);

    res.status(200).send({
      // export: file
    });
  } catch(e) {
    console.log(`No ziting found for this uuid: ${uuid}`);
    res.status(204).send({'msg': `No ziting found for this uuid: ${uuid}`});
  }
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
