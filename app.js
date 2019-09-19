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
  constructNieuwsbriefInfo,
  constructLinkZittingNieuws,
  constructMandateeAndPersonInfo,
  getNieuwsbriefInfoFromExport,
  constructThemeInfo,
  constructDocumentsInfo,
  getDocumentsFromTmp,
  constructDocumentsAndLatestVersie,
  constructLinkNieuwsDocumentVersie,
  constructDocumentTypesInfo,
  getDocumentVersiesFromExport,
  constructFilesInfo
} from './queries';

app.get('/', function( req, res ) {
  res.send('Hello from publieksontsluiting-export-service');
} );

app.post('/export/:uuid', async function(req, res, next) {
  const timestamp = new Date().toISOString().replace(/\D/g, '').substring(0, 14);
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;
  const kaleidosGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;
  const exportGraph = `http://mu.semte.ch/graphs/export/${timestamp}`;
  const publicGraph = `http://mu.semte.ch/graphs/public`;
  const uuid = req.params.uuid;

  try {
    const meeting = parseResult(await getMeetingUriFromKaleidos(kaleidosGraph, uuid))[0];
    if (!meeting)
      throw new Error(`No meeting found with uuid ${uuid}`);

    const meetingUri = meeting.s;

    const meetingInfo = constructMeetingInfo(kaleidosGraph, uuid);
    await copyToLocalGraph(meetingInfo, exportGraph);
    console.log(`Exported info of zitting ${uuid}`);

    const procedurestapInfoQuery = constructProcedurestapInfo(kaleidosGraph, meetingUri);
    await copyToLocalGraph(procedurestapInfoQuery, tmpGraph);

    const resultProcedurestappenInfo = await getProcedurestappenInfoFromTmp(tmpGraph);
    const procedurestappenInfo = parseResult(resultProcedurestappenInfo);

    for (let procedurestapInfo of procedurestappenInfo) {
      const nieuwsbriefInfoQuery = constructNieuwsbriefInfo(kaleidosGraph, procedurestapInfo);
      await copyToLocalGraph(nieuwsbriefInfoQuery, exportGraph);

      const mandateeAndPersonInfoQuery = constructMandateeAndPersonInfo(kaleidosGraph, procedurestapInfo);
      await copyToLocalGraph(mandateeAndPersonInfoQuery, exportGraph);

      const documentsInfoQuery = constructDocumentsInfo(kaleidosGraph, procedurestapInfo);
      await copyToLocalGraph(documentsInfoQuery, tmpGraph);

      
    }

    // TODO insert mededelingen as newsbrief info in export graph
    // TODO insert foaf:Document of mededeling in export graph

    await constructLinkZittingNieuws(exportGraph, meetingUri);

    const resultNieuwsbrievenInfo = await getNieuwsbriefInfoFromExport(exportGraph);
    const nieuwsbrievenInfo = parseResult(resultNieuwsbrievenInfo);

    for (let nieuwsbriefInfo of nieuwsbrievenInfo) {
      const themeInfoQuery = constructThemeInfo(kaleidosGraph, publicGraph, nieuwsbriefInfo);
      await copyToLocalGraph(themeInfoQuery, exportGraph);
    }

    const resultDocumentsInfo = await getDocumentsFromTmp(tmpGraph);
    const documentsInfo = parseResult(resultDocumentsInfo);

    for (let documentInfo of documentsInfo) {
      await constructDocumentsAndLatestVersie(exportGraph, tmpGraph, documentInfo);
      const documentTypesInfoQuery = constructDocumentTypesInfo(kaleidosGraph, publicGraph, documentInfo);
      await copyToLocalGraph(documentTypesInfoQuery, exportGraph);
    }

    for (let nieuwsbriefInfo of nieuwsbrievenInfo) {
      await constructLinkNieuwsDocumentVersie(exportGraph, tmpGraph, nieuwsbriefInfo);
    }

    const resultDocumentVersiesInfo = await getDocumentVersiesFromExport(exportGraph);
    const documentVersiesInfo = parseResult(resultDocumentVersiesInfo);

    for (let documentVersieInfo of documentVersiesInfo) {
      const filesInfoQuery = constructFilesInfo(kaleidosGraph, documentVersieInfo);
      await copyToLocalGraph(filesInfoQuery, exportGraph);
    }

    const file = `/data/exports/${timestamp}-publieksontsluiting.ttl`;
    await writeToFile(exportGraph, file);

    res.status(200).send({
      export: file,
      graph: exportGraph
    });
  } catch (e) {
    console.log(JSON.stringify(e));
    const error = new Error(`An error occurred while processing zitting ${uuid}: ${JSON.stringify(e)}`);
    error.status = 500;
    return next(error);
  }
});

app.use(errorHandler);
