import { app, query, update, uuid, errorHandler } from 'mu';
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

import { createJob, updateJob, addGraphAndFileToJob, getFirstScheduledJobId, getJob, FINISHED, FAILED, STARTED } from './jobs';

const kaleidosGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;
const publicGraph = `http://mu.semte.ch/graphs/public`;

app.get('/', function( req, res ) {
  res.send('Hello from publieksontsluiting-export-service');
} );

app.get('/export/:uuid', async function(req,res,next) {
  const result = parseResult((await getJob(req.params.uuid)));
  const firstResult = result[0];
  if (firstResult.status === FINISHED) {
    res.status(200).send({status: firstResult.status, export: firstResult.file, graph: firstResult.graph});
  }
  else {
    res.status(406).send({status: firstResult.status});
  }
});

app.post('/export/:uuid', async function(req, res, next) {
  const zitting_id = req.params.uuid;
  const result = parseResult(await getMeetingUriFromKaleidos(kaleidosGraph, zitting_id));
  if (result.length > 0) {
    const job_id = uuid();
    const zitting = result[0].s;
    await createJob(job_id, zitting);
    res.status(202).send({
      job_id
    });
  }
  else {
    res.status(404).send({ error: `could not find ${uuid} in ${kaleidosGraph}`});
  }
});

app.use(errorHandler);

executeJobs();
async function executeJobs() {
  const job = await getFirstScheduledJobId();
  if (job) {
    console.log(`executing ${job}`);
    await createExport(job);
    executeJobs();
  }
  else {
    setTimeout(executeJobs, 60000);
  }
}

async function createExport(uuid) {
  const timestamp = new Date().toISOString().replace(/\D/g, '').substring(0, 14);
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;
  const exportGraph = `http://mu.semte.ch/graphs/export/${timestamp}`;
  const file = `/data/exports/${timestamp}-publieksontsluiting.ttl`;
  const result = await getJob(uuid);
  const job = parseResult(result)[0];

  try {
    await updateJob(uuid, STARTED);
    const meetingUri = job.zitting;
    const meetingInfo = constructMeetingInfo(kaleidosGraph, meetingUri);

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
    await writeToFile(exportGraph, file);
    await addGraphAndFileToJob(uuid, exportGraph, file);
    await updateJob(uuid, FINISHED);
    console.log(`finished job ${uuid}`);
  } catch (e) {
    console.log(e);
    await updateJob(uuid, FAILED);
  }
}
