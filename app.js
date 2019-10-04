import { app, query, update, uuid, errorHandler } from 'mu';
import uniq from 'lodash.uniq';
import { writeToFile } from './lib/graph-helpers';
import { queryKaleidos } from './lib/kaleidos';
import { copyToLocalGraph } from './lib/query-helpers';
import { createJob, updateJob, addGraphAndFileToJob, getFirstScheduledJobId, getJob, FINISHED, FAILED, STARTED } from './lib/jobs';
import {
  copySession,
  copyThemaCodes,
  copyDocumentTypes,
  copyNewsItemForProcedurestap,
  copyNewsItemForAgendapunt,
  copyMandateeAndPerson,
  copyDocumentsForProcedurestap,
  copyDocumentsForAgendapunt,
  copyFileTriples,
  getSession,
  getProcedurestappenOfSession,
  getMededelingenOfSession,
  getDocuments,
  getLatestVersion,
  insertDocumentAndLatestVersion,
  linkNewsItemsToDocumentVersion,
  calculatePriorityNewsItems,
  calculatePriorityMededelingen
} from './queries';

const EXPORT_SINCE = new Date(Date.parse('2006-07-19T00:00:00.000Z'));
const MEDEDELINGEN_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));
const DOCUMENTS_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));
// const DOCUMENTS_SINCE = new Date(Date.parse('2020-09-08T00:00:00.000Z'));
const PUBLIC_ACCESS_LEVEL = 'http://kanselarij.vo.data.gift/id/concept/toegangs-niveaus/6ca49d86-d40f-46c9-bde3-a322aa7e5c8e';

const kaleidosGraph = `http://mu.semte.ch/graphs/organizations/kanselarij`;
const publicGraph = `http://mu.semte.ch/graphs/public`;

app.get('/', function( req, res ) {
  res.send('Hello from valvas-export-service');
} );

app.get('/export/:uuid', async function(req,res,next) {
  const job = (await getJob(req.params.uuid));
  if (job.status === FINISHED) {
    res.status(200).send({status: job.status, export: job.file, graph: job.graph});
  }
  else {
    res.status(406).send({status: job.status});
  }
});

app.post('/export/:uuid', async function(req, res, next) {
  const sessionId = req.params.uuid;
  const session = await getSession(sessionId);
  if (session) {
    const jobId = uuid();
    await createJob(jobId, session);
    res.status(202).send({
      jobId
    });
  }
  else {
    res.status(404).send({ error: `Could not find session with uuid ${sessionId} in Kaleidos`});
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
  const job = await getJob(uuid);
  const sessionDate = new Date(Date.parse(job.zittingDatum));
  console.log(sessionDate);

  if (sessionDate < EXPORT_SINCE) {
    console.log(`Public export didn't exist yet on ${sessionDate}. Nothing will be exported`);
    return;
  }

  const timestamp = new Date().toISOString().replace(/\D/g, '');
  const sessionTimestamp = sessionDate.toISOString().replace(/\D/g, '');
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;
  const exportFileBase = `/data/exports/${timestamp.substring(0, 14)}-${timestamp.slice(14)}-${uuid}-${sessionTimestamp}`;

  try {
    await updateJob(uuid, STARTED);
    const sessionUri = job.zitting;

    const exportGraphNewsItems = `http://mu.semte.ch/graphs/export/${timestamp}-news-items`;
    let file = `${exportFileBase}-news-items.ttl`;

    await copySession(sessionUri, exportGraphNewsItems);
    await copyThemaCodes(exportGraphNewsItems); // TODO add as migration

    // News items dump
    const procedurestappen = await getProcedurestappenOfSession(sessionUri);
    for (let procedurestap of procedurestappen) {
      await copyNewsItemForProcedurestap(procedurestap.uri, sessionUri, exportGraphNewsItems);
      await copyDocumentsForProcedurestap(procedurestap.uri, tmpGraph);
    }
    const mandatees = uniq(procedurestappen.map(p => p.mandatee).filter(m => m != null));
    for (let mandatee of mandatees) {
      await copyMandateeAndPerson(mandatee, exportGraphNewsItems);
    }
    await calculatePriorityNewsItems(exportGraphNewsItems);

    await writeToFile(exportGraphNewsItems, file);


    // Mededelingen dump

    const exportGraphMededelingen = `http://mu.semte.ch/graphs/export/${timestamp}-mededelingen`;
    if (sessionDate > MEDEDELINGEN_SINCE) {
      file = `${exportFileBase}-mededelingen.ttl`;

      const mededelingen = await getMededelingenOfSession(sessionUri);
      for (let mededeling of mededelingen) {
        if (mededeling.procedurestap) { // mededeling has a KB
          await copyNewsItemForProcedurestap(mededeling.procedurestap, sessionUri,  exportGraphMededelingen, "mededeling");
          await copyDocumentsForProcedurestap(mededeling.procedurestap, tmpGraph);
        } else { // construct 'fake' nieuwsbrief info based on agendapunt title
// TODO fix when there is a direct link from agendapunt to nieuwsbriefinfo
//          await copyNewsItemForAgendapunt(mededeling.agendapunt, sessionUri,  exportGraphMededelingen);
//          await copyDocumentsForAgendapunt(mededeling.agendapunt, tmpGraph);
        }
      }
      await calculatePriorityMededelingen(exportGraphMededelingen);

      await writeToFile(exportGraphMededelingen, file);
    } else {
      console.log(`Public export of mededelingen didn't exist yet on ${sessionDate}. Mededelingen will be exported`);
    }

    // Documents dump

    let exportGraphDocuments = null;
    if (sessionDate > DOCUMENTS_SINCE) {
       exportGraphDocuments = `http://mu.semte.ch/graphs/export/${timestamp}-documents`;
      file = `${exportFileBase}-documents.ttl`;

      const documents = await getDocuments(tmpGraph);

      for (let document of documents) {
        const version = await getLatestVersion(tmpGraph, document.uri);
        if (version && version.accessLevel == PUBLIC_ACCESS_LEVEL) {
          document.version = version.uri;
          await insertDocumentAndLatestVersion(document.uri, version.uri, tmpGraph, exportGraphDocuments);
        }
      }
      await copyDocumentTypes(exportGraphDocuments); // TODO add as migration

      await linkNewsItemsToDocumentVersion([exportGraphNewsItems, exportGraphMededelingen], tmpGraph, exportGraphDocuments);

      for (let document of documents) {
        if (document.version) {
          await copyFileTriples(document.version, exportGraphDocuments);
        }
      }

      await writeToFile(exportGraphDocuments, file);
    } else {
      console.log(`Public export of documents didn't exist yet on ${sessionDate}. Documents will be exported`);
    }

    await addGraphAndFileToJob(uuid, exportGraphDocuments, file);
    await updateJob(uuid, FINISHED);
    console.log(`finished job ${uuid}`);
  } catch (e) {
    console.log(e);
    await updateJob(uuid, FAILED);
  }
}
