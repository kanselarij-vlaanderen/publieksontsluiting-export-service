import { app, query, update, uuid, errorHandler, sparqlEscapeUri, sparqlEscapeString } from 'mu';
import uniq from 'lodash.uniq';
import { writeToFile } from './lib/graph-helpers';
import { queryKaleidos } from './lib/kaleidos';
import { copyToLocalGraph } from './lib/query-helpers';
import { createTaskToDelta } from './lib/task-helpers'
import { createJob, updateJob, addGraphAndFileToJob, getFirstScheduledJobId, getJob, FINISHED, FAILED, STARTED } from './lib/jobs';
import {
  copySession,
  copyNewsItemForProcedurestap,
  copyNewsItemForAgendapunt,
  copyMandateeAndPerson,
  copyDocumentsForProcedurestap,
  copyDocumentsForAgendapunt,
  copyFileTriples,
  getSession,
  getLatestAgendaOfSession,
  getProcedurestappenOfAgenda,
  getMededelingenOfAgenda,
  getDocumentContainers,
  getLatestVersion,
  insertDocumentAndLatestVersion,
  linkNewsItemsToDocumentVersion,
  calculatePriorityNewsItems,
  calculatePriorityMededelingen
} from './queries';
import bodyParser from 'body-parser';
import {promises as FsPromises} from 'fs'

const EXPORT_SINCE = new Date(Date.parse('2006-07-19T00:00:00.000Z'));
const MEDEDELINGEN_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));
const DOCUMENTS_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));

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

app.post('/export/:uuid', bodyParser.json(), async function(req, res, next) {
  const sessionId = req.params.uuid;
  const scope = req.body.scope
  if(scope && scope.includes('documents') && (!scope.includes('news-items') || !scope.includes('announcements'))) {
    res.status(400).send({error: 'If "documents" is included in the scope "news-items" and "announcements" also need to be included'});
  }
  const documentNotification = req.body.documentNotification
  const session = await getSession(sessionId);
  if (session) {
    const jobId = uuid();
    await createJob(jobId, session, scope, documentNotification);
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

  try {
    await updateJob(uuid, STARTED);
    const sessionUri = job.zitting;
    let files = []
    const timestamp = new Date().toISOString().replace(/\D/g, '');
    const sessionTimestamp = sessionDate.toISOString().replace(/\D/g, '');
    const exportFileBase = `/data/exports/${timestamp.substring(0, 14)}-${timestamp.slice(14)}-${uuid}-${sessionTimestamp}`;
    
    if(job.scope) {
      files = await partialExport(uuid, timestamp, exportFileBase, sessionDate, sessionUri, job.scope)
    } else {
      files = await completeExport(uuid, timestamp, exportFileBase, sessionDate, sessionUri)
    }

    if(job.documentNotification) {
      const documentNotificationFile = await createDocumentNotificationFile(uuid, sessionUri, exportFileBase, job.documentNotification)
      files.push(documentNotificationFile)
    }
    
    await updateJob(uuid, FINISHED);
    await createTaskToDelta(files)
    console.log(`finished job ${uuid}`);
  } catch (e) {
    console.log(e);
    await updateJob(uuid, FAILED);
  }
}

async function completeExport(uuid, timestamp, sessionDate, sessionUri) {
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;

  const files = []

  const sessionFile = await exportSessionInfo(uuid, sessionUri, exportFileBase, timestamp)
  files.push(sessionFile)

  const agenda = await getLatestAgendaOfSession(sessionUri);
  const agendaUri = agenda.uri;

  const exportGraphNewsItems = `http://mu.semte.ch/graphs/export/${timestamp}-news-items`;
  const newsItemsFile = await exportNewsItems(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphNewsItems)
  files.push(newsItemsFile)

  // Mededelingen dump
  const exportGraphMededelingen = `http://mu.semte.ch/graphs/export/${timestamp}-mededelingen`;

  if (sessionDate > MEDEDELINGEN_SINCE) {
    const mededelingFile = await exportMededeling(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphMededelingen)
    files.push(mededelingFile)
  } else {
    console.log(`Public export of mededelingen didn't exist yet on ${sessionDate}. Mededelingen will be exported`);
  }

  // Documents dump

  if (sessionDate > DOCUMENTS_SINCE) {
    const documentsFile = await exportDocuments(uuid, tmpGraph, exportFileBase, timestamp, exportGraphNewsItems, exportGraphMededelingen)
    files.push(documentsFile)
  } else {
    console.log(`Public export of documents didn't exist yet on ${sessionDate}. Documents will be exported`);
  }
  return files;
}

async function partialExport(uuid, timestamp, exportFileBase, sessionDate, sessionUri, scope) {
  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;

  const files = []

  const sessionFile = await exportSessionInfo(uuid, sessionUri, exportFileBase, timestamp)
  files.push(sessionFile)

  const agenda = await getLatestAgendaOfSession(sessionUri);
  const agendaUri = agenda.uri;

  const exportGraphNewsItems = `http://mu.semte.ch/graphs/export/${timestamp}-news-items`;
  if(scope.includes('news-items')) {
    const newsItemsFile = await exportNewsItems(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphNewsItems)
    files.push(newsItemsFile)
  }

  const exportGraphMededelingen = `http://mu.semte.ch/graphs/export/${timestamp}-mededelingen`;
  if(scope.includes('announcements')) {
    if (sessionDate > MEDEDELINGEN_SINCE) {
      const mededelingFile = await exportMededeling(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphMededelingen)
      files.push(mededelingFile)
    } else {
      console.log(`Public export of mededelingen didn't exist yet on ${sessionDate}. Mededelingen will be exported`);
    }
  }

  if(scope.includes('documents') && scope.includes('news-items') && scope.includes('announcements')) {
    if (sessionDate > DOCUMENTS_SINCE) {
      const documentsFile = await exportDocuments(uuid, tmpGraph, exportFileBase, timestamp, exportGraphNewsItems, exportGraphMededelingen)
      files.push(documentsFile)
    } else {
      console.log(`Public export of documents didn't exist yet on ${sessionDate}. Documents will be exported`);
    }
  }
  return files
}


async function exportSessionInfo(uuid, sessionUri, exportFileBase, timestamp) {
  const exportGraphSessionInfo = `http://mu.semte.ch/graphs/export/${timestamp}-session-info`;
  let file = `${exportFileBase}-session-info.ttl`;

  await copySession(sessionUri, exportGraphSessionInfo);

  await writeToFile(exportGraphSessionInfo, file);
  await addGraphAndFileToJob(uuid, exportGraphSessionInfo, file);
  return file.split('/').pop()
}

async function exportNewsItems(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphNewsItems) {
  const file = `${exportFileBase}-news-items.ttl`;


  if (agendaUri == null) {
    console.log(`No agenda found for session ${sessionUri}. Nothing to export.`);
    return;
  }

  // News items dump
  const procedurestappen = await getProcedurestappenOfAgenda(agendaUri);
  console.log(`Found ${procedurestappen.length} news items`);
  for (let procedurestap of procedurestappen) {
    await copyNewsItemForProcedurestap(procedurestap.uri, sessionUri, exportGraphNewsItems);
    await copyDocumentsForProcedurestap(procedurestap.uri, tmpGraph);
  }
  const mandatees = uniq(procedurestappen.map(p => p.mandatee).filter(m => m != null));
  console.log(`Found ${mandatees.length} mandatees`);
  for (let mandatee of mandatees) {
    await copyMandateeAndPerson(mandatee, exportGraphNewsItems);
  }
  await calculatePriorityNewsItems(exportGraphNewsItems);

  await writeToFile(exportGraphNewsItems, file);
  await addGraphAndFileToJob(uuid, exportGraphNewsItems, file);
  return file.split('/').pop()
}

async function exportMededeling(uuid, sessionUri, tmpGraph, exportFileBase, timestamp, agendaUri, exportGraphMededelingen) {
  const file = `${exportFileBase}-mededelingen.ttl`;

  const mededelingen = await getMededelingenOfAgenda(agendaUri);
  console.log(`Found ${mededelingen.length} mededelingen`);
  for (let mededeling of mededelingen) {
    if (mededeling.procedurestap) { // mededeling has a KB
      await copyNewsItemForProcedurestap(mededeling.procedurestap, sessionUri,  exportGraphMededelingen, "mededeling");
      await copyDocumentsForProcedurestap(mededeling.procedurestap, tmpGraph);
    } else { // construct 'fake' nieuwsbrief info based on agendapunt title
      await copyNewsItemForAgendapunt(mededeling.agendapunt, sessionUri,  exportGraphMededelingen);
      await copyDocumentsForAgendapunt(mededeling.agendapunt, tmpGraph);
    }
  }
  await calculatePriorityMededelingen(exportGraphMededelingen);

  await writeToFile(exportGraphMededelingen, file);
  await addGraphAndFileToJob(uuid, exportGraphMededelingen, file);
  return file.split('/').pop()
}

async function exportDocuments(uuid, tmpGraph, exportFileBase, timestamp, exportGraphNewsItems, exportGraphMededelingen) {
  const exportGraphDocuments = `http://mu.semte.ch/graphs/export/${timestamp}-documents`;
  const file = `${exportFileBase}-documents.ttl`;

  const documents = await getDocumentContainers(tmpGraph);

  for (let document of documents) {
    const version = await getLatestVersion(tmpGraph, document.uri);
    if (version) {
      document.version = version.uri;
      await insertDocumentAndLatestVersion(document.uri, version.uri, tmpGraph, exportGraphDocuments); // Rewrites to old document model
    }
  }

  await linkNewsItemsToDocumentVersion([exportGraphNewsItems, exportGraphMededelingen], tmpGraph, exportGraphDocuments);

  for (let document of documents) {
    if (document.version) {
      await copyFileTriples(document.version, exportGraphDocuments);
    }
  }

  await writeToFile(exportGraphDocuments, file);
  await addGraphAndFileToJob(uuid, exportGraphDocuments, file);
  return file.split('/').pop()
}

async function createDocumentNotificationFile(uuid, sessionUri, exportFileBase, documentNotification) {
  const file = `${exportFileBase}-document-notification.ttl`;
  const title = `Document ministerraad ${documentNotification.sessionDate}`
  const description = `De documenten van deze ministerraad zullen beschikbaar zijn vanaf ${documentNotification.documentPublicationDateTime}.`
  const fileContent = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    
    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        <http://kanselarij.vo.data.gift/notifications/$uuid> a ext:Notification ;
        mu:uuid ${sparqlEscapeString(uuid)} ;
        dct:title ${sparqlEscapeString(title)} ;
        dct:description ${sparqlEscapeString(description)} ;
        dct:subject ${sparqlEscapeUri(sessionUri)} .
      }
    }
  `
  await FsPromises.writeFile(file, fileContent)
  return file.split('/').pop()
}