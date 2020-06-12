import path from 'path';
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
  calculatePriorityMededelingen,
  insertDocumentNotification
} from '../queries';
import { writeToFile } from './graph-helpers';
import { addGraphAndFileToJob } from './jobs';

/**
    Not all news-items, announcements and documents have been exported to Valvas in the past.
 */
const EXPORT_SINCE = new Date(Date.parse('2006-07-19T00:00:00.000Z'));
const MEDEDELINGEN_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));
const DOCUMENTS_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));

async function generateExport(job) {
  const scope =  job.scope ? job.scope : ['news-items', 'announcements', 'documents'];

  const sessionDate = new Date(Date.parse(job.zittingDatum));
  console.log(`Generating export for session of ${sessionDate}`);

  if (sessionDate < EXPORT_SINCE) {
    console.log(`Public export didn't exist yet on ${sessionDate}. Nothing will be exported`);
    return [];
  }

  const files = [];

  const sessionUri = job.zitting;
  const timestamp = new Date().toISOString().replace(/\D/g, '');
  const sessionTimestamp = sessionDate.toISOString().replace(/\D/g, '');
  const exportFileBase = `/data/exports/${timestamp.substring(0, 14)}-${timestamp.slice(14)}-${job.id}-${sessionTimestamp}`;

  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;

  const exportGraphSessionInfo = `http://mu.semte.ch/graphs/export/${timestamp}-session-info`;
  const sessionFile = await exportSessionInfo(job.id, sessionUri, exportFileBase, exportGraphSessionInfo);
  files.push(sessionFile);

  const agenda = await getLatestAgendaOfSession(sessionUri);
  const agendaUri = agenda.uri;

  if (agendaUri == null) {
    console.log(`No agenda found for session ${sessionUri}. Nothing to export.`);
    return [];
  }

  const exportGraphNewsItems = `http://mu.semte.ch/graphs/export/${timestamp}-news-items`;
  if (scope.includes('news-items')) {
    const newsItemsFile = await exportNewsItems(job.id, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphNewsItems);
    files.push(newsItemsFile);
  }

  const exportGraphMededelingen = `http://mu.semte.ch/graphs/export/${timestamp}-mededelingen`;
  if (scope.includes('announcements')) {
    if (sessionDate > MEDEDELINGEN_SINCE) {
      const mededelingFile = await exportMededeling(job.id, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphMededelingen);
      files.push(mededelingFile);
    } else {
      console.log(`Public export of announcements didn't exist yet on ${sessionDate}. Announcements will not be exported`);
    }
  }

  const exportGraphDocuments = `http://mu.semte.ch/graphs/export/${timestamp}-documents`;
  if (scope.includes('documents') && scope.includes('news-items') && scope.includes('announcements')) {
    if (sessionDate > DOCUMENTS_SINCE) {
      const documentsFile = await exportDocuments(job.id, tmpGraph, exportFileBase, exportGraphNewsItems, exportGraphMededelingen, exportGraphDocuments);
      files.push(documentsFile);
    } else {
      console.log(`Public export of documents didn't exist yet on ${sessionDate}. Documents will not be exported`);
    }
  }

  if (job.documentNotification) {
    const exportGraphDocumentNotification = `http://mu.semte.ch/graphs/export/${timestamp}-document-notification`;
    const documentNotificationFile = await exportDocumentNotification(job.id, sessionUri, exportGraphDocumentNotification, exportFileBase, job.documentNotification);
    files.push(documentNotificationFile);
  }

  return files;
}


async function exportSessionInfo(uuid, sessionUri, exportFileBase, exportGraphSessionInfo) {
  let file = `${exportFileBase}-session-info.ttl`;

  await copySession(sessionUri, exportGraphSessionInfo);

  await writeToFile(exportGraphSessionInfo, file);
  await addGraphAndFileToJob(uuid, exportGraphSessionInfo, file);
  return path.basename(file);
}

async function exportNewsItems(uuid, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphNewsItems) {
  const file = `${exportFileBase}-news-items.ttl`;

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
  return path.basename(file);
}

async function exportMededeling(uuid, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphMededelingen) {
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
  return path.basename(file);
}

async function exportDocuments(uuid, tmpGraph, exportFileBase, exportGraphNewsItems, exportGraphMededelingen, exportGraphDocuments) {
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
  return path.basename(file);
}

async function exportDocumentNotification(uuid, sessionUri, exportGraphDocumentNotification, exportFileBase, documentNotification) {
  const file = `${exportFileBase}-document-notification.ttl`;

  const title = `Documenten ministerraad ${documentNotification.sessionDate}`;
  const description = `De documenten van deze ministerraad zullen beschikbaar zijn vanaf ${documentNotification.documentPublicationDateTime}.`;

  await insertDocumentNotification(exportGraphDocumentNotification);

  await writeToFile(exportGraphDocumentNotification, file);
  await addGraphAndFileToJob(uuid, exportGraphDocumentNotification, file);
  return path.basename(file);
}

export {
  generateExport
}
