import uniq from 'lodash.uniq';
import {
  copySession,
  getAgendaItemsOfAgenda,
  getProcedurestap,
  copyAgendaItemDetails,
  copyNewsItemForAgendaItemTreatment,
  copyProcedurestapDetails,
  copyMandateeRank,
  copyDocumentsForAgendapunt,
  copyFileTriples,
  getLatestAgendaOfSession,
  getDocumentContainers,
  getLatestVersion,
  insertDocumentAndLatestVersion,
  linkNewsItemsToDocumentVersion,
  calculateNotaNewsItemsPriority,
  calculatePriorityMededelingen,
  insertDocumentNotification
} from '../queries';
import { writeToFile, clean as cleanGraph } from './graph-helpers';
import { addGraphAndFileToJob } from './jobs';

/**
    Not all news-items, announcements and documents have been exported to Valvas in the past.
 */
const EXPORT_SINCE = new Date(Date.parse('2006-07-19T00:00:00.000Z'));
const MEDEDELINGEN_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));
const DOCUMENTS_SINCE = new Date(Date.parse('2016-09-08T00:00:00.000Z'));

const EXPORT_DIR = process.env.EXPORT_DIR || `/data/exports/`;

async function generateExport(job) {
  const scope =  job.scope ? job.scope : ['news-items', 'announcements', 'documents'];

  const sessionDate = new Date(Date.parse(job.zittingDatum));
  console.log(`Generating export for session of ${sessionDate} with scope ${JSON.stringify(scope)}`);

  if (sessionDate < EXPORT_SINCE) {
    console.log(`Public export didn't exist yet on ${sessionDate}. Nothing will be exported`);
    return;
  }

  const sessionUri = job.zitting;
  const timestamp = new Date().toISOString().replace(/\D/g, '');
  const sessionTimestamp = sessionDate.toISOString().replace(/\D/g, '');
  const exportFileBase = `${EXPORT_DIR}${timestamp.substring(0, 14)}-${timestamp.slice(14)}-${job.id}-${sessionTimestamp}`;

  const tmpGraph = `http://mu.semte.ch/graphs/tmp/${timestamp}`;

  const exportGraphSessionInfo = `http://mu.semte.ch/graphs/export/${timestamp}-session-info`;
  await exportSessionInfo(job.id, sessionUri, exportFileBase, exportGraphSessionInfo);

  const agenda = await getLatestAgendaOfSession(sessionUri);
  const agendaUri = agenda.uri;

  if (agendaUri == null) {
    console.log(`No agenda found for session ${sessionUri}. Nothing to export.`);
    return;
  }

  const exportGraphNewsItems = `http://mu.semte.ch/graphs/export/${timestamp}-news-items`;
  if (scope.includes('news-items')) {
    await exportNewsItems(job.id, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphNewsItems);
  }

  const exportGraphMededelingen = `http://mu.semte.ch/graphs/export/${timestamp}-mededelingen`;
  if (scope.includes('announcements')) {
    if (sessionDate > MEDEDELINGEN_SINCE) {
      await exportAnnouncements(job.id, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphMededelingen);
    } else {
      console.log(`Public export of announcements didn't exist yet on ${sessionDate}. Announcements will not be exported`);
    }
  }

  const exportGraphDocuments = `http://mu.semte.ch/graphs/export/${timestamp}-documents`;
  if (scope.includes('documents') && scope.includes('news-items') && scope.includes('announcements')) {
    if (sessionDate > DOCUMENTS_SINCE) {
      await exportDocuments(job.id, tmpGraph, exportFileBase, exportGraphNewsItems, exportGraphMededelingen, exportGraphDocuments);
    } else {
      console.log(`Public export of documents didn't exist yet on ${sessionDate}. Documents will not be exported`);
    }
  }

  const exportGraphDocumentNotification = `http://mu.semte.ch/graphs/export/${timestamp}-document-notification`;
  if (job.documentNotification) {
    await exportDocumentNotification(job.id, sessionUri, exportGraphDocumentNotification, exportFileBase, job.documentNotification);
  }

  console.log('Cleaning all temporary graphs used to generate the export');
  const graphs = [tmpGraph, exportGraphNewsItems, exportGraphMededelingen, exportGraphDocuments, exportGraphDocumentNotification];
  for (let graph of graphs) {
    await cleanGraph(graph);
  }
}


async function exportSessionInfo(uuid, sessionUri, exportFileBase, exportGraphSessionInfo) {
  let file = `${exportFileBase}-session-info.ttl`;

  await copySession(sessionUri, exportGraphSessionInfo);

  const count = await writeToFile(exportGraphSessionInfo, file);
  if (count) {
    const fileUri = file.replace(EXPORT_DIR, 'share://');
    await addGraphAndFileToJob(uuid, exportGraphSessionInfo, fileUri);
  }
}

async function exportNewsItems(uuid, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphNewsItems) {
  const file = `${exportFileBase}-news-items.ttl`;

  // News items for nota's dump
  const agendaItems = await getAgendaItemsOfAgenda(agendaUri, 'nota');
  console.log(`Found ${agendaItems.length} agenda-items of type "nota" with a news item`);
  const procedurestappen = [];
  for (let agendaItem of agendaItems) {
    await copyAgendaItemDetails(agendaItem.agendapunt, tmpGraph);
    await copyNewsItemForAgendaItemTreatment(agendaItem.behandeling, sessionUri,  exportGraphNewsItems, 'mededeling');
    await copyDocumentsForAgendapunt(agendaItem.uri, tmpGraph);
    if (agendaItem.procedurestap) { // Copy mandatee info etc
      await copyProcedurestapDetails(agendaItem.procedurestap, tmpGraph);
      const procedurestap = await getProcedurestap(agendaItem.procedurestap);
      procedurestappen.push(procedurestap);
    }
  }

  const mandatees = uniq(procedurestappen.map(p => p.mandatee).filter(m => m != null));
  console.log(`Found ${mandatees.length} mandatees`);

  for (let mandatee of mandatees) {
    await copyMandateeRank(mandatee, tmpGraph);
  }
  await calculateNotaNewsItemsPriority(exportGraphNewsItems, tmpGraph);

  const count = await writeToFile(exportGraphNewsItems, file);
  if (count) {
    const fileUri = file.replace(EXPORT_DIR, 'share://');
    await addGraphAndFileToJob(uuid, exportGraphNewsItems, fileUri);
  }
}

async function exportAnnouncements(uuid, sessionUri, tmpGraph, exportFileBase, agendaUri, exportGraphMededelingen) {
  const file = `${exportFileBase}-mededelingen.ttl`;

  const mededelingen = await getAgendaItemsOfAgenda(agendaUri, 'mededeling');
  console.log(`Found ${mededelingen.length} agenda-items of type "mededeling" with a news item`);
  const procedurestappen = [];
  for (let mededeling of mededelingen) {
    await copyAgendaItemDetails(mededeling.agendapunt, tmpGraph);
    await copyNewsItemForAgendaItemTreatment(mededeling.behandeling, sessionUri,  exportGraphMededelingen, 'mededeling');
    await copyDocumentsForAgendapunt(mededeling.agendapunt, tmpGraph);
    if (mededeling.procedurestap) {
      // Copy mandatee info etc. Not for calculating priority. Just for display.
      await copyProcedurestapDetails(mededeling.procedurestap, tmpGraph);
      const procedurestap = await getProcedurestap(mededeling.procedurestap);
      procedurestappen.push(procedurestap);
    }
  }
  const mandatees = uniq(procedurestappen.map(p => p.mandatee).filter(m => m != null));
  console.log(`Found ${mandatees.length} mandatees`);
  
  await calculatePriorityMededelingen(exportGraphMededelingen);

  const count = await writeToFile(exportGraphMededelingen, file);
  if (count) {
    const fileUri = file.replace(EXPORT_DIR, 'share://');
    await addGraphAndFileToJob(uuid, exportGraphMededelingen, fileUri);
  }
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

  const count = await writeToFile(exportGraphDocuments, file);
  if (count) {
    const fileUri = file.replace(EXPORT_DIR, 'share://');
    await addGraphAndFileToJob(uuid, exportGraphDocuments, fileUri);
  }
}

async function exportDocumentNotification(uuid, sessionUri, exportGraphDocumentNotification, exportFileBase, documentNotification) {
  const file = `${exportFileBase}-document-notification.ttl`;

  const title = `Documenten ministerraad ${documentNotification.sessionDate}`;
  const description = `De documenten van deze ministerraad zullen beschikbaar zijn vanaf ${documentNotification.documentPublicationDateTime}.`;

  await insertDocumentNotification(exportGraphDocumentNotification, sessionUri, title, description);

  const count = await writeToFile(exportGraphDocumentNotification, file);
  if (count) {
    const fileUri = file.replace(EXPORT_DIR, 'share://');
    await addGraphAndFileToJob(uuid, exportGraphDocumentNotification, fileUri);
  }
}

export {
  generateExport
};
