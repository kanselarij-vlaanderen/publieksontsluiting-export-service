import { sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime, uuid } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { parseResult } from './query-helpers';
import { generateExport } from './export';

const SCHEDULED = "scheduled";
const STARTED = "started";
const FINISHED = "done";
const FAILED = "failed";

async function createJob(uuid, session, scope, documentNotification) {
  const jobUri = `http://valvas.data.gift/public-export-jobs/${uuid}`;

  const scopeStatements = (scope || []).map((s) => (
    `${sparqlEscapeUri(jobUri)} ext:scope ${sparqlEscapeString(s)}.`
  ));

  let documentNotificationStatements = '';
  if (documentNotification) {
    const uri = `http://valvas.data.gift/document-notifications/${uuid()}`;
    documentNotificationStatements = `
      ${sparqlEscapeUri(jobUri)} ext:documentNotification ${sparqlEscapeUri(uri)}.
      ${sparqlEscapeUri(uri)} ext:zittingDatum ${sparqlEscapeString(documentNotification.sessionDate)}.
      ${sparqlEscapeUri(uri)} ext:publicationDate ${sparqlEscapeString(documentNotification.documentPublicationDateTime)}.
    `;
  }

  await update(`
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(jobUri)} a ext:PublicExportJob;
                           mu:uuid ${sparqlEscapeString(uuid)};
                           ext:zitting ${sparqlEscapeUri(session.uri)};
                           ext:zittingDatum "${session.geplandeStart}"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
                           ext:status ${sparqlEscapeString(SCHEDULED)};
                           dct:created ${sparqlEscapeDateTime(new Date())};
                           dct:modified ${sparqlEscapeDateTime(new Date())}.
        ${scopeStatements.join('\n')}
        ${documentNotificationStatements}
    }
  }`);
}

async function updateJob(uuid, status) {
  await update(`
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>

  DELETE {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ?job  dct:modified ?modified;
              ext:status ?status.
    }
  } INSERT {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ?job dct:modified ${sparqlEscapeDateTime(new Date())};
             ext:status ${sparqlEscapeString(status)}.
    }
  } WHERE {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ?job a ext:PublicExportJob;
               dct:modified ?modified;
               ext:status ?status;
               mu:uuid ${sparqlEscapeString(uuid)}.
    }
  }`);
}

async function addGraphAndFileToJob(uuid, graph, file) {
  const graphStatement = graph ? `?job ext:graph ${sparqlEscapeUri(graph)} . ` : '';
  const fileStatement = graph ? `?job ext:file ${sparqlEscapeString(file)} . ` : '';
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ${fileStatement}
        ${graphStatement}
    }
  } WHERE {
    GRAPH <http://mu.semte.ch/graphs/public> {
        ?job a ext:PublicExportJob;
             mu:uuid ${sparqlEscapeString(uuid)}.
    }
  }`;
  await update(queryString);
}

async function getJob(uuid) {
  const queryString = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    SELECT ?job ?status ?zitting ?zittingDatum ?graph ?file
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?job a ext:PublicExportJob;
               dct:modified ?modified;
               ext:status ?status;
               ext:zitting ?zitting;
               ext:zittingDatum ?zittingDatum ;
               mu:uuid ${sparqlEscapeString(uuid)}.
        OPTIONAL {
           ?job ext:graph ?graph ;
                ext:file ?file .
        }
      }
    } ORDER BY DESC(?modified) LIMIT 1
  `;
  const jobs = parseResult(await query(queryString));

  const job = jobs.length ? jobs[0] : null;

  if (job) {
    job.id = uuid;

    const scopeQueryString = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?scope
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(job.job)} ext:scope ?scope
      }
    }`;
    const scopeResult = await query(scopeQueryString);
    job.scope = scopeResult.results.bindings.map(b => b.scope.value);

    const documentNotificationQueryString = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?sessionDate ?documentPublicationDateTime
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(job.job)} ext:documentNotification ?documentNotificationUri.
        ?documentNotificationUri ext:zittingDatum ?sessionDate;
                                 ext:publicationDate ?documentPublicationDateTime.
      }
    } LIMIT 1`;
    const documentNotifications = parseResult(await query(documentNotificationQueryString));
    job.documentNotification = documentNotifications.length ? documentNotifications[0] : null;
  }

  return job;
}

async function getNextScheduledJob() {
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  SELECT ?id
  WHERE {
    GRAPH <http://mu.semte.ch/graphs/public> {
      FILTER NOT EXISTS { ?job ext:status ${sparqlEscapeString(STARTED)} . }
      ?job a ext:PublicExportJob;
           dct:created ?created;
           ext:status ${sparqlEscapeString(SCHEDULED)};
           mu:uuid ?id.
    }
  } ORDER BY ASC(?created) LIMIT 1`;
  const result = await query(queryString);
  const bindings = result.results.bindings;
  if (bindings.length == 1) {
    return bindings[0].id.value;
  } else {
    return null;
  }
}

async function executeJob(uuid) {
  const job = await getJob(uuid);

  try {
    await updateJob(uuid, STARTED);
    await generateExport(job);
    await updateJob(uuid, FINISHED);
    await createTtlToDeltaTask(job.job);
    console.log(`finished job ${uuid}`);
  } catch (e) {
    console.log(e);
    await updateJob(uuid, FAILED);
  }
}

async function createTtlToDeltaTask(job) {
  const result = await query(`
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    SELECT ?file
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(job)} ext:file ?file .
      }
    }
  `);

  if (result.results.bindings.length) {
    const status = 'http://redpencil.data.gift/ttl-to-delta-tasks/8C7E9155-B467-49A4-B047-7764FE5401F7'; // not started
    const taskUuid = uuid();
    const taskUri = `<http://valvas.data.gift/ttl-to-delta-tasks/${taskUuid}>`;

    const fileStatements = result.results.bindings.map((binding) => {
      const fileUuid = uuid();
      const fileUri = `<http://valvas.data.gift/files/${fileUuid}>`;
      const physicalFileUri = `<share://${binding['file'].value}>`;
      return `
        ${taskUri} prov:used ${fileUri}.
        ${physicalFileUri} nie:dataSource ${fileUri}.
    `;
    });

    // TODO numberOfRetries should not be required on task creation
    await update(`
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    INSERT DATA {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${taskUri} a <http://mu.semte.ch/vocabularies/ext/TtlToDeltaTask>;
          task:numberOfRetries 0;
          adms:status <${status}> .
          ${fileStatements.join('\n')}
      }
    }
  `);
  } else {
    console.log(`No files generated by export job <${job}>. No need to create a ttl-to-delta task.`);
  }
}

export { getJob, addGraphAndFileToJob, updateJob, createJob, getNextScheduledJob, executeJob, FINISHED, FAILED, STARTED};
