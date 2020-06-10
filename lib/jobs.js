import { query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime, uuid } from 'mu';
import { parseResult } from './query-helpers';

const SCHEDULED = "scheduled";
const STARTED = "started";
const FINISHED = "done";
const FAILED = "failed";

async function createJob(uuid, session, scope, documentNotification) {
  const jobUri = `http://mu.semte.ch/public-export-jobs/${uuid}`;

  const scopeStatements = (scope || []).map((s) => (
    `${sparqlEscapeUri(jobUri)} ext:scope ${sparqlEscapeString(s)}.`
  ));

  const documentNotificationStatements = '';
  if (documentNotification) {
    const uri = `http://mu.semte.ch/document-notifications/${uuid()}`;
    documentNotificationStatements = `
      ${sparqlEscapeUri(jobUri)} ext:documentNotification ${sparqlEscapeUri(uri)}.
      ${sparqlEscapeUri(uri)} ext:zittingDatum ${sparqlEscapeString(documentNotification.sessionDate)}.
      ${sparqlEscapeUri(uri)} ext:publicationDate ${sparqlEscapeString(documentNotification.documentPublicationDateTime)}.
    `;
  }

  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT DATA {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
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
  }`;
  await update(queryString);
}

async function updateJob(uuid, status) {
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>

  DELETE {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
        ?job  dct:modified ?modified;
              ext:status ?status.
    }
  } INSERT {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
        ?job dct:modified ${sparqlEscapeDateTime(new Date())};
             ext:status ${sparqlEscapeString(status)}.
    }
  } WHERE {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
        ?job a ext:PublicExportJob;
               dct:modified ?modified;
               ext:status ?status;
               mu:uuid ${sparqlEscapeString(uuid)}.
    }
  }`;
  await update(queryString);
}

async function addGraphAndFileToJob(uuid, graph, file) {
  const graphStatement = graph ? `?job ext:graph ${sparqlEscapeUri(graph)} . ` : '';
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
        ?job ext:file ${sparqlEscapeString(file)}.
        ${graphStatement}
    }
  } WHERE {
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
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
      GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
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
    const scopeQueryString = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?scope
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
        ${sparqlEscapeUri(job.job)} ext:scope ?scope
      }
    }`;
    const scopeResult = await query(scopeQueryString);
    job.scope = scopeResult.results.bindings.map(b => b.scope);

    const documentNotificationQueryString = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?sessionDate ?documentPublicationDateTime
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
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
    GRAPH <http://mu.semte.ch/graphs/public-export-jobs> {
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

export { getJob, addGraphAndFileToJob, updateJob, createJob, getNextScheduledJob, FINISHED, FAILED, STARTED};
