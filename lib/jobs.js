import { query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';
import { parseResult } from './query-helpers';

const SCHEDULED = "scheduled";
const STARTED = "started";
const FINISHED = "done";
const FAILED = "failed";

async function createJob(uuid, session) {
  const jobUri=`http://mu.semte.ch/public-export-jobs/${uuid}`;
  const queryString = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT DATA {
    GRAPH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")} {
        ${sparqlEscapeUri(jobUri)} a ext:PublicExportJob;
                           mu:uuid ${sparqlEscapeString(uuid)};
                           ext:zitting ${sparqlEscapeUri(session.uri)};
                           ext:zittingDatum "${session.geplandeStart}"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
                           ext:status ${sparqlEscapeString(SCHEDULED)};
                           dct:created ${sparqlEscapeDateTime(new Date())};
                           dct:modified ${sparqlEscapeDateTime(new Date())}.
    }
  }`;
  await update(queryString);
}
async function getFirstScheduledJobId() {
  const queryString = `
  PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  SELECT ?id
  FROM ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
  WHERE {
      ?job a ext:PublicExportJob;
    dct:created ?created;
    ext:status ${sparqlEscapeString(SCHEDULED)};
    mu:uuid ?id.
  } ORDER BY ASC(?created) LIMIT 1`;
  const result = await query(queryString);
  const bindings = result.results.bindings;
  if (bindings.length == 1) {
    return bindings[0].id.value;
  }
  else
    return null;
}

async function updateJob(uuid, status) {
  const queryString = `
  PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  WITH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
  DELETE {
        ?job  dct:modified ?modified;
              ext:status ?status.
   }
  INSERT {
        ?job dct:modified ${sparqlEscapeDateTime(new Date())};
                           ext:status ${sparqlEscapeString(status)}.
    }
  WHERE {
        ?job a ext:PublicExportJob;
                           dct:modified ?modified;
                           ext:status ?status;
                           mu:uuid ${sparqlEscapeString(uuid)}.
}`;
  await update(queryString);
}

async function addGraphAndFileToJob(uuid, graph, file) {
  const graphStatement = graph ? `?job ext:graph ${sparqlEscapeUri(graph)} . ` : '';
  const queryString = `
  PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  WITH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
  INSERT {
        ?job ext:file ${sparqlEscapeString(file)}.
        ${graphStatement}
    }
  WHERE {
        ?job a ext:PublicExportJob;
                           mu:uuid ${sparqlEscapeString(uuid)}.
}`;
  await update(queryString);
}
async function getJob(uuid) {
  const queryString = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX dct: <http://purl.org/dc/terms/>
    SELECT ?job ?status ?zitting ?zittingDatum ?graph ?file
    FROM ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
    WHERE {
        ?job a ext:PublicExportJob;
                           dct:modified ?modified;
                           ext:status ?status;
                           ext:zitting ?zitting;
                           ext:zittingDatum ?zittingDatum ;
                           mu:uuid ${sparqlEscapeString(uuid)}.
        OPTIONAL { ?job ext:graph ?graph;  ext:file ?file. }
    }
`;
  const jobs =  parseResult(await query(queryString));
  return jobs.length ? jobs[0] : null;
}

export { getJob, addGraphAndFileToJob, updateJob, createJob, getFirstScheduledJobId, FINISHED, FAILED, STARTED}
