import { query, update, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';

const SCHEDULED = "scheduled";
const STARTED = "started";
const FINISHED = "done";
const FAILED = "failed";
async function createJob(uuid, zitting_uri) {
  const job_uri=`http://mu.semte.ch/public-export-jobs/${uuid}`;
  const queryString = `
  PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  INSERT DATA {
    GRAPH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")} {
        ${sparqlEscapeUri(job_uri)} a ext:PublicExportJob;
                           mu:uuid ${sparqlEscapeString(uuid)};
                           ext:zitting ${sparqlEscapeUri(zitting_uri)};
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
  WITH GRAPH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
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

async function addGraphAndFileToJob(uuid,graph, file) {
  const queryString = `
  PREFIX    mu: <http://mu.sedmte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  WITH GRAPH ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
  INSERT {
        ?job ext:graph ${sparqlEscapeUri(graph)};
                           ext:file ${sparqlEscapeString(file)}.
    }
  WHERE {
        ?job a ext:PublicExportJob;
                           dct:modified ?modified;
                           ext:status ?status;
                           mu:uuid ${sparqlEscapeString(uuid)}.
}`;
  await update(queryString);
}
async function getJob(uuid) {
  const queryString = `
  PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX    ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dct: <http://purl.org/dc/terms/>
  SELECT ?job ?status ?zitting ?graph ?file
  FROM ${sparqlEscapeUri("http://mu.semte.ch/graph/public-export-jobs")}
  WHERE {
        ?job a ext:PublicExportJob;
                           dct:modified ?modified;
                           ext:status ?status;
                           ext:zitting ?zitting;
                           mu:uuid ${sparqlEscapeString(uuid)}.
        OPTIONAL { ?job ext:graph ?graph;  ext:file ?file. }
}
`;
return  await query(queryString);
}

export { getJob, addGraphAndFileToJob, updateJob, createJob, getFirstScheduledJobId, FINISHED, FAILED, STARTED}
