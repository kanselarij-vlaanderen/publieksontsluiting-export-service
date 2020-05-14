import { query, uuid, sparqlEscapeUri } from 'mu';

const statusUris = {
  'not-started': 'http://redpencil.data.gift/ttl-to-delta-tasks/8C7E9155-B467-49A4-B047-7764FE5401F7',
  'started': 'http://redpencil.data.gift/ttl-to-delta-tasks/B9418001-7DFE-40EF-8950-235349C2C7D1',
  'completed': 'http://redpencil.data.gift/ttl-to-delta-tasks/89E2E19A-91D0-4932-9720-4D34E62B89A1',
  'error': 'http://redpencil.data.gift/ttl-to-delta-tasks/B740E2A0-F8CC-443E-A6BE-248393A0A9AE',
};

export async function createTaskToDelta(files) {
  const taskUuid = uuid();
  const taskUri = `<http://mu.semte.ch/graphs/public/delta-task/${taskUuid}>`;
  await query(`
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    INSERT DATA{
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${taskUri} a <http://mu.semte.ch/vocabularies/ext/TtlToDeltaTask>;
          rdfs:label 'TestTask';
          rdfs:comment 'Test task to try the service';
          task:numberOfRetries 0;
          adms:status ${sparqlEscapeUri(statusUris['not-started'])}.
          ${files.map((file) => {
            const fileUuid = uuid();
            const fileUri = `<http://mu.semte.ch/graphs/public/file/${fileUuid}>`;
            const physicalFileUri = `<share://${file}>`;
            return `
              ${taskUri} prov:used ${fileUri}.
              ${physicalFileUri} nie:dataSource ${fileUri}.
            `;
          }).join('\n')}
      }
    }
  `);
}