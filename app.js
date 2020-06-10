import { app, uuid, errorHandler } from 'mu';
import { createTaskToDelta } from './lib/task-helpers';
import { createJob, getNextScheduledJob, getJob, executeJob, FINISHED } from './lib/jobs';
import { getSession } from './queries';
import bodyParser from 'body-parser';

app.get('/', function( req, res ) {
  res.send('Hello from valvas-export-service');
} );

app.get('/export/:uuid', async function(req,res) {
  const job = await getJob(req.params.uuid);
  if (job.status === FINISHED) {
    res.status(200).send({status: job.status, export: job.file, graph: job.graph});
  } else {
    res.status(406).send({status: job.status});
  }
});

app.post('/export/:uuid', bodyParser.json(), async function(req, res) {
  const sessionId = req.params.uuid;
  const scope = req.body.scope;
  if (scope && scope.includes('documents') && (!scope.includes('news-items') || !scope.includes('announcements'))) {
    res.status(400).send({
      error: 'If "documents" is included in the scope "news-items" and "announcements" also need to be included'
    });
  }
  const documentNotification = req.body.documentNotification;
  if (documentNotification && (!documentNotification.sessionDate || !documentNotification.documentPublicationDateTime)) {
    res.status(400).send({
      error: 'Fields "sessionDate" and "documentPublicationDateTime" are required for a document notification'
    });
  }

  const session = await getSession(sessionId);
  if (session) {
    const jobId = uuid();
    await createJob(jobId, session, scope, documentNotification);
    executeJobs();  // async execution of export job
    res.status(202).send({
      jobId
    });
  } else {
    res.status(404).send(
      { error: `Could not find session with uuid ${sessionId} in Kaleidos`}
    );
  }
});

app.use(errorHandler);

executeJobs();

async function executeJobs() {
  const job = await getNextScheduledJob();
  if (job) {
    await executeJob(job);
    executeJobs(); // trigger execution of next job if there is one scheduled
  }
  // else: no job scheduled. Nothing should happen
}
