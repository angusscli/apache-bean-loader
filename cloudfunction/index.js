/**
 * Responds to any HTTP request that can provide a "message" field in the body.
 *
 * @param {!Object} req Cloud Function request context.
 * @param {!Object} res Cloud Function response context.
 */
const google = require('googleapis');

exports.trigger = (req, res) => {
    var date = new Date();
  var timestamp =  date.getFullYear() + ("0" + (date.getMonth() + 1)).slice(-2) + ("0" + date.getDate()).slice(-2) + ("0" + date.getHours() + 1 ).slice(-2) + ("0" + date.getMinutes()).slice(-2) + ("0" + date.getSeconds()).slice(-2) ;
  
        google.auth.getApplicationDefault(function(err, authClient, projectId) {
            if (err) {
                throw err;
            }
            if (authClient.createScopedRequired && authClient.createScopedRequired()) {
                authClient = authClient.createScoped([
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/userinfo.email'
                ]);
            }
  
  
          const dataflow = google.dataflow({
                version: 'v1b3',
                auth: authClient
            });
            dataflow.projects.templates.create({
                projectId: 'traded-risk-project-1',
                resource: {
                    jobName: `irc-stream-pipeline-${timestamp}`,
                    gcsPath: 'gs://staging-testing-43541281/templates/StreamLoaderTemplate'
                }
            }, function(err, response) {
                if (err) {
  
                    console.error("problem running dataflow template, error was: ", err);
                }
  
                console.log("Dataflow template response: ", response);
                res.status(200).send('Done\n');
            });
        });
  
  
res.status(200).send('Done\n');
  
};
