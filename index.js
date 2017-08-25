/*eslint-env node */
/**
 * Cloud Function.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} The callback function.
 */
exports.bqExport = function bqExport(req, res) {

	const projectId = "bryanarm-sandbox";
	const resultFileName = "results.txt";

	// GCS dependency import
	const Storage = require("@google-cloud/storage");
	// Instantiates a client
	const storage = Storage({
		projectId: projectId,
		keyFilename: "./bryanarm-sandbox-c9f6422fe4e3.json"
	});

	// Imports the Google Cloud client library
	const BigQuery = require("@google-cloud/bigquery");

	// Instantiates a client
	const bq = BigQuery({
		projectId: projectId,
		keyFilename: "./bryanarm-sandbox-c9f6422fe4e3.json"
	});

	console.log("Starting BQ exporter");

	var destinationBucket = req.body.destinationBucket || false;

	if (destinationBucket) {

		console.log("Destination storage bucket: " + destinationBucket);

		let bucket = storage.bucket(destinationBucket);
		let sqlQuery = "SELECT * FROM shared_views.exported_view";

		// Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
		const options = {
			query: sqlQuery,
			timeoutMs: 30000,
			useLegacySql: true // Using legacy because apparently I have a legacy view?
		};

		console.log("Running query...");
		console.log("Hi Cloud Masters!");
		let strResults = "";
		bq.query(options)
			.then((results) => {
				const rows = results[0];
				
				console.log("Getting file reference...");
				var remoteWriteStream = bucket.file("test_stream_dest.json").createWriteStream();
		
				remoteWriteStream.write("\n\n NEW DATA " + new Date().toISOString() + " \n\n");
				console.log("Iterating through rows...");
				rows.forEach(function(row) {
					let current = "";
					for (let key in row) {
						if (current) {
							current = `${current}\n`;
						}
						current = `${current}${key}: ${row[key]}`;
					}
					remoteWriteStream.write(current);
				});
				remoteWriteStream.end();
				console.log("Query finished successfully: " + strResults); 
			});
			
//		console.log("Part two: exporting tables");
//		
//		let exportFile = storage.bucket(destinationBucket).file("test_export_dest.txt");
//		
//		let exportOptions = {
//			format: "csv",
//			gzip: true
//		};
//		
//		bq.dataset("shared_views").table("exported_view").export(exportOptions, options, function(e, job, apiResponse) {});
		

	} else {

		console.error("No destination bucket defined.  Exiting.");
		res.status(400).end();
	}


	res.status(200).end();
};