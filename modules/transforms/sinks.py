import apache_beam as beam
import logging, json, os, datetime, time
from google.cloud import storage, bigquery, datastore

class StorageSink(beam.DoFn):
	def __init__(self, bucket, folder):
		self.bucket = bucket
		self.folder = folder

	def process(self, element):
		key, values = element
		vehicle_model, output_folder = key

		logging.info("StorageSink : Event Count : (%s, %d)" % (output_folder, len(values)))
		
		dirname = os.path.join(os.path.join(self.folder, vehicle_model), output_folder)
		
		logging.info("StorageSink : Directory : %s" % dirname)
		
		if self.bucket is None:
			if not os.path.isdir(dirname):
				os.makedirs(dirname)

			count = len(os.listdir(dirname))
			filepath = os.path.join(dirname, ("%.2d.json" % count))

			logging.info("StorageSink : Filepath : %s" % filepath)

			with open(filepath, "w") as fout:
				json.dump(values, fout, sort_keys=True, indent=4, separators=("," , ": "))
		else:
			client = storage.Client()
			bucket = client.get_bucket(self.bucket)

			count = len(list(bucket.list_blobs(prefix=dirname)))
			filepath = os.path.join(dirname, ("%.2d.json" % count))

			logging.info("StorageSink : Filepath : gs://%s/%s" % (self.bucket, filepath))

			blob = bucket.blob(filepath)
			blob.upload_from_string(json.dumps(values, sort_keys=True, indent=4, separators=(",", ": ")))

class BigQuerySink(beam.DoFn):
	def __init__(self, project, dataset_name, table_name):
		self.project = project
		self.dataset_name = dataset_name
		self.table_name = table_name

	def get_ratio(self, cell_id):
		client = datastore.Client()
		query = client.query(kind="ratio")
		query.add_filter("CELL_ID", "=", cell_id)
		ents = list(query.fetch())
		if ents:
			return ents[0]["RATIO"]
		return 1.0

	def process(self, element):
		key, dictionary = element

		logging.info("BigQuerySink : Key-Value : %s : %s" % (key, str(dictionary)))

		cell_id, timestamp, vehicle_model = key
		dt = datetime.datetime.strptime(timestamp, "%Y/%m/%d/%H-%M-%S")
		timestamp = dt.strftime("%d-%m-%Y %H:%M:%S")

		row = {}
		row["INSERTION_TIME"] = int(time.time() * 10**6)
		row["TIMESTAMP"] = timestamp
		row["CELL_ID"] = cell_id
		row["VEHICLE_MODEL"] = vehicle_model
		row["TRIP_REQUEST_COUNT"] = sum(dictionary["TRIP_REQUEST_COUNT"])
		row["APP_OPEN_COUNT_ZERO_VEHICLES"] = sum(dictionary["APP_OPEN_COUNT_ZERO_VEHICLES"])
		row["APP_OPEN_COUNT_NONZERO_VEHICLES"] = sum(dictionary["APP_OPEN_COUNT_NONZERO_VEHICLES"])
		row["FREE_DRIVER_COUNT"] = sum(dictionary["FREE_DRIVER_COUNT"])

		client = bigquery.Client()
		table_ref = client.dataset(self.dataset_name).table(self.table_name)
		table = client.get_table(table_ref)

		query = (
			"SELECT * FROM `%s.%s.%s` " 
			"WHERE "
			"CELL_ID = '%s' AND " 
			"TIMESTAMP = '%s' AND "
			"VEHICLE_MODEL = '%s' "
			"ORDER BY INSERTION_TIME DESC "
			"LIMIT 1"
		) % (self.project, self.dataset_name, self.table_name, cell_id, timestamp, vehicle_model)

		logging.info("BigQuerySink : Searching Query : %s" % query)

		rows = list(client.query(query).result())

		if rows:
			logging.info("BigQuerySink : Existing Row Found")
			row["TRIP_REQUEST_COUNT"] += rows[0]["TRIP_REQUEST_COUNT"] if rows[0]["TRIP_REQUEST_COUNT"] is not None else 0
			row["APP_OPEN_COUNT_ZERO_VEHICLES"] += rows[0]["APP_OPEN_COUNT_ZERO_VEHICLES"] if rows[0]["APP_OPEN_COUNT_ZERO_VEHICLES"] is not None else 0
			row["APP_OPEN_COUNT_NONZERO_VEHICLES"] += rows[0]["APP_OPEN_COUNT_NONZERO_VEHICLES"] if rows[0]["APP_OPEN_COUNT_NONZERO_VEHICLES"] is not None else 0
			row["FREE_DRIVER_COUNT"] += rows[0]["FREE_DRIVER_COUNT"] if rows[0]["FREE_DRIVER_COUNT"] is not None else 0
		
		ratio = self.get_ratio(cell_id)
		row["SUPPLY"] = row["FREE_DRIVER_COUNT"]
		row["DEMAND"] = row["TRIP_REQUEST_COUNT"] + ratio * row["APP_OPEN_COUNT_ZERO_VEHICLES"]

		logging.info("BigQuerySink : Inserting Row : %s" % str(row))

		result = client.insert_rows(table, [row])
		if not result:	result = "Successful"

		logging.info("BigQuerySink : Inserting Row Result : %s " % str(result))
