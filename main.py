from __future__ import absolute_import

import logging, apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

from modules.options import CustomOptions
from modules.transforms import PubSubEventSource, StorageSink, BigQuerySink
from modules.transforms.funcs import AttachTimestamp, AttachCellID, AttachVehicleModel, AttachVehicleModelForStorageSink
from modules.transforms.funcs import FreeDriverFilter, DriverCountFilter

def run(argv=None):
	custom_options = PipelineOptions().view_as(CustomOptions)
	pipeline_options = PipelineOptions()

	with beam.Pipeline(options=pipeline_options) as p:
		event_types = ["Driver", "Passenger", "Trip"]
		events_colls = {}

		for key in event_types:
			events = ( p | ("Reading %s Events From PubSub" % key) >> 
								PubSubEventSource(
									event_subscription=getattr(custom_options, "%s_event_subscription" % key.lower()),
									event_type=getattr(custom_options, "%s_event_type" % key.lower())
								)
						 | ("Attaching Timestamps to %s Events" % key) >> beam.ParDo(AttachTimestamp(custom_options)) )

			events_colls[key] = ( events | ("Window For %s Events" % key) >> beam.WindowInto(window.FixedWindows(custom_options.window_size)) )

			( events_colls[key] | ("Attaching Vehicle Model to %s Events for Storage Sink" % key) >> beam.ParDo(AttachVehicleModelForStorageSink(custom_options))
								| ("Group %s Events by Vehicle Model and Timestamps" % key) >> beam.GroupByKey()
								| ("Storage Sink For %s Events" % key) >> beam.ParDo(StorageSink(custom_options.output_bucket, getattr(custom_options, "%s_event_output_folder" % key.lower()))) )

			events_colls[key] = ( events_colls[key] | ("Attach Cell ID To %s Events" % key) >> beam.ParDo(AttachCellID(custom_options))
													| ("Attach Vehicle Model to %s Events" % key) >> beam.ParDo(AttachVehicleModel(custom_options)) )

		counts = {}
		counts["TRIP_REQUEST_COUNT"] = ( events_colls["Trip"] | "Count Trip Create Events" >> beam.combiners.Count.PerKey() )


		counts["APP_OPEN_COUNT_ZERO_VEHICLES"] = ( events_colls["Passenger"] | "Filter Events with No Available Drivers" >> beam.ParDo(DriverCountFilter(zero=True)) 
				| "Count App Open Events With No Drivers" >> beam.combiners.Count.PerKey() )

		counts["APP_OPEN_COUNT_NONZERO_VEHICLES"] = ( events_colls["Passenger"] | "Filter Events with Available Drivers" >> beam.ParDo(DriverCountFilter(zero=False))
				| "Count App Open Events With Drivers" >> beam.combiners.Count.PerKey() )
		
		counts["FREE_DRIVER_COUNT"] = ( events_colls["Driver"] | "Filter Events with Free Drivers" >> beam.ParDo(FreeDriverFilter())
				| "Count Free Drivers" >> beam.combiners.Count.PerKey() )

		( counts | "Group Different Counts By Key" >> beam.CoGroupByKey() 
				 | "BigQuery Update Counts" >> beam.ParDo(BigQuerySink(custom_options.project, custom_options.output_bigquery_dataset, custom_options.output_bigquery_table)) )


if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	run()
