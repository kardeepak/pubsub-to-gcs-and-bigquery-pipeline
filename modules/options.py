from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

class CustomOptions(GoogleCloudOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
	parser.add_argument("--driver-event-subscription", help="Driver Event Subscription of PubSub", required=True)
	parser.add_argument("--driver-event-type", help="Type of Driver Event", default="DriverStatusChanged")
	parser.add_argument("--driver-event-output-folder", help="Output Folder for Driver Event", default="driver-events")

	parser.add_argument("--passenger-event-subscription", help="Passenger Event Subscription of PubSub", required=True)
	parser.add_argument("--passenger-event-type", help="Type of Passenger Event", default="driver_requested")
	parser.add_argument("--passenger-event-output-folder", help="Output Folder for Passenger Event", default="passenger-events")
	
	parser.add_argument("--trip-event-subscription", help="Trip Event Subscription of PubSub", required=True)
	parser.add_argument("--trip-event-type", help="Type of Trip Event", default="trip_created")
	parser.add_argument("--trip-event-output-folder", help="Output Folder for Trip Event", default="trip-events")
	
	parser.add_argument("--output-bucket")
	parser.add_argument("--output-bigquery-dataset")
	parser.add_argument("--output-bigquery-table")

	parser.add_argument("--window-size", type=int, default=5*60)
