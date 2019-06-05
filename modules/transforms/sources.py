import apache_beam as beam
import logging, json

class PubSubEventSource(beam.PTransform):
	def __init__(self, event_subscription=None, event_type=None):
		self.event_subscription = event_subscription
		self.event_type = event_type

	def parse_json(self, element):
		try:
			element = json.loads(element)
		except:
			logging.error("PubSubEventSource : Event : Invalid JSON")
		else:
			return element

	def filter_by_type(self, element):
		if not isinstance(element, dict):
			return False
		if element.get("type") == self.event_type:
			logging.info("PubSubEventSource : Event ID : %s " % element.get("id"))
			logging.info("PubSubEventSource : Event Type : %s " % element.get("type"))
		return element.get("type") == self.event_type		

	def expand(self, pinput):
		return ( pinput | beam.io.ReadFromPubSub(subscription=self.event_subscription)
						| beam.Map(self.parse_json)
						| beam.Filter(self.filter_by_type) )
