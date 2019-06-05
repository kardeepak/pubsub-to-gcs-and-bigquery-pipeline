import apache_beam as beam
import logging, json, datetime, s2sphere
import random

class AttachTimestamp(beam.DoFn):
	def __init__(self, options):
		self.options = options

	def round_time(self, date, round_to, to='down'):
		"""
		Round a datetime object to a multiple of a timedelta
		date : datetime.datetime object.
		round_to : seconds to which needs to be rounded.
		"""
		seconds = (date - date.min).seconds

		if seconds % round_to == 0:
			rounding = (seconds + round_to / 2) // round_to * round_to
		else:
			if to == 'up':
				rounding = (seconds + round_to) // round_to * round_to
			elif to == 'down':
				rounding = seconds // round_to * round_to
			else:
				rounding = (seconds + round_to / 2) // round_to * round_to

		return date + datetime.timedelta(0, rounding - seconds, -date.microsecond)

	def process(self, element):
		logging.info("AttachTimestamp : Attaching Timestamp to Event : %s" % element.get("id"))
		dt = None
		event_type = element.get("type")
		if event_type == self.options.driver_event_type:
			dt = datetime.datetime.strptime(element["body"]["lastUpdatedAt"], "%Y-%m-%dT%H:%M:%SZ")
		elif event_type == self.options.passenger_event_type:
			dt = datetime.datetime.fromtimestamp(float(element["createdAt"]) / (10**9))
		elif event_type == self.options.trip_event_type:
			dt = datetime.datetime.fromtimestamp(float(element["created_at"] / 1000))

		if dt is not None:
			dt = self.round_time(dt, self.options.window_size)
			yield (dt.strftime("%Y/%m/%d/%H-%M-%S"), element)

class AttachCellID(beam.DoFn):
	def __init__(self, options):
		self.options = options

	def lat_lng_to_cell(self, lat, lng, parent=15):
		p1 = s2sphere.LatLng.from_degrees(lat, lng)
		cell = s2sphere.CellId.from_lat_lng(p1).parent(parent)
		cell_id = cell.id()
		cell_token = str(cell.to_token())
		return cell_id, cell_token

	def process(self, element):
		location = None
		timestamp, value = element
		event_type = value.get("type")

		logging.info("AttachCellID : Attaching Cell ID To Event : %s" % value.get("id"))

		if event_type == self.options.driver_event_type:
			location = value.get("body", {}).get("location")
		elif event_type == self.options.passenger_event_type:
			location = value.get("body", {}).get("passengerLocation")
		elif event_type == self.options.trip_event_type:
			location = value.get("body", {}).get("pickup", {}).get("location", [ None, ])[0]

		if location is not None:
			latitute = float(location["lat"])
			longitude = float(location["lng"])
			cell_id, cell_token = self.lat_lng_to_cell(latitute, longitude)
			yield ((str(cell_id), timestamp), value)

class AttachVehicleModel(beam.DoFn):
	def __init__(self, options):
		self.options = options

	def process(self, element):
		key, value = element
		cell_id, timestamp = key
		vehicle_model = None
		event_type = value.get("type")

		logging.info("AttachVehicleModel : Attaching Vehicle Model To Event : %s" % value.get("id"))

		if event_type == self.options.driver_event_type or event_type == self.options.passenger_event_type:
			vehicle_model = value.get("body", {}).get("vehicleModel")
		elif event_type == self.options.trip_event_type:
			vehicle_model = value.get("body", {}).get("vehicle_type")

		if vehicle_model is not None:
			yield ((cell_id, timestamp, vehicle_model), value)

class AttachVehicleModelForStorageSink(beam.DoFn):
	def __init__(self, options):
		self.options = options

	def process(self, element):
		timestamp, value = element
		vehicle_model = None
		event_type = value.get("type")

		logging.info("AttachVehicleModelToStorageSink : Attaching Vehicle Model To Event : %s" % value.get("id"))

		if event_type == self.options.driver_event_type or event_type == self.options.passenger_event_type:
			vehicle_model = value.get("body", {}).get("vehicleModel")
		elif event_type == self.options.trip_event_type:
			vehicle_model = value.get("body", {}).get("vehicle_type")

		if vehicle_model is not None:
			yield((str(vehicle_model), timestamp), value)

class DriverCountFilter(beam.DoFn):
	def __init__(self, zero):
		self.zero = zero 

	def process(self, element):
		key, value = element
		drivers = value.get("body", {}).get("availableDrivers", [])
		if self.zero and len(drivers) == 0:
			yield element
		elif not self.zero and len(drivers) > 0:
			yield element

class FreeDriverFilter(beam.DoFn):
	def process(self, element):
		key, value = element
		if value.get("body", {}).get("status") == "F" and value.get("body", {}).get("shiftIn", False):
			yield element

