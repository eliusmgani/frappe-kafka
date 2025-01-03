# Copyright (c) 2024, eliusmgani and contributors
# For license information, please see license.txt

import frappe
from confluent_kafka import Producer
from frappe.model.document import Document
from confluent_kafka.schema_registry import SchemaRegistryClient

ProducerMap = {}
class KafkaSettings(Document):
	def get_kafka_producer(self):
		if frappe.local.site not in ProducerMap:
			ProducerMap[frappe.local.site] = {}
		
		if f"{self.name}_producer" not in ProducerMap[frappe.local.site]:
			conf = self.get_kafka_config()
			ProducerMap[frappe.local.site][f"{self.name}_producer"] = Producer(**conf)

		return ProducerMap[frappe.local.site][f"{self.name}_producer"]
	

	def get_schema_registry_client(self):
		if frappe.local.site not in ProducerMap:
			ProducerMap[frappe.local.site] = {}
		
		if f"{self.name}_schema_registry" not in ProducerMap[frappe.local.site]:
			conf = self.get_registry_config()
			schema_registry = SchemaRegistryClient(conf)
			if schema_registry:
				ProducerMap[frappe.local.site][f"{self.name}_schema_registry"] = schema_registry
			else:
				frappe.log_error(
					message="Schema Registry Client not found, Check the schema registry configuration settings",
					title="SchemaRegistryClient Error",
				)
				frappe.throw(
					"Schema Registry Client not found, Please check the schema registry configuration settings"
				)

		return ProducerMap[frappe.local.site][f"{self.name}_schema_registry"]
	
	
	def get_kafka_config(self):
		conf = {
			"bootstrap.servers": self.bootstrap_servers,
			"client.id": self.client_id,
			"security.protocol": self.protocol,
			"sasl.mechanism": "PLAIN",
			"sasl.username": self.get_password("api_key"),
			"sasl.password": self.get_password("api_secret"),
		}

		return conf
	
	
	def get_registry_config(self):
		conf = {
			"url": self.schema_registry_url,
		}
		if self.username and self.get_password('password'):
			conf["basic.auth.user.info"] = f"{self.username}:{self.get_password('password')}",

		return conf
