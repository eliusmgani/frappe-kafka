__version__ = "0.0.1"
__title__ = "Frappe Kafka Client"

import json
import frappe
from confluent_kafka.serialization import (
    StringSerializer,
    MessageField,
    SerializationContext
)
from confluent_kafka.schema_registry.json_schema import JSONSerializer

class FrappeKafkaClient():
    def __init__(self, frappe_connector, source_doc):
        self.source_doc = source_doc
        self.connector = frappe_connector
        self.settings = frappe.get_cached_doc("Kafka Settings", self.connector.kafka_settings)
        self.producer = self.settings.get_kafka_producer()
        self.schema_registry = self.settings.get_schema_registry_client()

    def get_json_data(self):
        data = {}
        if self.connector.selected_fields:
            for row in self.connector.selected_fields:
                data[row.fieldname] = str(self.source_doc.get(row.fieldname)) or ""
        
        else:
            values = self.source_doc.as_dict(convert_dates_to_str=True)
            for key, value in data.items():
                if value is None:
                    data[key] = ""
        
        return data
    
    def get_json_schema(self):
        required = []
        properties = {}

        meta = frappe.get_meta(self.connector.data_source)

        for field in meta.fields:
            if self.connector.selected_fields:
                if field.fieldname in self.connector.selected_fields:
                    properties[field.fieldname] = {
                        "type": self.map_fieldtype_to_json_schema_type(field.fieldtype)
                    }

                    if field.reqd:
                        required.append(field.fieldname)
            else:
                properties[field.fieldname] = {
                    "type": self.map_fieldtype_to_json_schema_type(field.fieldtype)
                }

                if field.reqd:
                    required.append(field.fieldname)
        
        schema = {
            "title": f"{self.connector.data_source}Schema",
            "type": "object",
            "properties": properties
        }
        # "$schema": "http://json-schema.org/draft-07/schema#",
        
        if required:
            schema["required"] = required

        return schema
    
    def map_fieldtype_to_json_schema_type(self, fieldtype):
        """Map Frappe field types to JSON Schema types"""
        type_mapping = {
            'Data': 'string',
            'Text': 'string',
            'Long Text': 'string',
            'Small Text': 'string',
            'Int': 'integer',
            'Float': 'number',
            'Currency': 'number',
            'Date': 'string',
            'Datetime': 'string',
            'Check': 'boolean',
            'JSON': 'object'
        }
        return type_mapping.get(fieldtype, 'string')

    def send_message(self):
        if self.connector.data_format == "JSON":
            self.send_json_data()
    
    def send_json_data(self):
        # default json serializer conf
        serializer_conf = {
            "auto.register.schemas": True,
            "normalize.schemas": True,
            "use.latest.version": False,
            # "use.deprecated.format": False,
        }
        
        json_data = self.get_json_data()
        json_schema = self.get_json_schema()
        json_schema_str = json.dumps(json_schema)
		
		# try:
        # string_serializer = StringSerializer('utf_8')
		
        json_serializer = JSONSerializer(
			json_schema_str,
			self.schema_registry,
			lambda x, json_data: x,
            serializer_conf
		)
		
        ctx = SerializationContext(
			self.connector.kafka_topic,
			MessageField.VALUE
		)
		
        serialized_value = json_serializer(json_data, ctx)
        # serialized_key = string_serializer(json_data.get("name"))
        serialized_key = json_data.get("name", "").encode('utf-8') if json_data.get("name") else None
		
        self.producer.produce(
			self.connector.kafka_topic,
			key=serialized_key,
			value=serialized_value,
			callback=self.delivery_report
		)
        self.producer.poll(0)
        
        # except Exception as e:
        #     frappe.log_error(f"Error sending data to Kafka: {e}")
        #     frappe.throw(title="Kafka Error", msg=f"{e}")
        
        self.producer.flush()
    
    def delivery_report(self, err, msg):
        if err is not None:
            frappe.log_error(f"Message delivery failed: {err}")
        else:
            frappe.log(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
