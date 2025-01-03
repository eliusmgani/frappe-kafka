import frappe
from frappe_kafka import FrappeKafkaClient


def send_kafka(kwargs):
    doc = kwargs.get("doc")
    doc_list = kwargs.get("doc_list")
    connector_name = kwargs.get("connector_name")

    if not doc and not doc_list:
        frappe.throw("Document or Document List is required to send message to Kafka")
    
    if not connector_name:
        frappe.throw("Connector name is required to send message to Kafka")
    
    doc_list = doc_list or []
    if doc:
        doc_list.append(doc)
    
    frappe_connector = frappe.get_cached_doc("Kafka Connector", connector_name)

    if len(doc_list) > 0:
        for doc in doc_list:
            kafka_client = FrappeKafkaClient(frappe_connector, doc)
            kafka_client.send_message()