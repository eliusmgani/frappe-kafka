# Copyright (c) 2024, eliusmgani and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.model import no_value_fields, table_fields

class KafkaConnector(Document):
	@frappe.whitelist()
	def get_source_fields(self, fields):
		"""Get fields from the source document"""

		multicheck_fields = []
		doc_fields = frappe.get_meta(self.data_source).fields

		for field in doc_fields:
			if (
				(
					field.fieldtype not in no_value_fields
					or field.fieldtype in table_fields
				)
				and not field.hidden
			):
				multicheck_fields.append(
					{
						"label": field.label,
						"value": field.fieldname,
						"checked": 1 if field.fieldname in fields else 0,
					}
				)
		
		return multicheck_fields