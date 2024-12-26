# Copyright (c) 2024, eliusmgani and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.safe_exec import get_safe_globals
from frappe.model import no_value_fields, table_fields

class KafkaConnector(Document):
	def validate(self):
		self.validate_docevent()
		self.validate_condition()
	
	def validate_docevent(self):
		if self.data_source:
			is_submittable = frappe.get_cached_value("DocType", self.data_source, "is_submittable")
			if not is_submittable:
				submittable_events = ["on_submit", "on_cancel", "on_update_after_submit"]

				for row in self.events:
					if row.event_name in submittable_events:
						frappe.throw(_(f"DocType must be Submittable for the selected Doc Event: {row.event_name}"))


	def validate_condition(self):
		temp_doc = frappe.new_doc(self.data_source)
		if self.condition:
			try:
				frappe.safe_eval(self.condition, eval_locals=get_context(temp_doc))
			except Exception as e:
				frappe.throw(_(f"Invalid Condition: {e}"))
	
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


def get_context(doc):
	return {"doc": doc, "utils": get_safe_globals().get("frappe").get("utils")}
