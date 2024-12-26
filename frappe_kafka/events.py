import frappe
from frappe.utils.safe_exec import add_data_utils


def get_safe_frappe_utils():
    data_utils = frappe._dict()
    add_data_utils(data_utils)
    return data_utils

EVENT_CONTEXT = {"utils": get_safe_frappe_utils()}

def get_kafka_connector_events(doctype) -> dict:
    def event_generator():
        events = {}
        event_list = frappe.db.get_all(
            "Kafka Connector",
            filters={"disabled": 0, "data_source": doctype},
            fields=["name", "condition"]
        )

        for row in event_list:
            conn_events = frappe.db.get_all(
                "Kafka DocEvent Detail", 
                {"parent": row.name},
                ["event_name"],
                pluck="event_name"
            )

            row.update({
                "events": conn_events
            })

            events.setdefault(doctype, []).append(row)
        
        return events
    
    return frappe.cache().get_value("doc_events", generator=event_generator)


def trigger_events(doc, method):
    if not method:
        return
    
    if (
        frappe.flags.in_import
        or frappe.flags.in_patch
        or frappe.flags.in_install
        or frappe.flags.in_migrate
    ):
        return

    event_list = {
        "on_update", "after_insert", "on_submit", "on_cancel", "on_trash"
    }

    if not doc.flags.in_insert:
        event_list.update(["on_change", "before_update_after_submit"])
    
    if method not in event_list:
        return
    
    if frappe.flags.event_executed is None:
        frappe.flags.event_executed = {}
    
    if frappe.flags.doc_events is None:
        frappe.flags.doc_events = get_kafka_connector_events(doc.doctype)

    if not frappe.flags.doc_events:
        return

    for row in frappe.flags.doc_events.get(doc.doctype):
        if method not in row.events:
            continue

        if method in frappe.flags.event_executed.get(doc.name, []):
            continue

        trigger_connector = False
        if not row.condition:
            trigger_connector = True

        elif frappe.safe_eval(
            row.condition, eval_locals={**EVENT_CONTEXT, "doc": doc}
        ):
            trigger_connector = True

        if not trigger_connector:
            continue

        frappe.flags.event_executed.setdefault(doc.name, []).append(method)



