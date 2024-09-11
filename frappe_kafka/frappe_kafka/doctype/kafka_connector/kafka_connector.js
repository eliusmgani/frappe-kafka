// Copyright (c) 2024, eliusmgani and contributors
// For license information, please see license.txt

frappe.ui.form.on('Kafka Connector', {
	refresh: function(frm) {

	},
	select_fields: function(frm) {
		frm.events.field_selector(frm);
	},
	field_selector: (frm) => {
		let document_fields = [];
		if (frm.doc.selected_fields)
			document_fields = (JSON.parse(frm.doc.selected_fields)).map(f => f.fieldname);

		frm.call({
			method: 'get_source_fields',
			doc: frm.doc,
			args: {
				fields: document_fields
			},
			freeze: true,
			freeze_message: __('Fetching fields...'),
			callback: (r) => {
				if (r.message) {
					let doc_fields = r.message;
					frm.events.show_field_selector_dialog(frm, doc_fields);
				}
			}
		});
	},
	show_field_selector_dialog: (frm, doc_fields) => {
		let d = new frappe.ui.Dialog({
			title: __('{0} Fields', [__(frm.doc.data_source)]),
			fields: [
				{
					label: __('Select Fields'),
					fieldtype: 'MultiCheck',
					fieldname: 'fields',
					options: doc_fields,
					columns: 4
				}
			]
		});

		d.$body.prepend(`
			<div class="columns-search">
				<input type="text" placeholder="${__('Search')}" data-element="search" class="form-control input-xs">
			</div>`
		);

		frappe.utils.setup_search(d.$body, '.unit-checkbox', '.label-area');

		d.set_primary_action(__('Save'), () => {
			let values = d.get_values().fields;

			let selected_fields = [];

			frappe.model.with_doctype(frm.doc.data_source, function() {
				for (let idx in values) {
					let value = values[idx];

					let field = frappe.get_meta(frm.doc.data_source).fields.filter((df) => df.fieldname == value)[0];
					console.log(field);
					if (field) {
						selected_fields.push({
							label: field.label,
							fieldname: field.fieldname,
							fieldtype: field.fieldtype
						});
					}
				}

				d.refresh();
				frappe.model.set_value(frm.doc.doctype, frm.doc.name, 'selected_fields', JSON.stringify(selected_fields));
			});

			d.hide();
		});

		d.$wrapper.find('.modal-content').css('width', '900px')

		d.show();
	}
});
