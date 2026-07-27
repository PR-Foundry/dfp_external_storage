
frappe.ui.form.on('DFP External Storage', {

	setup: frm => {
		frm.button_remote_files_list = null
	},

	refresh: function(frm) {
		if (frm.is_new() && !frm.doc.doctypes_ignored.length) {
			frm.doc.doctypes_ignored.push({doctype_to_ignore: 'Data Import'})
			frm.doc.doctypes_ignored.push({doctype_to_ignore: 'Prepared Report'})
			frm.refresh_field('doctypes_ignored')
		}

		if (frm.doc.enabled) {
			frm.button_remote_files_list = frm.add_custom_button(
				__('List files in bucket'),
				() => frappe.set_route('dfp-s3-bucket-list', frm.doc.name)
				// () => frappe.set_route('dfp-s3-bucket-list', { storage: frm.doc.name })
			)
		}

		frm.set_query('folders', function() {
			return {
				filters: {
					is_folder: 1,
				},
			}
		})

		// Exclude only folders already owned by a DIFFERENT storage, so the
		// "one folder -> one storage" invariant holds without hiding unassigned
		// folders or this storage's own already-assigned folders. `parent` is the
		// owning storage's docname; `folder` is the assigned File folder. (The
		// previous code compared the CHILD row `name` to the PARENT docname — they
		// never match, so it wrongly excluded every assigned folder.)
		frappe.db.get_list(
			'DFP External Storage by Folder',
			{fields: ['parent', 'folder'], parent: 'DFP External Storage'}
		).then(data => {
			let folders_owned_by_other_storages = (data || [])
				.filter(d => d.parent != frm.doc.name)
				.map(d => d.folder)
			frm.set_query('folders', function () {
				return {
					filters: {
						is_folder: 1,
						name: ['not in', folders_owned_by_other_storages],
					},
				}
			})
		})

	},

})
