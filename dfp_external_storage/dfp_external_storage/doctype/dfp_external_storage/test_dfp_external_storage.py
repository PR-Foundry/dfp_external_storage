# Copyright (c) 2023, DFP and Contributors
# See license.txt

from types import SimpleNamespace

import urllib3
from frappe.tests.utils import FrappeTestCase

import dfp_external_storage.dfp_external_storage.doctype.dfp_external_storage.dfp_external_storage as dfp_mod
from dfp_external_storage.dfp_external_storage.doctype.dfp_external_storage.dfp_external_storage import (
	DFP_S3_CONNECT_TIMEOUT,
	DFP_S3_READ_TIMEOUT,
	DFP_S3_RETRIES,
	DFPExternalStorageFile,
	MinioConnection,
	dfp_bounded_http_client,
	hook_file_after_delete,
	hook_file_before_save,
)


class TestDFPExternalStorage(FrappeTestCase):
	def test_reposting_temp_files_stay_local(self):
		"""framework#70: ERPNext stock reposting writes an internal .json.gz temp
		File (attached_to_field == "reposting_data_file") and reads it back via
		File.get_full_path(), which needs a LOCAL path. dfp must NEVER upload
		these to external storage — otherwise the url becomes "/file/..." and
		reposting fails with "Cannot access file path". The ignore guard must key
		on the field marker, regardless of the connection's doctypes_ignored list.
		"""
		# A reposting temp file → always ignored (kept local), even with no
		# connection configured on the file.
		repost_file = SimpleNamespace(
			attached_to_field="reposting_data_file",
			attached_to_doctype="Repost Item Valuation",
			dfp_external_storage_doc=None,
		)
		self.assertTrue(
			DFPExternalStorageFile.dfp_external_storage_ignored_doctypes(repost_file)
		)

		# A normal attachment with no connection is NOT force-ignored by this
		# guard (the method returns a falsy value → external storage may apply).
		normal_file = SimpleNamespace(
			attached_to_field="", attached_to_doctype="Sales Invoice", dfp_external_storage_doc=None
		)
		self.assertFalse(
			bool(DFPExternalStorageFile.dfp_external_storage_ignored_doctypes(normal_file))
		)

	def test_hooks_skip_non_dfp_files(self):
		"""framework#24: the global File doc_event hooks must early-return on Files
		that lack the DFP override methods (e.g. Frappe Drive Files, where the class
		override did not apply) instead of raising AttributeError and breaking the
		other app's File creation/deletion.

		Stand-in: a File-like object WITHOUT dfp_external_storage_upload_file /
		dfp_external_storage_delete_file. Pre-fix both hooks raised AttributeError;
		the hasattr guard makes them no-ops. Verified end-to-end on dev via
		drive.api.product.create_team (creates a base File) returning a team name
		instead of 500.
		"""
		non_dfp_file = SimpleNamespace()
		self.assertFalse(hasattr(non_dfp_file, "dfp_external_storage_upload_file"))
		self.assertIsNone(hook_file_before_save(non_dfp_file, "before_save"))
		self.assertIsNone(hook_file_after_delete(non_dfp_file, "after_delete"))

	def test_bounded_http_client_timeouts(self):
		"""framework#95 (origin Floreer-Africa/framework#126): dfp streams every
		external-storage image through the gunicorn worker, and minio's default
		urllib3 pool waits 300s with 5 retries — a stalled S3 endpoint therefore
		pins a worker far past its request timeout and a burst of cold-cache fetches
		can exhaust the pool and take a site down. dfp_bounded_http_client() must
		return a urllib3 pool whose timeouts/retries are the short, fail-fast values,
		so a stall frees the worker instead of pinning it.
		"""
		pool = dfp_bounded_http_client()

		self.assertIsInstance(pool, urllib3.PoolManager)

		timeout = pool.connection_pool_kw["timeout"]
		self.assertIsInstance(timeout, urllib3.Timeout)
		self.assertEqual(timeout.connect_timeout, DFP_S3_CONNECT_TIMEOUT)
		self.assertEqual(timeout._read, DFP_S3_READ_TIMEOUT)

		retries = pool.connection_pool_kw["retries"]
		self.assertIsInstance(retries, urllib3.Retry)
		self.assertEqual(retries.total, DFP_S3_RETRIES)

	def test_minio_connection_wires_bounded_client(self):
		"""The bounded pool is useless unless MinioConnection actually hands it to
		the minio Client, so a real deployment gets the fail-fast timeouts. Capture
		the http_client kwarg the constructor passes to Minio (no network needed) and
		assert it is a bounded pool carrying framework#95's timeouts.
		"""
		captured = {}

		def fake_minio(**kwargs):
			captured.update(kwargs)
			return SimpleNamespace(**kwargs)

		original_minio = dfp_mod.Minio
		dfp_mod.Minio = fake_minio
		try:
			MinioConnection(
				endpoint="s3.example.test",
				access_key="ak",
				secret_key="sk",
				region="us-east-1",
				secure=True,
			)
		finally:
			dfp_mod.Minio = original_minio

		http_client = captured.get("http_client")
		self.assertIsInstance(http_client, urllib3.PoolManager)
		timeout = http_client.connection_pool_kw["timeout"]
		self.assertEqual(timeout.connect_timeout, DFP_S3_CONNECT_TIMEOUT)
		self.assertEqual(timeout._read, DFP_S3_READ_TIMEOUT)
