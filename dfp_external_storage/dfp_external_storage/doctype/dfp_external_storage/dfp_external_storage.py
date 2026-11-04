
import os
import re
import io
import mimetypes
import redis.exceptions
import typing as t
from datetime import timedelta
from urllib.parse import quote
from werkzeug.wrappers import Response
from werkzeug.wsgi import wrap_file
from functools import cached_property
from minio import Minio
import urllib3
import frappe
from frappe import _
from frappe.core.doctype.file.file import File
from frappe.core.doctype.file.file import URL_PREFIXES
from frappe.model.document import Document
from frappe.utils.password import get_decrypted_password


DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX = "external_storage_public_file:"

# http://[host:port]/<file>/[File:name]/[File:file_name]
# http://myhost.localhost:8000/file/c7baa5b2ff/my-image.png
DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD = "file"

# frappe.website.path_resolver.resolve_path() strips a trailing ".html" from the
# endpoint BEFORE the renderer chain runs, so that "/about.html" serves the
# "about" template. Files on external storage are resolved by that router — local
# ones are not, frappe/app.py serves "/private/files/…" ahead of it — so an .html
# file reaches the renderer without its extension. framework#168.
DFP_FRAPPE_ROUTER_STRIPPED_SUFFIX = ".html"


DFP_EXTERNAL_STORAGE_CONNECTION_FIELDS = [
	"type", "endpoint", "secure", "bucket_name", "region", "access_key", "secret_key"]
DFP_EXTERNAL_STORAGE_CRITICAL_FIELDS = [
	"type", "endpoint", "secure", "bucket_name", "region", "access_key", "secret_key", "folders"]


class S3FileProxy:

	def __init__(self, readFn, object_size):
		self.readFn = readFn
		self.object_size = object_size
		# self.size = object_size # DEPRECATED! size is deprecated tell to Khoran, must be replaced by object_size
		self.offset = 0

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		pass

	def seek(self, offset, whence=0):
		if whence == io.SEEK_SET:
			self.offset = offset
		elif whence == io.SEEK_CUR:
			self.offset = self.offset + offset
		elif whence == io.SEEK_END:
			self.offset = self.object_size + offset

	def seekable(self):
		return True

	def tell(self):
		return self.offset

	def read(self, size=0):
		content = self.readFn(self.offset, size)
		self.offset = self.offset + len(content)
		return content


class DFPExternalStorage(Document):

	def validate(self):
		def has_changed(doc_a:Document, doc_b:Document, fields:list):
			for param in fields:
				value_a = getattr(doc_a, param)
				value_b = getattr(doc_b, param)
				if type(value_a) == list:
					if not [i.name for i in value_a] == [i.name for i in value_b]:
						return True
				elif value_a != value_b:
					return True
			return False

		if self.stream_buffer_size < 8192:
			frappe.msgprint(_("Stream buffer size must be at least of 8192 bytes."))
			self.stream_buffer_size = 8192

		# framework#240: refuse the presign-everything configuration rather than repairing
		# the one row someone remembers. On a sibling stack this outage was fixed by setting
		# "video/" on the storage and then RECURRED through a new record created without it.
		if dfp_presign_allowlist_is_unsafe(self.presigned_urls, self.presigned_mimetypes_starting):
			frappe.throw(
				_("Presigned urls are enabled but no mimetypes are listed, which presigns EVERY "
				  "file. S3 then serves each one with whatever content type it was uploaded with, "
				  "so a stylesheet can be dropped by the browser and render the site unstyled with "
				  "nothing appearing in any log. List the mimetypes to presign (for example "
				  "<code>video/</code>), or disable presigned urls."),
				title=_("Unsafe presigned url configuration"),
			)

		# Recheck S3 connection if needed
		previous = self.get_doc_before_save()
		if previous:
			if self.files_within and has_changed(self, previous, DFP_EXTERNAL_STORAGE_CRITICAL_FIELDS):
				frappe.msgprint(_("There are {} files using this bucket. The field you just updated is critical, be careful!").format(self.files_within))
		if not previous or has_changed(self, previous, DFP_EXTERNAL_STORAGE_CONNECTION_FIELDS):
			self.validate_bucket()

	def on_trash(self):
		if self.files_within:
			frappe.throw(_("Can not be deleted. There are {} files using this bucket.")
				.format(self.files_within))

	@cached_property
	def setting_stream_buffer_size(self):
		return self.stream_buffer_size if self.stream_buffer_size >= 8192 else 8192

	@cached_property
	def setting_cache_files_smaller_than(self):
		"Default: 5Mb"
		return self.cache_files_smaller_than if self.cache_files_smaller_than >= 0 else 5000000

	@cached_property
	def setting_cache_expiration_secs(self):
		"Default: 1 day"
		return self.cache_expiration_secs if self.cache_expiration_secs >= 0 else 60 * 60 * 24

	@cached_property
	def setting_presigned_url_expiration(self):
		"Default: 3 hours"
		return self.presigned_url_expiration if self.presigned_url_expiration > 0 else 60 * 60 * 3

	@cached_property
	def files_within(self):
		return frappe.db.count("File", filters={"dfp_external_storage": self.name})

	def validate_bucket(self):
		if self.client:
			self.client.validate_bucket(self.bucket_name)

	@cached_property
	def client(self):
		# Allow access_key/secret_key to be optional: if not provided in this DocType,
		# fallback to environment variables.
		if self.endpoint and self.region:
			try:
				# Resolve access key
				access_key = self.access_key or \
					os.getenv("AWS_ACCESS_KEY_ID") or \
					os.getenv("MINIO_ACCESS_KEY") or \
					os.getenv("MINIO_ROOT_USER")

				# Resolve secret key
				if self.is_new() and self.secret_key:
					key_secret = self.secret_key
				elif self.secret_key:
					key_secret = get_decrypted_password("DFP External Storage", self.name, "secret_key") if self.name else None
				else:
					key_secret = None

				secret_key = key_secret or \
					os.getenv("AWS_SECRET_ACCESS_KEY") or \
					os.getenv("MINIO_SECRET_KEY") or \
					os.getenv("MINIO_ROOT_PASSWORD")

				if access_key and secret_key:
					return MinioConnection(
						endpoint=self.endpoint,
						access_key=access_key,
						secret_key=secret_key,
						region=self.region,
						secure=self.secure,
					)
			except Exception:
				pass

	def remote_files_list(self):
		return self.client.list_objects(self.bucket_name, recursive=True)


# Bounded S3 client timeouts (framework#95, origin Floreer-Africa/framework#126). minio's
# default urllib3 pool waits 300s (connect+read) with 5 retries, so a stalled S3 endpoint
# pins a gunicorn worker far past its request timeout; a burst of cold-cache image fetches
# during an endpoint slowdown exhausted the pool and took a production site down (orders
# blocked). These short, fail-fast values free the worker instead. The read timeout is
# urllib3's inter-byte timeout, not a total-transfer cap, so large objects still stream
# fine — it only fires on a genuine stall.
DFP_S3_CONNECT_TIMEOUT = 5
DFP_S3_READ_TIMEOUT = 10
DFP_S3_RETRIES = 1


def dfp_bounded_http_client() -> urllib3.PoolManager:
	"""urllib3 pool with bounded timeouts so a stalled S3 endpoint can never pin a
	web worker (framework#95, origin Floreer-Africa/framework#126). Mirrors minio's own
	PoolManager defaults (TLS verification, pool size) but replaces the 300s timeout / 5
	retries with short, fail-fast values. Guarded by the timeout regression test in this
	repo's suite; re-verify after every upstream-sync (upstream owns
	``MinioConnection.__init__``)."""
	import certifi

	return urllib3.PoolManager(
		timeout=urllib3.Timeout(connect=DFP_S3_CONNECT_TIMEOUT, read=DFP_S3_READ_TIMEOUT),
		maxsize=10,
		cert_reqs="CERT_REQUIRED",
		ca_certs=os.environ.get("SSL_CERT_FILE") or certifi.where(),
		retries=urllib3.Retry(
			total=DFP_S3_RETRIES,
			backoff_factor=0.2,
			status_forcelist=[500, 502, 503, 504],
		),
	)


def dfp_presign_mimetype_prefixes(mimetypes_starting):
	"""Parse a newline-separated mime-prefix field into its real prefixes.

	The one parser for this field format. ``presigned_mimetypes_starting`` and
	``route_mimetypes_starting`` are both newline-separated Small Text: whitespace is
	stripped and blank lines dropped. A never-set column reads back as ``None``.

	Extracted because the same three-line comprehension had been written out at every
	call site, so a field holding only whitespace parsed to nothing in one place while
	looking configured in another. Every caller must use this, or the presign guard and
	the presign decision can disagree about whether the same field is empty.
	"""
	if not mimetypes_starting:
		return []
	return [p.strip() for p in mimetypes_starting.split("\n") if p.strip()]


def dfp_presign_allowlist_is_unsafe(presigned_urls, presigned_mimetypes_starting):
	"""Is this the presign-everything hazard? (PR-Foundry/framework#240)

	``dfp_presigned_url_get`` applies the allow-list only inside a conditional that an
	empty list makes falsy, so an enabled storage with no prefixes presigns EVERY file.
	A presigned url 301s to S3, which serves whatever ContentType the upload set
	(``application/octet-stream`` when unset); dfp's own mimetype guess is applied only
	on the streaming path, so a redirect never carries it. Browsers sniff images, so
	images survive M-bM-^@M-^T stylesheets do not, and a dropped stylesheet renders the whole
	site unstyled with nothing 404ing and no server-side log of any kind.

	Presigning switched OFF is always safe: the allow-list is then irrelevant, and an
	empty one is the normal configuration for a proxying storage.
	"""
	if not presigned_urls:
		return False
	return not dfp_presign_mimetype_prefixes(presigned_mimetypes_starting)


def dfp_mimetype_longest_prefix(mimetype, route_mimetypes_starting):
	"""Return the longest configured mime prefix that ``mimetype`` starts with.

	``route_mimetypes_starting`` is the newline-separated per-storage field (same
	format as ``presigned_mimetypes_starting``): whitespace is stripped and blank
	lines dropped. Returns the most specific (longest) matching prefix, or ``None``
	when either input is empty or nothing matches — i.e. type routing is off.
	"""
	if not mimetype or not route_mimetypes_starting:
		return None
	prefixes = dfp_presign_mimetype_prefixes(route_mimetypes_starting)
	matches = [p for p in prefixes if mimetype.startswith(p)]
	return max(matches, key=len) if matches else None


def dfp_storage_route_by_mimetype(mimetype, routes):
	"""Pick the storage a File of ``mimetype`` should route to, or ``None``.

	``routes`` is an iterable of ``(storage_name, route_mimetypes_starting)`` pairs
	for the ENABLED storages only (the caller is responsible for that filter). The
	most specific match wins (longest matching prefix); an exact-length tie is
	broken by the lowest storage name so the result is always deterministic.
	"""
	if not mimetype:
		return None
	matches = []
	for name, route_mimetypes_starting in routes:
		prefix = dfp_mimetype_longest_prefix(mimetype, route_mimetypes_starting)
		if prefix:
			matches.append((len(prefix), name))
	if not matches:
		return None
	matches.sort(key=lambda m: (-m[0], m[1]))
	return matches[0][1]


class MinioConnection:
	def __init__(self, endpoint:str, access_key:str, secret_key:str, region:str, secure:bool):
		self.client = Minio(
			endpoint=endpoint,
			access_key=access_key,
			secret_key=secret_key,
			region=region,
			secure=secure,
			http_client=dfp_bounded_http_client(),
		)

	def validate_bucket(self, bucket_name:str):
		try:
			if self.client.bucket_exists(bucket_name):
				frappe.msgprint(_("Great! Bucket is accesible ;)"), indicator="green", alert=True)
				return True
			else:
				frappe.throw(_("Bucket not found"))
		except Exception as e:
			if hasattr(e, "message"):
				frappe.throw(_("Error when looking for bucket: {}".format(e.message)))
			elif hasattr(e, "reason"):
				frappe.throw(str(e))
		return False

	def remove_object(self, bucket_name:str, object_name:str):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param version_id: Version ID of the object.
		"""
		return self.client.remove_object(bucket_name=bucket_name, object_name=object_name)

	def stat_object(self, bucket_name:str, object_name:str):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param version_id: Version ID of the object.
		"""
		return self.client.stat_object(bucket_name=bucket_name, object_name=object_name)

	def get_object(self, bucket_name:str, object_name:str, offset:int=0, length:int=0):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param offset: Start byte position of object data.
		:param length: Number of bytes of object data from offset.
		:param request_headers: Any additional headers to be added with GET request.
		:param ssec: Server-side encryption customer key.
		:param version_id: Version-ID of the object.
		:param extra_query_params: Extra query parameters for advanced usage.
		:return: :class:`urllib3.response.HTTPResponse` object.
		"""
		return self.client.get_object(bucket_name=bucket_name, object_name=object_name, offset=offset, length=length)

	def fget_object(self, bucket_name:str, object_name:str,file_path:str):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param file_path: Name of file to download
		:param request_headers: Any additional headers to be added with GET request.
		:param ssec: Server-side encryption customer key.
		:param version_id: Version-ID of the object.
		:param extra_query_params: Extra query parameters for advanced usage.
		:param temp_file_path: Path to a temporary file
		:return: :class:`urllib3.response.HTTPResponse` object.
		"""
		return self.client.fget_object(bucket_name=bucket_name, object_name=object_name,file_path=file_path)

	def presigned_get_object(self, bucket_name:str, object_name:str, expires:int=timedelta(hours=3)):
		"""
		Minio params:
		Get presigned URL of an object to download its data with expiry time
		and custom request parameters.

		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param expires: Expiry in seconds; defaults to 7 days.
		:param response_headers: Optional response_headers argument to
								specify response fields like date, size,
								type of file, data about server, etc.
		:param request_date: Optional request_date argument to
							specify a different request date. Default is
							current date.
		:param version_id: Version ID of the object.
		:param extra_query_params: Extra query parameters for advanced usage.
		:return: URL string.

		Example::
			# Get presigned URL string to download 'my-object' in
			# 'my-bucket' with default expiry (i.e. 7 days).
			url = client.presigned_get_object("my-bucket", "my-object")
			print(url)

			# Get presigned URL string to download 'my-object' in
			# 'my-bucket' with two hours expiry.
			url = client.presigned_get_object("my-bucket", "my-object", expires=timedelta(hours=2))
			print(url)
		"""
		if type(expires) == int:
			expires = timedelta(seconds=expires)
		return self.client.presigned_get_object(bucket_name=bucket_name, object_name=object_name, expires=expires)

	def put_object(self, bucket_name, object_name, data, metadata=None, length=-1):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		:param object_name: Object name in the bucket.
		:param data: An object having callable read() returning bytes object.
		:param length: Data size; -1 for unknown size and set valid part_size.
		:param content_type: Content type of the object.
		:param metadata: Any additional metadata to be uploaded along
			with your PUT request.
		:param sse: Server-side encryption.
		:param progress: A progress object;
		:param part_size: Multipart part size.
		:param num_parallel_uploads: Number of parallel uploads.
		:param tags: :class:`Tags` for the object.
		:param retention: :class:`Retention` configuration object.
		:param legal_hold: Flag to set legal hold for the object.
		"""
		return self.client.put_object(bucket_name=bucket_name,
 object_name=object_name, data=data, metadata=metadata, length=length)

	def list_objects(self, bucket_name:str, recursive=True):
		"""
		Minio params:
		:param bucket_name: Name of the bucket.
		# :param prefix: Object name starts with prefix.
		# :param recursive: List recursively than directory structure emulation.
		# :param start_after: List objects after this key name.
		# :param include_user_meta: MinIO specific flag to control to include
		# 						user metadata.
		# :param include_version: Flag to control whether include object
		# 						versions.
		# :param use_api_v1: Flag to control to use ListObjectV1 S3 API or not.
		# :param use_url_encoding_type: Flag to control whether URL encoding type
		# 							to be used or not.
		:return: Iterator of :class:`Object <Object>`.
		"""
		return self.client.list_objects(bucket_name=bucket_name, recursive=recursive)


class DFPExternalStorageFile(File):
	def __init__(self, *args, **kwargs):
		super(DFPExternalStorageFile, self).__init__(*args, **kwargs)

	@property
	def is_remote_file(self):
		return True if self.dfp_external_storage_s3_key else super(DFPExternalStorageFile, self).is_remote_file

	@cached_property
	def dfp_external_storage_doc(self):
		dfp_ext_strg_doc = None
		# 1. Use defined
		if self.dfp_external_storage:
			try:
				dfp_ext_strg_doc = frappe.get_doc("DFP External Storage", self.dfp_external_storage)
			except:
				pass
		# 2. File-type routing: enabled storage whose mime prefix matches, evaluated
		# BEFORE folder routing so e.g. images always reach their bucket even when
		# they land in Home/Attachments. No-op when no storage configures routing.
		if not dfp_ext_strg_doc:
			dfp_ext_strg_doc = self.dfp_external_storage_doc_by_mimetype
		if not dfp_ext_strg_doc:
			# 3. Specific folder connection
			dfp_ext_strg_name = frappe.db.get_value(
				"DFP External Storage by Folder",
				fieldname="parent",
				filters={ "folder": self.folder }
			)
			# 4. Default connection (Home folder)
			if not dfp_ext_strg_name:
				dfp_ext_strg_name = frappe.db.get_value(
					"DFP External Storage by Folder",
					fieldname="parent",
					filters={ "folder": "Home" }
				)
			if dfp_ext_strg_name:
				dfp_ext_strg_doc = frappe.get_doc("DFP External Storage", dfp_ext_strg_name)
		return dfp_ext_strg_doc

	@cached_property
	def dfp_external_storage_doc_by_mimetype(self):
		"""File-type routing: the ENABLED storage whose ``route_mimetypes_starting``
		prefix best matches this File's guessed mime type, or ``None``.

		Only reached when the File has no explicit connection. The query is filtered
		to enabled storages that actually declare routing prefixes, so when nobody
		configures routing it returns nothing and behaviour is identical to before
		the feature (zero-cost, fully backward compatible).
		"""
		mimetype = self.dfp_mime_type_guess_by_file_name
		if not mimetype:
			return None
		routes = frappe.get_all(
			"DFP External Storage",
			filters={"enabled": 1, "route_mimetypes_starting": ["is", "set"]},
			fields=["name", "route_mimetypes_starting"],
		)
		name = dfp_storage_route_by_mimetype(
			mimetype, [(r.name, r.route_mimetypes_starting) for r in routes]
		)
		if name:
			return frappe.get_doc("DFP External Storage", name)
		return None

	def dfp_is_s3_remote_file(self):
		if self.dfp_external_storage_s3_key and self.dfp_external_storage_doc:
			return True

	def dfp_is_cacheable(self):
		return not self.is_private and self.dfp_external_storage_doc.setting_cache_files_smaller_than and self.dfp_file_size != 0 and self.dfp_file_size < self.dfp_external_storage_doc.setting_cache_files_smaller_than

	@cached_property
	def dfp_file_size(self) -> int:
		if self.dfp_is_s3_remote_file() and self.dfp_external_storage_doc.remote_size_enabled:
			try:
				object_info = self.dfp_external_storage_doc.client.stat_object(
					bucket_name=self.dfp_external_storage_doc.bucket_name,
					object_name=self.dfp_external_storage_s3_key)
				return object_info.size
			except:
				frappe.log_error(title=f"Error getting remote file size: {self.dfp_external_storage_s3_key}")
		return self.file_size

	@cached_property
	def dfp_external_storage_client(self):
		if self.dfp_external_storage_doc:
			return self.dfp_external_storage_doc.client

	def dfp_external_storage_ignored_doctypes(self):
		"Do not apply for files attached to specified doctypes"
		# framework#70: ERPNext stock reposting writes an internal .json.gz temp
		# file (a File with attached_to_field="reposting_data_file", on "Repost
		# Item Valuation") and reads it back via File.get_full_path(), which
		# requires a LOCAL filesystem path. Uploading it to external storage
		# rewrites file_url to "/file/<name>/..." and breaks reposting with
		# "Cannot access file path". These temp files must NEVER leave local disk,
		# regardless of the connection's configurable doctypes_ignored list, so
		# guard on ERPNext's own marker field (it exists only on Repost Item
		# Valuation).
		if getattr(self, "attached_to_field", None) == "reposting_data_file":
			return True
		if self.attached_to_doctype and self.dfp_external_storage_doc and self.attached_to_doctype in [i.doctype_to_ignore for i in self.dfp_external_storage_doc.doctypes_ignored]:
			frappe.msgprint(_("""This doctype does not allow remote files attached to it. Check "DFP External Storage" advanced settings for more details."""))
			return True

	def dfp_external_storage_upload_file(self, local_file=None):
		"""
		Critical fields: "dfp_external_storage_s3_key", "dfp_external_storage" and "file_url"
		:param local_file: if given, file path for reading the content. If not given, the content field of this File is used
		"""
		if self.dfp_external_storage_ignored_doctypes():
			self.dfp_external_storage = ""
			return False
		if not self.dfp_external_storage_doc or not self.dfp_external_storage_doc.enabled:
			return False
		if self.is_folder:
			return False
		if self.dfp_external_storage_s3_key:
			# File already on S3
			return False
		if self.file_url and self.file_url.startswith(URL_PREFIXES):
			# frappe.throw(_("Not implemented save http(s)://file(s) to local."))
			raise NotImplementedError("http(s)://file(s) not ready to be saved to local or external storage(s).")

		original_file_url = self.file_url

		# Define S3 key
		# key = f"{frappe.local.site}/{self.file_name}" # << Before 2024.03.03
		base, extension = os.path.splitext(self.file_name)
		key = f"{frappe.local.site}/{base}-{self.name}{extension}"

		is_public = "/public" if not self.is_private else ""
		if not local_file:
			local_file = "./" + frappe.local.site + is_public + self.file_url

		try:
			if not os.path.exists(local_file):
				frappe.throw(_("Local file not found"))
			with open(local_file, "rb") as f:
				self.dfp_external_storage_client.put_object(
					bucket_name=self.dfp_external_storage_doc.bucket_name,
					object_name=key,
					data=f,
					length=os.path.getsize(local_file),
					# Meta removed because same s3 file can be used within different File docs
					# metadata={"frappe_file_id": self.name}
				)

			self.dfp_external_storage_s3_key = key
			self.dfp_external_storage = self.dfp_external_storage_doc.name
			self.file_url = f"/{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}/{self.name}/{self.file_name}"
			os.remove(local_file)
		except Exception as e:
			error_msg = _("Error saving file in remote folder: {}").format(str(e))
			frappe.log_error(f"{error_msg}: {self.file_name}", message=e)
			# If file is new we upload to local filesystem
			if not self.get_doc_before_save():
				error_extra = _("File saved in local filesystem.")
				frappe.log_error(f"{error_msg} {error_extra}: {self.file_name}")
				self.dfp_external_storage_s3_key = ""
				self.dfp_external_storage = ""
				self.file_url = original_file_url
			# If modifing existent file throw error
			else:
				frappe.throw(error_msg)

	def dfp_external_storage_delete_file(self):
		if not self.dfp_is_s3_remote_file():
			return
		# Only delete if connection is enabled
		if not self.dfp_external_storage_doc or not self.dfp_external_storage_doc.enabled:
			error_msg = _("Error deleting file in remote folder.")
			error_extra = _("Write disabled for connection <strong>{}</strong>").format(
				self.dfp_external_storage_doc.title)
			frappe.throw(f"{error_msg} {error_extra}")
		# After commit, and only while no File row on this bucket still uses the key
		# (PR-Foundry/framework#338 review): a delete that rolls back keeps its object.
		dfp_remove_object_after_commit(
			self.dfp_external_storage_doc, self.dfp_external_storage_client, self.dfp_external_storage_s3_key
		)

	def dfp_external_storage_download_to_file(self, local_file):
		"""
		Stream file from S3 directly to local_file. This avoids reading the whole file into memory at any point
		:param local_file: path to a local file to stream content to
		"""
		if not self.dfp_is_s3_remote_file():
			# frappe.msgprint(_("S3 key not found: ") + self.file_name,
			# 	indicator="red", title=_("Error processing File"), alert=True)
			return
		try:
			key = self.dfp_external_storage_s3_key

			self.dfp_external_storage_client.fget_object(
				bucket_name=self.dfp_external_storage_doc.bucket_name,
				object_name=key,
				file_path=local_file)
		except Exception as e:
			error_msg = _("Error downloading to file from remote folder. Check Error Log for more information.")
			frappe.log_error(title=f"{error_msg}: {self.file_name}", message=e)
			frappe.throw(error_msg)

	def dfp_external_storage_file_proxy(self):
		"""
		Get a read-only context manager file-like object that will read requested bytes directly from S3. This allows you to avoid downloading the whole file when only parts or chunks of it will be read from.
		"""
		if not self.dfp_is_s3_remote_file():
			return

		def read_chunks(offset=0, size=0):
			with self.dfp_external_storage_client.get_object(
				bucket_name=self.dfp_external_storage_doc.bucket_name,
				object_name=self.dfp_external_storage_s3_key,
				offset=offset,
				length=size) as response:
				content = response.read()
			return content

		return S3FileProxy(readFn=read_chunks, object_size=self.dfp_file_size)

	def dfp_external_storage_download_file(self) -> bytes:
		content = b""
		if not self.dfp_is_s3_remote_file():
			return content
		try:
			with self.dfp_external_storage_client.get_object(
				bucket_name=self.dfp_external_storage_doc.bucket_name,
				object_name=self.dfp_external_storage_s3_key) as response:
				content = response.read()
			return content
		except:
			error_msg = _("Error downloading file from remote folder")
			frappe.log_error(title=f"{error_msg}: {self.file_name}")
			frappe.throw(error_msg)
		return content

	def dfp_external_storage_stream_file(self) -> t.Iterable[bytes]:
		return wrap_file(environ=frappe.local.request.environ,
			file=self.dfp_external_storage_file_proxy(),
				buffer_size=self.dfp_external_storage_doc.setting_stream_buffer_size)

	def download_to_local_and_remove_remote(self):
		try:
			storage_doc = self.dfp_external_storage_doc
			client = self.dfp_external_storage_client
			bucket = storage_doc.bucket_name
			key = self.dfp_external_storage_s3_key

			self.dfp_external_storage_s3_key = ""
			self.dfp_external_storage = ""

			with self.dfp_external_storage_client.get_object(bucket_name=bucket, object_name=key) as response:
				self._content = response.read()
			self.save_file_on_filesystem()

			# Not inline: a save that fails later, or a sibling row on the same key, would
			# lose the object (PR-Foundry/framework#338 review).
			dfp_remove_object_after_commit(storage_doc, client, key)
		except:
			error_msg = _("Error downloading and removing file from remote folder.")
			frappe.log_error(title=f"{error_msg}: {self.file_name}")
			frappe.throw(error_msg)

	def validate_file_on_disk(self):
		return True if self.dfp_is_s3_remote_file() else super(DFPExternalStorageFile, self).validate_file_on_disk()

	def exists_on_disk(self):
		return False if self.dfp_is_s3_remote_file() else super(DFPExternalStorageFile, self).exists_on_disk()

	@frappe.whitelist()
	def optimize_file(self):
		if self.dfp_is_s3_remote_file():
			raise NotImplementedError("Only local image files can be optimized")
		super(DFPExternalStorageFile, self).optimize_file()

	def _remote_file_local_path_get(self):
		return f"/{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}/{self.name}/{self.file_name}"

	def dfp_file_url_is_s3_location_check_if_s3_data_is_not_defined(self):
		"""
		Set `dfp_external_storage_s3_key` if `file_url` exists and can be rendered.
		Sometimes, when a file is copied (for example, when amending a sales invoice), we have the `file_url` but not the `key` (refer to the method `copy_attachments_from_amended_from` in `document.py`).
		"""
		if not self.file_url or self.is_remote_file or self.dfp_external_storage_s3_key:
			return
		try:
			dfp_es_file_renderer = DFPExternalStorageFileRenderer(path=self.file_url)
			if not dfp_es_file_renderer.can_render():
				return
			s3_data = frappe.get_value("File", dfp_es_file_renderer.file_id_get(), fieldname="*")
			if s3_data:
				self.dfp_external_storage = s3_data["dfp_external_storage"]
				self.dfp_external_storage_s3_key = s3_data["dfp_external_storage_s3_key"]
				self.content_hash = s3_data["content_hash"]
				self.file_size = s3_data["file_size"]
				# It is "duplicated" within Frappe but not in S3 😏
				self.flags.ignore_duplicate_entry_error = True
		except:
			pass

	def get_content(self, encodings=None) -> bytes:
		self.dfp_file_url_is_s3_location_check_if_s3_data_is_not_defined()
		if not self.dfp_is_s3_remote_file():
			return super(DFPExternalStorageFile, self).get_content(encodings=encodings)
		try:
			if not self.is_downloadable():
				raise Exception("File not available")
			return self.dfp_external_storage_download_file()
		except Exception:
			# If no document, no read permissions, etc. For security reasons do not give any information, so just raise a 404 error
			raise frappe.PageDoesNotExistError()

	@cached_property
	def dfp_mime_type_guess_by_file_name(self):
		content_type, _= mimetypes.guess_type(self.file_name)
		if content_type:
			return content_type

	def dfp_presigned_url_get(self):
		if not self.dfp_is_s3_remote_file() or not self.dfp_external_storage_doc.presigned_urls:
			return
		# Gate on the RAW field, NOT on the parsed list. A whitespace-only value is truthy
		# here but parses to no prefixes, so `any(...)` over an empty list refuses the
		# presign — gating on the parsed list instead would flip that one case from
		# "presign nothing" to "presign everything", which is the hazard framework#240
		# exists to remove. Behaviour here is deliberately unchanged; the unsafe
		# configuration is refused at save time by DFPExternalStorage.validate() instead.
		# Pinned by client_app.tests.test_dfp_presign_allowlist.
		if self.dfp_external_storage_doc.presigned_mimetypes_starting and self.dfp_mime_type_guess_by_file_name:
			presigned_mimetypes_starting = dfp_presign_mimetype_prefixes(
				self.dfp_external_storage_doc.presigned_mimetypes_starting)
			if not any(self.dfp_mime_type_guess_by_file_name.startswith(i) for i in presigned_mimetypes_starting):
				return
		return self.dfp_external_storage_client.presigned_get_object(bucket_name=self.dfp_external_storage_doc.bucket_name, object_name=self.dfp_external_storage_s3_key, expires=self.dfp_external_storage_doc.setting_presigned_url_expiration)


def dfp_remove_object_after_commit(storage_doc, client, key):
	"""Remove a File's object from its bucket once the transaction commits (Floreer-Africa/framework#286).

	Every path that drops an object (a move to another storage, a move back to local
	disk, a delete) goes through here. Removing it inline deleted the object before
	the transaction committed, so a save or delete that failed later left a row
	pointing at a key that no longer existed. It also ignored other File rows on the
	same bucket and key: choosing an existing library image creates exactly such a
	copy, and every copy's image broke.

	Siblings are matched by bucket, not by storage name: two storages can point at one
	bucket (folder routing makes that natural), and an object moved between them is the
	same object (PR-Foundry/framework#338 review). Checked at commit time, so a sibling
	repointed in the same transaction does not keep the object alive.

	Args:
	    storage_doc: the storage the object is in, read before the File was repointed.
	    client: that storage's S3 client.
	    key: the object's S3 key.
	"""
	if not key or not storage_doc:
		return
	bucket = storage_doc.bucket_name
	endpoint = storage_doc.endpoint

	def remove():
		same_bucket = frappe.get_all(
			"DFP External Storage", filters={"bucket_name": bucket, "endpoint": endpoint}, pluck="name"
		) or [storage_doc.name]
		if frappe.db.exists(
			"File", {"dfp_external_storage": ["in", same_bucket], "dfp_external_storage_s3_key": key}
		):
			return
		try:
			client.remove_object(bucket_name=bucket, object_name=key)
		except Exception:
			frappe.log_error(title=f"Error removing file from remote: {key}")

	frappe.db.after_commit.add(remove)


def hook_file_before_save(doc, method):
	"""
	This method is called before the document is saved to DB (insert or update row)
	Critical fields: dfp_external_storage_s3_key, dfp_external_storage and file_url
	"""
	# framework#24: DFP File hooks must skip Files that are not DFP-managed (e.g.
	# Frappe Drive Files, where the override class did not apply) instead of
	# raising AttributeError and breaking the other app File creation.
	if not hasattr(doc, "dfp_external_storage_upload_file"):
		return
	previous = doc.get_doc_before_save()

	if not previous:
		# NEW "File": Case 1: remote selected => upload to remote and continue "File" flow
		doc.dfp_external_storage_upload_file()
		return

	# MODIFY "File"

	# MODIFY "File": Case 1: Existent local file + new storage selected => upload to remote
	if not doc.dfp_external_storage_s3_key and doc.dfp_external_storage and not previous.dfp_external_storage:
		doc.dfp_external_storage_upload_file()

	# MODIFY "File": Case 2: Existent remote file + no storage selected => download to local + remove from remote
	elif previous.dfp_external_storage and not doc.dfp_external_storage:
		previous.download_to_local_and_remove_remote()
		doc.file_url = previous.file_url # << contains local file path downloaded
		doc.dfp_external_storage_s3_key = ""

	# MODIFY "File": Case 3: Existent remote file + new remote selected => stream from old to new remote + delete from old remote
	elif previous.dfp_external_storage and doc.dfp_external_storage and previous.dfp_external_storage != doc.dfp_external_storage:
		try:
			# MODIFY "File": Case 3.1.: new remote + not allowed doctype => download to local + remove old remote
			# TODO: Maybe we should left in old remote?? and we should check if old remote allows the doctype??
			if doc.dfp_external_storage_ignored_doctypes():
				previous.download_to_local_and_remove_remote()
				doc.file_url = previous.file_url # << contains local file path downloaded
				doc.dfp_external_storage_s3_key = ""
				doc.dfp_external_storage = ""
			# MODIFY "File": Case 3.2.: new remote + allowed doctype => stream from old to new remote + delete from old remote
			else:
				# Get file from previous remote in chunks of 10MB (not loading it fully in memory)
				with previous.dfp_external_storage_file_proxy() as response:
					doc.dfp_external_storage_client.put_object(
						bucket_name=doc.dfp_external_storage_doc.bucket_name,
						object_name=doc.dfp_external_storage_s3_key,
						data=response,
						length=response.object_size,
						# Meta removed because same s3 file can be used within different File docs
						# metadata={"frappe_file_id": self.name}
					)
				# New s3 key => update "file_url"
				doc.file_url = doc._remote_file_local_path_get()
				dfp_remove_object_after_commit(
					previous.dfp_external_storage_doc,
					previous.dfp_external_storage_client,
					previous.dfp_external_storage_s3_key,
				)
		except:
			error_msg = _("Error putting file from one remote to another.")
			frappe.log_error(f"{error_msg}: {doc.file_name}")
			frappe.throw(error_msg)

	# Clean cache when updating "File"
	if doc.dfp_external_storage_s3_key:
		dfp_forget_cached_file(doc.name)


def dfp_forget_cached_file(name:str):
	"""Drop dfp's cached copy of File `name`, now and again once the transaction commits.

	The cache holds a public file's whole response and is served with no permission check, so
	it must go when the file is made private or deleted (PR-Foundry/framework#332 review).
	Clearing only now leaves a gap: a request between this and the commit still reads the old
	row and re-caches the old public copy. The second clear, after the commit, closes it.
	"""
	cache_key = f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}{name}"

	def forget():
		# Bump the generation first: a request that loaded the file before this point then
		# finds it moved and does not write its (possibly stale) response back.
		dfp_cache_generation_bump(name)
		frappe.cache().delete_value(cache_key)

	forget()
	frappe.db.after_commit.add(forget)


def dfp_cache_generation_key(name:str) -> str:
	"""Redis key counting how often File `name` was forgotten (PR-Foundry/framework#332 re-review).

	A request that loaded the old public row before a commit could otherwise write its response
	into the cache after the after-commit clear, and that stale copy would then be served with no
	permission check until it expired. file() reads the generation before loading the file and
	writes the cache only if it is unchanged.
	"""
	return f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}generation:{name}"


# Raw Redis on the site-prefixed key: frappe's get_value pickles and keeps a request-local copy,
# so a second read in the same request would always agree with the first. Unlike get_value, raw
# calls do not swallow a lost Redis, so both helpers do: an unreachable cache must not 500 every
# /file/ request or fail File saves (framework#332 review).
def dfp_cache_generation(name:str) -> int | None:
	"""The file's generation, or None when Redis cannot be read (then nothing is cached)."""
	cache = frappe.cache()
	try:
		return int(cache.get(cache.make_key(dfp_cache_generation_key(name))) or 0)
	except redis.exceptions.ConnectionError:
		return None


def dfp_cache_generation_bump(name:str):
	cache = frappe.cache()
	key = cache.make_key(dfp_cache_generation_key(name))
	try:
		cache.incr(key)
		cache.expire(key, 60 * 60 * 24)  # outlives any request that could still be loading the file
	except redis.exceptions.ConnectionError:
		# No Redis, no cached copy to protect: the delete that follows is a no-op too.
		pass


def hook_file_on_update(doc, method):
	"""DEPRECATED! Remove method after 2025.01.01 ("/dfp_external_storage/dfp_external_storage/hooks.py" too)"""
	pass


def hook_file_after_delete(doc, method):
	"Called after a document is deleted"
	if not hasattr(doc, "dfp_external_storage_delete_file"):
		return
	# A deleted public file must not stay servable from the cache (framework#332 review).
	if doc.get("dfp_external_storage_s3_key"):
		dfp_forget_cached_file(doc.name)
	doc.dfp_external_storage_delete_file()


def dfp_file_name_matches_request(stored_file_name:str, requested:str) -> bool:
	"""True when `requested` names `stored_file_name`, allowing for the router.

	The file is resolved by docname, which is authoritative; comparing the name
	is a consistency guard. This re-attaches the ONE suffix frappe's router
	removes (framework#168) and nothing else, so a bare "report" still cannot
	name "report.pdf".
	"""
	if not stored_file_name or not requested:
		return False
	if stored_file_name == requested:
		return True
	return stored_file_name == f"{requested}{DFP_FRAPPE_ROUTER_STRIPPED_SUFFIX}"


DFP_PUBLIC_EDGE_MAX_AGE = 60 * 60


def dfp_public_cache_control(max_age:int) -> str:
	"""The Cache-Control a PUBLIC file is served with (Floreer-Africa/framework#283, PR-Foundry/framework#332).

	frappe's `process_response` stamps every response `no-store` and flushes the
	session cookies onto it unless the endpoint opts in with a "public"
	Cache-Control, so without this no browser or CDN could cache a product image
	and every page view re-fetched every image through the backend workers. A
	private file must never get it: it is permission-checked per request, and a
	shared cache would hand it to the next caller.

	The max-age is capped at DFP_PUBLIC_EDGE_MAX_AGE: making a file private or
	deleting it clears only dfp's Redis copy, so whatever a CDN or browser holds is
	served until it expires. A day-long edge cache would keep a mistakenly public
	upload reachable for a day after it was withdrawn.
	"""
	return f"public, max-age={min(int(max_age), DFP_PUBLIC_EDGE_MAX_AGE)}"


def dfp_force_download_header(file_name:str):
	"""Frappe's own force-download header for `file_name`, or None.

	Mirrors what `frappe.utils.response.send_private_file` does for a local
	private file: HTML/SVG/XML served INLINE from the site origin executes with
	the site's cookies, so frappe never serves those inline. External storage
	must not be the weaker path. The extension list is IMPORTED rather than
	re-listed — a second copy drifts from the one frappe actually enforces —
	and a frappe without the list forces nothing, so there is nothing to mirror.
	"""
	try:
		from frappe.utils.response import FORCE_DOWNLOAD_EXTENSIONS
	except ImportError:
		return None
	if os.path.splitext(file_name or "")[1].lower() not in FORCE_DOWNLOAD_EXTENSIONS:
		return None
	return ("Content-Disposition", f"attachment; filename*=UTF-8''{quote(file_name)}")


class DFPExternalStorageFileRenderer:
	def __init__(self, path, status_code=None):
		self.path = path
		self.status_code = status_code
		self._regex = None

	def _regexed_path(self):
		# Anchored on the url segment, and WITHOUT an extension requirement:
		# frappe's router strips a trailing ".html" from the endpoint before any
		# renderer sees it, so requiring an extension made every .html file
		# unservable — the path fell through to frappe's PrintPage, which claims
		# it because "file" matches the File DocType case-insensitively, and died
		# with "ImportError: file" (framework#168). Both groups are single path
		# segments: a file url is always "/file/<name>/<file_name>".
		self._regex = re.search(
			rf"^\/?{DFP_EXTERNAL_STORAGE_URL_SEGMENT_FOR_FILE_LOAD}\/([^\/]+)\/([^\/]+)$", self.path)

	def file_id_get(self):
		if self.can_render():
			return self._regex[1]

	def can_render(self):
		if not self._regex:
			self._regexed_path()
		if self._regex:
			return True

	def render(self):
		file_id = self._regex[1]
		file_name = self._regex[2] if len(self._regex.regs) == 3 else ""
		return file(name=file_id, file=file_name)


def file(name:str, file:str):
	if not name or not file:
		raise frappe.PageDoesNotExistError()

	cache_key = f"{DFP_EXTERNAL_STORAGE_PUBLIC_CACHE_PREFIX}{name}"

	generation = dfp_cache_generation(name)
	response_values = frappe.cache().get_value(cache_key)
	# A copy written by a request that loaded the file before it was forgotten carries the old
	# generation: the check before the write and the write itself are two steps, so a forget can
	# land between them. Treat such a copy as a miss (framework#332 review).
	if response_values and (generation is None or response_values.get("generation", 0) != generation):
		response_values = None
	cache_hit = bool(response_values)
	if cache_hit and not any(h[0] == "Cache-Control" for h in response_values.get("headers", [])):
		# Only public files are ever cached here; an entry written before
		# Floreer-Africa/framework#283 lacks the header until it expires.
		response_values["headers"].append(("Cache-Control", dfp_public_cache_control(60 * 60 * 24)))
	if not response_values:
		try:
			doc = frappe.get_doc("File", name)
		except frappe.DoesNotExistError:
			raise frappe.PageDoesNotExistError()

		if not dfp_file_name_matches_request(doc.file_name, file):
			raise frappe.PageDoesNotExistError()

		if not doc.is_downloadable():
			raise frappe.PermissionError()

		response_values = {}
		response_values["headers"] = []

		try:
			presigned_url = doc.dfp_presigned_url_get()
			if presigned_url:
				frappe.flags.redirect_location = presigned_url
				raise frappe.Redirect
			# Do not stream file if cacheable or smaller than stream buffer chunks size
			if doc.dfp_is_cacheable() or doc.dfp_file_size < doc.dfp_external_storage_doc.setting_stream_buffer_size:
				response_values["response"] = doc.dfp_external_storage_download_file()
			else:
				response_values["response"] = doc.dfp_external_storage_stream_file()
				response_values["headers"].append(("Content-Length", doc.dfp_file_size))
		except frappe.Redirect:
			raise
		except:
			frappe.log_error(f"Error obtaining remote file content: {name}/{file}")

		if "response" not in response_values or not response_values["response"]:
			raise frappe.PageDoesNotExistError()

		if doc.dfp_mime_type_guess_by_file_name:
			response_values["mimetype"] = doc.dfp_mime_type_guess_by_file_name
		# Set before the cache write below, so a cached response carries it too.
		download_header = dfp_force_download_header(doc.file_name)
		if download_header:
			response_values["headers"].append(download_header)
		if not doc.is_private:
			response_values["headers"].append(("Cache-Control", dfp_public_cache_control(
				doc.dfp_external_storage_doc.setting_cache_expiration_secs)))
		response_values["status"] = 200

		# Not if the file was forgotten while this request loaded it (framework#332 re-review).
		if doc.dfp_is_cacheable() and generation is not None and dfp_cache_generation(name) == generation:
			frappe.cache().set_value(key=cache_key,
				val={**response_values, "generation": generation},
				expires_in_sec=doc.dfp_external_storage_doc.setting_cache_expiration_secs)

	if "status" in response_values and response_values["status"] == 200:
		response_values.pop("generation", None)
		return Response(**response_values)

	raise frappe.PageDoesNotExistError()
