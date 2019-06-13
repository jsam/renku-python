# -*- coding: utf-8 -*-
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Zenodo API integration."""
import json
import os
import pathlib
import urllib
from urllib.parse import urlparse

import attr
import requests
from requests import HTTPError
from tqdm import tqdm

from renku._compat import Path
from renku.cli._providers.api import ProviderApi
from renku.cli._providers.doi import DOIProvider
from renku.models.datasets import Dataset, DatasetFile
from renku.utils.doi import is_doi

ZENODO_BASE_URL = 'https://zenodo.org'
ZENODO_BASE_PATH = 'api'


def make_records_url(record_id):
    """Create URL to access record by ID."""
    return urllib.parse.urljoin(
        ZENODO_BASE_URL,
        pathlib.posixpath.join(ZENODO_BASE_PATH, 'records', record_id)
    )


@attr.s
class ZenodoFileSerializer:
    """Zenodo record file."""

    id = attr.ib(default=None, kw_only=True)

    checksum = attr.ib(default=None, kw_only=True)

    links = attr.ib(default=None, kw_only=True)

    filename = attr.ib(default=None, kw_only=True)

    filesize = attr.ib(default=None, kw_only=True)

    @property
    def remote_url(self):
        """Get remote URL as ``urllib.ParseResult``."""
        return urllib.parse.urlparse(self.links['download'])

    @property
    def type(self):
        """Get file type."""
        return self.filename.split('.')[-1]


@attr.s
class ZenodoMetadataSerializer:
    """Zenodo metadata."""

    access_right = attr.ib(default=None, kw_only=True)

    communities = attr.ib(default=None, kw_only=True)

    contributors = attr.ib(default=None, kw_only=True)

    creators = attr.ib(default=None, kw_only=True)

    description = attr.ib(default=None, kw_only=True)

    doi = attr.ib(default=None, kw_only=True)

    grants = attr.ib(default=None, kw_only=True)

    image_type = attr.ib(default=None, kw_only=True)

    journal_issue = attr.ib(default=None, kw_only=True)

    journal_pages = attr.ib(default=None, kw_only=True)

    journal_title = attr.ib(default=None, kw_only=True)

    journal_volume = attr.ib(default=None, kw_only=True)

    keywords = attr.ib(default=None, kw_only=True)

    language = attr.ib(default=None, kw_only=True)

    license = attr.ib(default=None, kw_only=True)

    notes = attr.ib(default=None, kw_only=True)

    prereserve_doi = attr.ib(default=None, kw_only=True)

    publication_date = attr.ib(default=None, kw_only=True)

    publication_type = attr.ib(default=None, kw_only=True)

    references = attr.ib(default=None, kw_only=True)

    related_identifiers = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, kw_only=True)

    upload_type = attr.ib(default=None, kw_only=True)

    version = attr.ib(default=None, kw_only=True)


def _metadata_converter(data):
    """Convert dict to ZenodoMetadata instance."""
    return ZenodoMetadataSerializer(**data)


@attr.s
class ZenodoRecordSerializer:
    """Zenodo record."""

    _jsonld = attr.ib(default=None, init=False)

    id = attr.ib(default=None, kw_only=True)

    doi = attr.ib(default=None, kw_only=True)

    doi_url = attr.ib(default=None, kw_only=True)

    title = attr.ib(default=None, kw_only=True)

    files = attr.ib(default=None, kw_only=True)

    links = attr.ib(default=None, kw_only=True)

    metadata = attr.ib(
        default=None,
        kw_only=True,
        type=ZenodoMetadataSerializer,
        converter=_metadata_converter
    )

    modified = attr.ib(default=None, kw_only=True)

    owner = attr.ib(default=None, kw_only=True)

    record_id = attr.ib(default=None, kw_only=True)

    state = attr.ib(default=None, kw_only=True)

    submitted = attr.ib(default=None, kw_only=True)

    created = attr.ib(default=None, kw_only=True)

    conceptrecid = attr.ib(default=None, kw_only=True)

    conceptdoi = attr.ib(default=None, kw_only=True)

    _zenodo = attr.ib(default=None, kw_only=True)

    _uri = attr.ib(default=None, kw_only=True)

    @property
    def version(self):
        """Get record version."""
        return self.metadata.version

    def is_last_version(self, uri):
        """Check if record is at last possible version."""
        if is_doi(uri):
            return uri == self.metadata.prereserve_doi['doi']

        record_id = self.metadata.prereserve_doi['recid']
        return ZenodoProvider.record_id(uri) == record_id

    def get_jsonld(self):
        """Get record metadata as jsonld."""
        response = self._zenodo.accept_jsonld().make_request(self._uri)
        self._jsonld = response.json()
        return self._jsonld

    def get_files(self):
        """Get Zenodo files metadata as ``ZenodoFile``."""
        if len(self.files) == 0:
            raise LookupError('no files have been found')

        return [ZenodoFileSerializer(**file_) for file_ in self.files]

    def as_dataset(self):
        """Deserialize `ZenodoRecordSerializer` to `Dataset`."""
        files = self.get_files()
        metadata = self.get_jsonld()
        dataset = Dataset.from_jsonld(metadata)

        serialized_files = []
        for file_ in files:
            remote_ = file_.remote_url
            dataset_file = DatasetFile(
                url=remote_,
                id=file_.id,
                checksum=file_.checksum,
                filename=file_.filename,
                filesize=file_.filesize,
                filetype=file_.type,
                dataset=dataset.name,
                path='',
            )
            serialized_files.append(dataset_file)

        dataset.files = serialized_files

        if isinstance(dataset.url, dict) and '_id' in dataset.url:
            dataset.url = urllib.parse.urlparse(dataset.url.pop('_id'))
            dataset.url = dataset.url.geturl()

        return dataset


@attr.s
class ZenodoExporter:
    """Zenodo export manager."""

    ZENODO_BASE_URL = 'https://zenodo.org/'
    ZENODO_SANDBOX_URL = 'https://sandbox.zenodo.org/'

    API_PATH = 'api/'
    DEPOSIT_PATH = 'deposit/'
    PUBLISH_PATH = 'record/'

    PUBLISH_ACTION_PATH = 'depositions/{0}/actions/publish'
    METADATA_URL = 'depositions/{0}'
    FILES_URL = 'depositions/{0}/files'
    NEW_DEPOSIT_URL = 'depositions'

    HEADERS = {'Content-Type': 'application/json'}

    dataset = attr.ib()
    secret = attr.ib()

    @property
    def zenodo_url(self):
        """Returns correct Zenodo URL based on environment."""
        if 'ZENODO_USE_SANDBOX' in os.environ:
            return self.ZENODO_SANDBOX_URL

        return self.ZENODO_BASE_URL

    def publish_url(self, deposition_id):
        """Returns publish URL."""
        return urllib.parse.urlparse((
            self.zenodo_url + self.API_PATH + self.DEPOSIT_PATH +
            self.PUBLISH_ACTION_PATH
        ).format(deposition_id))

    def attach_metadata_url(self, deposition_id):
        """Return URL for attaching metadata."""
        return urllib.parse.urlparse((
            self.zenodo_url + self.API_PATH + self.DEPOSIT_PATH +
            self.METADATA_URL
        ).format(deposition_id))

    def upload_file_url(self, deposition_id):
        """Return URL for uploading file."""
        return urllib.parse.urlparse((
            self.zenodo_url + self.API_PATH + self.DEPOSIT_PATH +
            self.FILES_URL
        ).format(deposition_id))

    def new_deposit_url(self):
        """Return URL for creating new deposit."""
        return urllib.parse.urlparse((
            self.zenodo_url + self.API_PATH + self.DEPOSIT_PATH +
            self.NEW_DEPOSIT_URL
        ))

    def published_at(self, deposition_id):
        """Return published at URL."""
        return urllib.parse.urlparse(
            (self.zenodo_url + self.PUBLISH_PATH + str(deposition_id))
        )

    def deposit_at(self, deposition_id):
        """Return deposit at URL."""
        return urllib.parse.urlparse(
            (self.zenodo_url + self.DEPOSIT_PATH + str(deposition_id))
        )

    @staticmethod
    def _check_or_raise(response):
        """Check for expected response status code."""
        if response.status_code not in [200, 201, 202]:
            if response.status_code == 401:
                raise HTTPError('Access unauthorized - update access token.')

            if response.status_code == 400:
                err_response = response.json()
                errors = [
                    '"{0}" failed with "{1}"'.format(
                        err['field'], err['message']
                    ) for err in err_response['errors']
                ]

                raise HTTPError('\n' + '\n'.join(errors))

            else:
                raise HTTPError(response.content)

    @property
    def default_params(self):
        """Create request default params."""
        return {'access_token': self.secret}

    def dataset_to_request(self):
        """Prepare dataset metadata for request."""
        jsonld = self.dataset.asjsonld()
        jsonld['upload_type'] = 'dataset'
        return jsonld

    def export(self, to_publish):
        """Execute entire export process."""
        # Step 1. Create new deposition
        response = self.new_deposition()
        ZenodoExporter._check_or_raise(response)
        response = response.json()

        deposition_id = response['id']

        # Step 2. Upload all files to created deposition
        with tqdm(total=len(self.dataset.files)) as progressbar:
            for file_ in self.dataset.files:
                response = self.upload_file(
                    deposition_id,
                    file_.full_path,
                )
                ZenodoExporter._check_or_raise(response)
                progressbar.update(1)

        # Step 3. Attach metadata to deposition
        response = self.attach_metadata(deposition_id, self.dataset)
        ZenodoExporter._check_or_raise(response)

        # Step 4. Publish newly created deposition
        if to_publish:
            response = self.publish_deposition(deposition_id, self.secret)
            ZenodoExporter._check_or_raise(response)
            return self.published_at(deposition_id)

        return self.deposit_at(deposition_id)

    def new_deposition(self):
        """Create new deposition on Zenodo."""
        url = self.new_deposit_url().geturl()
        response = requests.post(
            url=url, params=self.default_params, json={}, headers=self.HEADERS
        )

        return response

    def upload_file(self, deposition_id, filepath):
        """Upload and attach a file to existing deposition on Zenodo."""
        request_payload = {'filename': Path(filepath).name}
        file = {'file': open(filepath, 'rb')}

        url = self.upload_file_url(deposition_id).geturl()
        response = requests.post(
            url=url,
            params=self.default_params,
            data=request_payload,
            files=file,
        )

        return response

    def attach_metadata(self, deposition_id, dataset):
        """Attach metadata to deposition on Zenodo."""
        request_payload = {
            'metadata': {
                'title': dataset.name,
                'upload_type': 'dataset',
                'description': dataset.description,
                'creators': [{
                    'name': creator.name,
                    'affiliation': creator.affiliation
                } for creator in dataset.creator]
            }
        }

        url = self.attach_metadata_url(deposition_id).geturl()
        response = requests.put(
            url=url,
            params=self.default_params,
            data=json.dumps(request_payload),
            headers=self.HEADERS
        )

        return response

    def publish_deposition(self, deposition_id, secret):
        """Publish existing deposition."""
        url = self.publish_url(deposition_id).geturl()

        response = requests.post(url=url, params=self.default_params)

        return response


@attr.s
class ZenodoProvider(ProviderApi):
    """zenodo.org registry API provider."""

    is_doi = attr.ib(default=False)
    _accept = attr.ib(default='application/json')

    @staticmethod
    def record_id(uri):
        """Extract record id from uri."""
        return urlparse(uri).path.split('/')[-1]

    def accept_json(self):
        """Receive response as json."""
        self._accept = 'application/json'
        return self

    def accept_jsonld(self):
        """Receive response as jsonld."""
        self._accept = 'application/ld+json'
        return self

    def make_request(self, uri):
        """Execute network request."""
        record_id = ZenodoProvider.record_id(uri)
        response = requests.get(
            make_records_url(record_id), headers={'Accept': self._accept}
        )
        if response.status_code != 200:
            raise LookupError('record not found')
        return response

    def find_record(self, uri):
        """Retrieves a record from Zenodo.

        :raises: ``LookupError``
        :param uri: DOI or URL
        :return: ``ZenodoRecord``
        """
        if self.is_doi:
            return self.find_record_by_doi(uri)

        return self.get_record(uri)

    def find_record_by_doi(self, doi):
        """Resolve the DOI and make a record for the retrieved record id."""
        doi = DOIProvider().find_record(doi)
        return self.get_record(ZenodoProvider.record_id(doi.url))

    def get_record(self, uri):
        """Retrieve record metadata and return ``ZenodoRecord``."""
        response = self.make_request(uri)

        return ZenodoRecordSerializer(**response.json(), zenodo=self, uri=uri)

    def get_exporter(self, dataset, secret):
        """Create export manager for given dataset."""
        return ZenodoExporter(dataset=dataset, secret=secret)
