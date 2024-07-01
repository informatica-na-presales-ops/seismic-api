import datetime
import httpx
import logging
import uuid

from typing import Generator

log = logging.getLogger(__name__)

def now():
    return datetime.datetime.now(datetime.timezone.utc)


class SeismicClient:
    client_id: uuid.UUID
    client_secret: uuid.UUID
    tenant: str
    user_id: uuid.UUID

    _session: httpx.Client = None
    _token: str = None
    _token_expiration: datetime.datetime = None

    def __init__(self, client_id: uuid.UUID, client_secret: uuid.UUID, tenant: str, user_id: uuid.UUID):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.user_id = user_id

    def _get_json(self, endpoint: str, params: dict = None):
        url = f'https://api.seismic.com/reporting/v2/{endpoint}'
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def content_usage_history(self, params: dict = None):
        return self._get_json('contentUsageHistory', params)

    def content_view_history(self, params: dict = None):
        return self._get_json('contentViewHistory', params)

    def library_content_versions(self, params: dict = None):
        return self._get_json('libraryContentVersions', params)

    def library_contents(self, params: dict = None):
        return self._get_json('libraryContents', params)

    def scim_users(self) -> Generator[dict, None, None]:
        results_per_page = 100
        url = 'https://api.seismic.com/scim/v2/Users'
        params = {
            'count': results_per_page,
            'startIndex': 1,
        }
        more = True
        while more:
            resp = self.session.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            yield from data.get('Resources')
            params.update({
                'startIndex': params.get('startIndex') + results_per_page,
            })
            if params.get('startIndex') > data.get('totalResults'):
                more = False

    def search_history(self, params: dict = None):
        return self._get_json('searchHistory', params)

    @property
    def session(self) -> httpx.Client:
        if self._session is None:
            log.debug('Setting up a new session')
            self._session = httpx.Client()
            self._session.headers.update({
                'Accept': 'application/json',
            })
        if self._token is None \
        or self._token_expiration is None \
        or self._token_expiration < now():
            log.debug('Getting a new access token')
            url = f'https://auth.seismic.com/tenants/{self.tenant}/connect/token'
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'grant_type': 'delegation',
                'scope': 'seismic.reporting seismic.library.view seismic.user.manage seismic.user.view',
                'user_id': self.user_id,
            }
            resp = self._session.post(url, data=data)
            resp.raise_for_status()
            j = resp.json()
            self._token = j.get('access_token')
            expires_in = j.get('expires_in')
            self._token_expiration = now() + datetime.timedelta(seconds=expires_in - 10)
            self._session.headers.update({
                'Authorization': f'Bearer {self._token}',
            })
        return self._session

    def user_property_assignments(self, params: dict = None):
        return self._get_json('userPropertyAssignments', params)

    def users(self, params: dict = None):
        return self._get_json('users', params)

    def workspace_content_versions(self, params: dict = None):
        return self._get_json('workspaceContentVersions', params)

    def workspace_contents(self, params: dict = None):
        return self._get_json('workspaceContents', params)
