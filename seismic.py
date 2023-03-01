import datetime
import logging
import requests
import uuid

log = logging.getLogger(__name__)

def now():
    return datetime.datetime.now(datetime.timezone.utc)


class SeismicClient:
    client_id: uuid.UUID
    client_secret: uuid.UUID
    tenant: str
    user_id: uuid.UUID

    _session: requests.Session = None
    _token: str = None
    _token_expiration: datetime.datetime = None

    def __init__(self, client_id: uuid.UUID, client_secret: uuid.UUID, tenant: str, user_id: uuid.UUID):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant
        self.user_id = user_id

    def content_usage_history(self, params: dict = None):
        url = 'https://api.seismic.com/reporting/v2/contentUsageHistory'
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def content_view_history(self, params: dict = None):
        url = 'https://api.seismic.com/reporting/v2/contentViewHistory'
        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                'Accept': 'application/json',
            })
        if self._token is None \
        or self._token_expiration is None \
        or self._token_expiration >= now():
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
