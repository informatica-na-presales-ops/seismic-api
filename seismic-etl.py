import apscheduler.schedulers.blocking
import datetime
import logging
import os
import pathlib
import requests
import requests.auth
import signal
import sys

log = logging.getLogger(__name__)


class Settings:
    _true_values = ('1', 'true', 'on', 'yes')

    def __init__(self):
        self.client_id = os.getenv('SEISMIC_CLIENT_ID')
        self.client_secret = os.getenv('SEISMIC_CLIENT_SECRET')
        self.interval = int(os.getenv('INTERVAL', '6'))
        self.log_format = os.getenv('LOG_FORMAT', '%(levelname)s [%(name)s] %(message)s')
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.output_folder = pathlib.Path(os.getenv('OUTPUT_FOLDER', '/data'))
        self.password = os.getenv('SEISMIC_PASSWORD')
        self.run_and_exit = os.getenv('RUN_AND_EXIT', 'False').lower() in self._true_values
        self.tenant = os.getenv('SEISMIC_TENANT')
        self.username = os.getenv('SEISMIC_USERNAME')
        self.version = os.getenv('APP_VERSION')


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token: str):
        self.token = token

    def __call__(self, r: requests.Request) -> requests.Request:
        r.headers['Authorization'] = f'Bearer {self.token}'
        return r


class SeismicClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.session = requests.Session()
        self._token = None
        self.token_expiration = None

    @property
    def token_expired(self) -> bool:
        if self.token_expiration is None:
            return True
        if self.token_expiration < datetime.datetime.utcnow():
            return True
        return False

    @property
    def token(self) -> str:
        if self._token is None or self.token_expired:
            log.debug('Getting a new access token')
            url = f'https://auth.seismic.com/tenants/{self.settings.tenant}/connect/token'
            data = {
                'grant_type': 'password',
                'client_id': self.settings.client_id,
                'client_secret': self.settings.client_secret,
                'username': self.settings.username,
                'password': self.settings.password,
                'scope': 'download library reporting'
            }
            resp = self.session.post(url, data=data)
            resp.raise_for_status()
            j = resp.json()
            self._token = j.get('access_token')
            expires_in = j.get('expires_in')
            self.token_expiration = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)
            log.debug(f'Access token {self._token[:6]}... will expire at {self.token_expiration}')
        return self._token

    def get_report_csv(self, report_name: str, **kwargs):
        log.info(f'Getting csv for {report_name}')
        url = f'https://api.seismic.com/reporting/v2/{report_name}'
        headers = {'Accept': 'text/csv'}
        resp = self.session.get(url, auth=BearerAuth(self.token), headers=headers, params=kwargs)
        resp.raise_for_status()
        return resp.text

    def get_report_json(self, report_name: str, **kwargs):
        log.info(f'Getting json for {report_name}')
        url = f'https://api.seismic.com/reporting/v2/{report_name}'
        headers = {'Accept': 'application/json'}
        resp = self.session.get(url, auth=BearerAuth(self.token), headers=headers, params=kwargs)
        resp.raise_for_status()
        return resp.json()


def main_job(settings: Settings):
    c = SeismicClient(settings)
    reports = [
        'contentUsageHistory',
        'contentViewHistory',
        'searchHistory',
        'livesendLinks',
        'livesendLinkContents',
        'livesendLinkMembers',
        'livesendPageViews',
        'generatedLiveDocs',
        'generatedLiveDocComponents',
        'generatedLiveDocSlides',
        'generatedLiveDocFields',
        'generatedLiveDocOutputFormats',
        'libraryContents',
        'contentPropertyAssignments',
        'workspaceContents',
        'users',
        'userPropertyAssignments',
        'groupMembers',
    ]
    for report_name in reports:
        try:
            result = c.get_report_csv(report_name)
        except requests.exceptions.HTTPError as e:
            log.error(e)
            continue
        output_file = settings.output_folder / f'{report_name}.csv'
        with output_file.open('w') as f:
            f.write(result)


def main():
    settings = Settings()
    logging.basicConfig(format=settings.log_format, level=logging.DEBUG, stream=sys.stdout)
    log.debug(f'seismic-etl {settings.version}')
    if not settings.log_level == 'DEBUG':
        log.debug(f'Changing log level to {settings.log_level}')
    logging.getLogger().setLevel(settings.log_level)

    log.info(f'RUN_AND_EXIT: {settings.run_and_exit}')
    if settings.run_and_exit:
        main_job(settings)
    else:
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()

        # Run it now ...
        scheduler.add_job(main_job, args=[settings])
        # ... and run it later!
        scheduler.add_job(main_job, 'interval', args=[settings], hours=settings.interval)

        scheduler.start()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
