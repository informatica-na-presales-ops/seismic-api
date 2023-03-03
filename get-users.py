import apscheduler.schedulers.blocking
import datetime
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_users')


def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_users_raw (
            id, created_at, default_content_profile_id, default_content_profile_name, deleted_at, email,
            email_domain, first_name, full_name, is_deleted, is_seismic_employee, is_system_admin,
            last_name, license_type, modified_at, organization, sso_user_id, title, username,
            is_locked, address, phone_number, latest_activity_date, external_id
        ) values (
            %(id)s, %(createdAt)s, %(defaultContentProfileId)s, %(defaultContentProfileName)s, %(deletedAt)s, %(email)s,
            %(emailDomain)s, %(firstName)s, %(fullName)s, %(isDeleted)s, %(isSeismicEmployee)s, %(isSystemAdmin)s,
            %(lastName)s, %(licenseType)s, %(modifiedAt)s, %(organization)s, %(ssoUserId)s, %(title)s, %(username)s,
            %(isLocked)s, %(address)s, %(phoneNumber)s, %(latestActivityDate)s, %(externalId)s
        ) on conflict (id) do update set
            created_at = %(createdAt)s, default_content_profile_id = %(defaultContentProfileId)s,
            default_content_profile_name = %(defaultContentProfileName)s, deleted_at = %(deletedAt)s, email = %(email)s,
            email_domain = %(emailDomain)s, first_name = %(firstName)s, full_name = %(fullName)s,
            is_deleted = %(isDeleted)s, is_seismic_employee = %(isSeismicEmployee)s,
            is_system_admin = %(isSystemAdmin)s, last_name = %(lastName)s, license_type = %(licenseType)s,
            modified_at = %(modifiedAt)s, organization = %(organization)s, sso_user_id = %(ssoUserId)s,
            title = %(title)s, username = %(username)s, is_locked = %(isLocked)s, address = %(address)s,
            phone_number = %(phoneNumber)s, latest_activity_date = %(latestActivityDate)s, external_id = %(externalId)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_users_raw
    '''
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get('max_modified_at')
    if val is None:
        val = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    return val


def main_job(repeat_interval_hours: int = None):
    log.info('Running the main job')

    cnx = psycopg2.connect(os.getenv('DB'), cursor_factory=psycopg2.extras.DictCursor)

    with cnx:
        with cnx.cursor() as cur:
            modified_at_start_time = get_max_modified_at(cur)

    client_id = uuid.UUID(hex=os.getenv('CLIENT_ID'))
    client_secret = uuid.UUID(hex=os.getenv('CLIENT_SECRET'))
    user_id = uuid.UUID(hex=os.getenv('USER_ID'))
    c = seismic.SeismicClient(client_id, client_secret, os.getenv('TENANT'), user_id)

    while modified_at_start_time < datetime.datetime.now(tz=datetime.timezone.utc):
        modified_at_end_time = modified_at_start_time + datetime.timedelta(days=30)

        modified_at_end_time_s = modified_at_end_time.strftime('%Y-%m-%dT%H:%M:%S')
        modified_at_start_time_s = modified_at_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        log.info(f'Looking for users modified between {modified_at_start_time_s} and {modified_at_end_time_s}')
        params = {
            'modifiedAtStartTime': modified_at_start_time_s,
            'modifiedAtEndTime': modified_at_end_time_s,
        }

        records = c.users(params)

        with cnx:
            with cnx.cursor() as cur:
                batch_upsert_records(cur, records)

        modified_at_start_time = modified_at_end_time

    if repeat_interval_hours:
        plural = 's'
        if repeat_interval_hours == 1:
            plural = ''
        repeat_message = f'see you again in {repeat_interval_hours} hour{plural}'
    else:
        repeat_message = 'quitting'
    log.info(f'Main job complete, {repeat_message}')


def main():
    repeat = os.getenv('REPEAT', 'false').lower() in ('1', 'on', 'true', 'yes')
    if repeat:
        repeat_interval_hours = int(os.getenv('REPEAT_INTERVAL_HOURS', '6'))
        log.info(f'This job will repeat every {repeat_interval_hours} hours')
        log.info('Change this value by setting the REPEAT_INTERVAL_HOURS environment variable')
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', args=[repeat_interval_hours], hours=repeat_interval_hours)
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
