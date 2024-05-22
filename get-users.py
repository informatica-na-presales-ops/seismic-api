import apscheduler.schedulers.blocking
import datetime
import itertools
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_users')


def batch_upsert_user_property_assignments(cur, records):
    sql = '''
        insert into seismic_user_property_assignments (
            _synced, modified_at, user_id, user_property_id, user_property_name, user_property_type,
            user_property_value, user_property_value_boolean, user_property_value_datetime,
            user_property_value_float, user_property_value_integer
        ) values (
            true, %(modifiedAt)s, %(userId)s, %(userPropertyId)s, %(userPropertyName)s, %(userPropertyType)s,
            %(userPropertyValue)s, %(userPropertyValueBoolean)s, %(userPropertyValueDatetime)s,
            %(userPropertyValueFloat)s, %(userPropertyValueInteger)s
        ) on conflict (user_id, user_property_id) do update set
            _synced = true, modified_at = excluded.modified_at, user_property_name = excluded.user_property_name,
            user_property_type = excluded.user_property_type, user_property_value = excluded.user_property_value,
            user_property_value_boolean = excluded.user_property_value_boolean,
            user_property_value_datetime = excluded.user_property_value_datetime,
            user_property_value_float = excluded.user_property_value_float,
            user_property_value_integer = excluded.user_property_value_integer
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} user property assignment{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def user_property_assignments_sync_begin(cur):
    sql = '''
        update seismic_user_property_assignments
        set _synced = false
        where _synced is null or _synced is true
    '''
    cur.execute(sql)


def user_property_assignments_sync_end(cur):
    sql = '''
        delete from seismic_user_property_assignments
        where _synced is false
    '''
    cur.execute(sql)


def batch_upsert_users(cur, records):
    sql = '''
        insert into seismic_users_raw (
            id, created_at, default_content_profile_id, default_content_profile_name, deleted_at, email,
            email_domain, first_name, full_name, is_deleted, is_lessonly_enabled, is_seismic_employee,
            is_seismic_enabled, is_system_admin, last_name, license_type, modified_at, organization,
            sso_user_id, title, username, is_locked, address, phone_number, latest_activity_date,
            external_id
        ) values (
            %(id)s, %(createdAt)s, %(defaultContentProfileId)s, %(defaultContentProfileName)s, %(deletedAt)s, %(email)s,
            %(emailDomain)s, %(firstName)s, %(fullName)s, %(isDeleted)s, %(isLessonlyEnabled)s, %(isSeismicEmployee)s,
            %(isSeismicEnabled)s, %(isSystemAdmin)s, %(lastName)s, %(licenseType)s, %(modifiedAt)s, %(organization)s,
            %(ssoUserId)s, %(title)s, %(username)s, %(isLocked)s, %(address)s, %(phoneNumber)s, %(latestActivityDate)s,
            %(externalId)s
        ) on conflict (id) do update set
            created_at = excluded.created_at, default_content_profile_id = excluded.default_content_profile_id,
            default_content_profile_name = excluded.default_content_profile_name, deleted_at = excluded.deleted_at,
            email = excluded.email, email_domain = excluded.email_domain, first_name = excluded.first_name,
            full_name = excluded.full_name, is_deleted = excluded.is_deleted,
            is_lessonly_enabled = excluded.is_lessonly_enabled, is_seismic_employee = excluded.is_seismic_employee,
            is_seismic_enabled = excluded.is_seismic_enabled, is_system_admin = excluded.is_system_admin,
            last_name = excluded.last_name, license_type = excluded.license_type, modified_at = excluded.modified_at,
            organization = excluded.organization, sso_user_id = excluded.sso_user_id, title = excluded.title,
            username = excluded.username, is_locked = excluded.is_locked, address = excluded.address,
            phone_number = excluded.phone_number, latest_activity_date = excluded.latest_activity_date,
            external_id = excluded.external_id
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} user{plural} to database')
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
        val = datetime.datetime(2018, 1, 1, tzinfo=datetime.timezone.utc)
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
                batch_upsert_users(cur, records)

        modified_at_start_time = modified_at_end_time

    log.info('Updating user property assignments')
    with cnx:
        with cnx.cursor() as cur:
            user_property_assignments_sync_begin(cur)
    for batch in itertools.batched(c.user_property_assignments(), 3000):
        with cnx:
            with cnx.cursor() as cur:
                batch_upsert_user_property_assignments(cur, batch)
    with cnx:
        with cnx.cursor() as cur:
            user_property_assignments_sync_end(cur)

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
