import apscheduler.schedulers.blocking
import datetime
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_content_view_history')


def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_content_view_history_raw (
            id, action, application, content_id, content_version_id, content_profile_id,
            content_profile_name, context_id, context_name, context_type, context_system_type,
            instance_name, library_content_id, library_content_version_id, occurred_at, product_area,
            user_id, user_username, workspace_content_id, workspace_content_version_id, modified_at
        ) values (
            %(id)s, %(action)s, %(application)s, %(contentId)s, %(contentVersionId)s, %(contentProfileId)s,
            %(contentProfileName)s, %(contextId)s, %(contextName)s, %(contextType)s, %(contextSystemType)s,
            %(instanceName)s, %(libraryContentId)s, %(libraryContentVersionId)s, %(occurredAt)s, %(productArea)s,
            %(userId)s, %(userUsername)s, %(workspaceContentId)s, %(workspaceContentVersionId)s, %(modifiedAt)s
        ) on conflict (id) do update set
            action = %(action)s, application = %(application)s, content_id = %(contentId)s,
            content_version_id = %(contentVersionId)s, content_profile_id = %(contentProfileId)s,
            content_profile_name = %(contentProfileName)s, context_id = %(contextId)s, context_name = %(contextName)s,
            context_type = %(contextType)s, context_system_type = %(contextSystemType)s,
            instance_name = %(instanceName)s, library_content_id = %(libraryContentId)s,
            library_content_version_id = %(libraryContentVersionId)s, occurred_at = %(occurredAt)s,
            product_area = %(productArea)s, user_id = %(userId)s, user_username = %(userUsername)s,
            workspace_content_id = %(workspaceContentId)s, workspace_content_version_id = %(workspaceContentVersionId)s,
            modified_at = %(modifiedAt)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_content_view_history_raw
    '''
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get('max_modified_at')
    if val is None:
        val = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    return val


def main_job(repeat_interval_minutes: int = None):
    log.info('Running the main job')

    cnx = psycopg2.connect(os.getenv('DB'), cursor_factory=psycopg2.extras.DictCursor)

    with cnx:
        with cnx.cursor() as cur:
            max_modified_at = get_max_modified_at(cur)
    max_modified_at_s = max_modified_at.strftime('%Y-%m-%dT%H:%M:%S')

    params = {
        'modifiedAtStartTime': max_modified_at_s,
    }

    log.info(f'Looking for content view history modified after {max_modified_at_s}')

    client_id = uuid.UUID(hex=os.getenv('CLIENT_ID'))
    client_secret = uuid.UUID(hex=os.getenv('CLIENT_SECRET'))
    user_id = uuid.UUID(hex=os.getenv('USER_ID'))

    c = seismic.SeismicClient(client_id, client_secret, os.getenv('TENANT'), user_id)
    records = c.content_view_history(params)

    with cnx:
        with cnx.cursor() as cur:
            batch_upsert_records(cur, records)

    if repeat_interval_minutes:
        plural = 's'
        if repeat_interval_minutes == 1:
            plural = ''
        repeat_message = f'see you again in {repeat_interval_minutes} minute{plural}'
    else:
        repeat_message = 'quitting'
    log.info(f'Main job complete, {repeat_message}')


def main():
    repeat = os.getenv('REPEAT', 'false').lower() in ('1', 'on', 'true', 'yes')
    if repeat:
        repeat_interval_minutes = int(os.getenv('REPEAT_INTERVAL_MINUTES', '60'))
        log.info(f'This job will repeat every {repeat_interval_minutes} minutes')
        log.info('Change this value by setting the REPEAT_INTERVAL_MINUTES environment variable')
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', args=[repeat_interval_minutes], minutes=repeat_interval_minutes)
        scheduler.add_job(main_job, args=[repeat_interval_minutes])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
