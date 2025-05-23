import apscheduler.schedulers.blocking
import datetime
import logging
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

notch.configure()
log = logging.getLogger(__name__)

def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_content_usage_history_raw (
            id, action, action_type, application, content_id, content_version_id,
            content_profile_id, content_profile_name, context_id, context_name, context_type,
            context_system_type, instance_name, is_bound_delivery, library_content_id,
            library_content_version_id, livesend_link_content_id, livesend_link_id, occurred_at, product_area,
            total_pages, user_id, user_username, workspace_content_id, workspace_content_version_id,
            modified_at, interaction_id
        ) values (
            %(id)s, %(action)s, %(actionType)s, %(application)s, %(contentId)s, %(contentVersionId)s,
            %(contentProfileId)s, %(contentProfileName)s, %(contextId)s, %(contextName)s, %(contextType)s,
            %(contextSystemType)s, %(instanceName)s, %(isBoundDelivery)s, %(libraryContentId)s,
            %(libraryContentVersionId)s, %(livesendLinkContentId)s, %(livesendLinkId)s, %(occurredAt)s, %(productArea)s,
            %(totalPages)s, %(userId)s, %(userUsername)s, %(workspaceContentId)s, %(workspaceContentVersionId)s,
            %(modifiedAt)s, %(interactionId)s
        ) on conflict (id) do update set
            action = %(action)s, action_type = %(actionType)s, application = %(application)s,
            content_id = %(contentId)s, content_version_id = %(contentVersionId)s,
            content_profile_id = %(contentProfileId)s, content_profile_name = %(contentProfileName)s,
            context_id = %(contextId)s, context_name = %(contextName)s, context_type = %(contextType)s,
            context_system_type = %(contextSystemType)s, instance_name = %(instanceName)s,
            is_bound_delivery = %(isBoundDelivery)s, library_content_id = %(libraryContentId)s,
            library_content_version_id = %(libraryContentVersionId)s,
            livesend_link_content_id = %(livesendLinkContentId)s, livesend_link_id = %(livesendLinkId)s,
            occurred_at = %(occurredAt)s, product_area = %(productArea)s, total_pages = %(totalPages)s,
            user_id = %(userId)s, user_username = %(userUsername)s, workspace_content_id = %(workspaceContentId)s,
            workspace_content_version_id = %(workspaceContentVersionId)s, modified_at = %(modifiedAt)s,
            interaction_id = %(interactionId)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_content_usage_history_raw
    '''
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get('max_modified_at')
    if val is None:
        val = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
    return val


def main_job(repeat_interval_hours: int = None):
    log.info('Running the main job')

    cnx = psycopg2.connect(os.getenv('DB'), cursor_factory=psycopg2.extras.DictCursor)

    with cnx:
        with cnx.cursor() as cur:
            max_modified_at = get_max_modified_at(cur)
    max_modified_at_s = max_modified_at.strftime('%Y-%m-%dT%H:%M:%S')

    params = {
        'modifiedAtStartTime': max_modified_at_s,
    }

    log.info(f'Looking for content usage history modified after {max_modified_at_s}')

    client_id = uuid.UUID(hex=os.getenv('CLIENT_ID'))
    client_secret = uuid.UUID(hex=os.getenv('CLIENT_SECRET'))
    user_id = uuid.UUID(hex=os.getenv('USER_ID'))

    c = seismic.SeismicClient(client_id, client_secret, os.getenv('TENANT'), user_id)
    records = c.content_usage_history(params)

    with cnx:
        with cnx.cursor() as cur:
            batch_upsert_records(cur, records)

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
