import apscheduler.schedulers.blocking
import datetime
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_workspace_contents')


def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_workspace_contents_raw (
            id, created_at, created_by, is_cart_content, is_contextual_folder_content, is_deleted,
            latest_workspace_content_version_created_at, latest_workspace_content_version_id,
            latest_workspace_content_version_size, origin_content_profile_id, library_content_id,
            materialized_path, modified_at, name, preview_image_id, preview_image_url,
            thumbnail_image_id, thumbnail_image_url, version, context_id, context_name, context_type,
            context_system_type, origin_application
        ) values (
            %(id)s, %(createdAt)s, %(createdBy)s, %(isCartContent)s, %(isContextualFolderContent)s, %(isDeleted)s,
            %(latestWorkspaceContentVersionCreatedAt)s, %(latestWorkspaceContentVersionId)s,
            %(latestWorkspaceContentVersionSize)s, %(originContentProfileId)s, %(libraryContentId)s,
            %(materializedPath)s, %(modifiedAt)s, %(name)s, %(previewImageId)s, %(previewImageUrl)s,
            %(thumbnailImageId)s, %(thumbnailImageUrl)s, %(version)s, %(contextId)s, %(contextName)s, %(contextType)s,
            %(contextSystemType)s, %(originApplication)s
        ) on conflict (id) do update set
            created_at = %(createdAt)s, created_by = %(createdBy)s, is_cart_content = %(isCartContent)s,
            is_contextual_folder_content = %(isContextualFolderContent)s, is_deleted = %(isDeleted)s,
            latest_workspace_content_version_created_at = %(latestWorkspaceContentVersionCreatedAt)s,
            latest_workspace_content_version_id = %(latestWorkspaceContentVersionId)s,
            latest_workspace_content_version_size = %(latestWorkspaceContentVersionSize)s,
            origin_content_profile_id = %(originContentProfileId)s, library_content_id = %(libraryContentId)s,
            materialized_path = %(materializedPath)s, modified_at = %(modifiedAt)s, name = %(name)s,
            preview_image_id = %(previewImageId)s, preview_image_url = %(previewImageUrl)s,
            thumbnail_image_id = %(thumbnailImageId)s, thumbnail_image_url = %(thumbnailImageUrl)s,
            version = %(version)s, context_id = %(contextId)s, context_name = %(contextName)s,
            context_type = %(contextType)s, context_system_type = %(contextSystemType)s,
            origin_application = %(originApplication)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_workspace_contents_raw
    '''
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get('max_modified_at')
    if val is None:
        val = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    return val


def main_job(repeat_interval_minutes: int = None):
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
        modified_at_end_time = modified_at_start_time + datetime.timedelta(days=7)

        modified_at_end_time_s = modified_at_end_time.strftime('%Y-%m-%dT%H:%M:%S')
        modified_at_start_time_s = modified_at_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        log.info(f'Looking for workspace contents modified between {modified_at_start_time_s} and {modified_at_end_time_s}')
        params = {
            'modifiedAtStartTime': modified_at_start_time_s,
            'modifiedAtEndTime': modified_at_end_time_s,
        }

        records = c.workspace_contents(params)

        with cnx:
            with cnx.cursor() as cur:
                batch_upsert_records(cur, records)

        modified_at_start_time = modified_at_end_time

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
