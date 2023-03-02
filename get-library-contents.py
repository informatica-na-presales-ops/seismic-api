import apscheduler.schedulers.blocking
import datetime
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_library_contents')


def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_library_contents_raw (
            id, name, version, created_at, modified_at, type, format, is_checked_out,
            is_deleted, is_published, published_version_expires_at, latest_library_content_version_created_at,
            latest_library_content_version_created_by, latest_library_content_version_created_by_username,
            latest_library_content_version_id, latest_library_content_version_size, library_url, doc_center_url,
            news_center_url, owner_id, owner_username, owner_email, teamsite_id, teamsite_name,
            preview_image_id, preview_image_url, thumbnail_image_id, thumbnail_image_url, description,
            short_id, parent_folder_library_content_id, library_path, has_planner_associations, origin_type,
            last_modified
        ) values (
            %(id)s, %(name)s, %(version)s, %(createdAt)s, %(modifiedAt)s, %(type)s, %(format)s, %(isCheckedOut)s,
            %(isDeleted)s, %(isPublished)s, %(publishedVersionExpiresAt)s, %(latestLibraryContentVersionCreatedAt)s,
            %(latestLibraryContentVersionCreatedBy)s, %(latestLibraryContentVersionCreatedByUsername)s,
            %(latestLibraryContentVersionId)s, %(latestLibraryContentVersionSize)s, %(libraryUrl)s, %(docCenterUrl)s,
            %(newsCenterUrl)s, %(ownerId)s, %(ownerUsername)s, %(ownerEmail)s, %(teamsiteId)s, %(teamsiteName)s,
            %(previewImageId)s, %(previewImageUrl)s, %(thumbnailImageId)s, %(thumbnailImageUrl)s, %(description)s,
            %(shortId)s, %(parentFolderLibraryContentId)s, %(libraryPath)s, %(hasPlannerAssociations)s, %(originType)s,
            %(lastModified)s
        ) on conflict (id) do update set
            name = %(name)s, version = %(version)s, created_at = %(createdAt)s, modified_at = %(modifiedAt)s,
            type = %(type)s, format = %(format)s, is_checked_out = %(isCheckedOut)s, is_deleted = %(isDeleted)s,
            is_published = %(isPublished)s, published_version_expires_at = %(publishedVersionExpiresAt)s,
            latest_library_content_version_created_at = %(latestLibraryContentVersionCreatedAt)s,
            latest_library_content_version_created_by = %(latestLibraryContentVersionCreatedBy)s,
            latest_library_content_version_created_by_username = %(latestLibraryContentVersionCreatedByUsername)s,
            latest_library_content_version_id = %(latestLibraryContentVersionId)s,
            latest_library_content_version_size = %(latestLibraryContentVersionSize)s, library_url = %(libraryUrl)s,
            doc_center_url = %(docCenterUrl)s, news_center_url = %(newsCenterUrl)s, owner_id = %(ownerId)s,
            owner_username = %(ownerUsername)s, owner_email = %(ownerEmail)s, teamsite_id = %(teamsiteId)s,
            teamsite_name = %(teamsiteName)s, preview_image_id = %(previewImageId)s,
            preview_image_url = %(previewImageUrl)s, thumbnail_image_id = %(thumbnailImageId)s,
            thumbnail_image_url = %(thumbnailImageUrl)s, description = %(description)s, short_id = %(shortId)s,
            parent_folder_library_content_id = %(parentFolderLibraryContentId)s, library_path = %(libraryPath)s,
            has_planner_associations = %(hasPlannerAssociations)s, origin_type = %(originType)s,
            last_modified = %(lastModified)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_library_contents_raw
    '''
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get('max_modified_at')
    if val is None:
        val = datetime.datetime(2023, 1, 20, tzinfo=datetime.timezone.utc)
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
        modified_at_end_time = modified_at_start_time + datetime.timedelta(days=10)

        modified_at_end_time_s = modified_at_end_time.strftime('%Y-%m-%dT%H:%M:%S')
        modified_at_start_time_s = modified_at_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        log.info(f'Looking for library contents modified between {modified_at_start_time_s} and {modified_at_end_time_s}')
        params = {
            'modifiedAtStartTime': modified_at_start_time_s,
            'modifiedAtEndTime': modified_at_end_time_s,
        }

        records = c.library_contents(params)

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
