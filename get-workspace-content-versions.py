import datetime
import logging
import os
import signal
import sys
import types
import uuid

import apscheduler.schedulers.blocking
import notch
import psycopg2.extras

import seismic

notch.configure()
log = logging.getLogger(__name__)


def batch_upsert_records(cur: psycopg2.extras.DictCursor, records: list[dict]) -> None:
    sql = """
        insert into seismic_workspace_content_versions_raw (
            id, created_at, created_by, format,
            library_content_version_id, name, preview_image_id,
            preview_image_url, thumbnail_image_id, thumbnail_image_url, size,
            version, version_creation_method, workspace_content_id,
            modified_at
        ) values (
            %(id)s, %(createdAt)s, %(createdBy)s, %(format)s,
            %(libraryContentVersionId)s, %(name)s, %(previewImageId)s,
            %(previewImageUrl)s, %(thumbnailImageId)s, %(thumbnailImageUrl)s, %(size)s,
            %(version)s, %(versionCreationMethod)s, %(workspaceContentId)s,
            %(modifiedAt)s
        ) on conflict (id) do update set
            created_at = %(createdAt)s, created_by = %(createdBy)s, format = %(format)s,
            library_content_version_id = %(libraryContentVersionId)s, name = %(name)s,
            preview_image_id = %(previewImageId)s,
            preview_image_url = %(previewImageUrl)s,
            thumbnail_image_id = %(thumbnailImageId)s,
            thumbnail_image_url = %(thumbnailImageUrl)s, size = %(size)s,
            version = %(version)s, version_creation_method = %(versionCreationMethod)s,
            workspace_content_id = %(workspaceContentId)s, modified_at = %(modifiedAt)s
    """
    plural = "s"
    if len(records) == 1:
        plural = ""
    log.info(f"Saving {len(records)} record{plural} to database")
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur: psycopg2.extras.DictCursor) -> datetime.datetime:
    sql = """
        select max(modified_at)::timestamptz max_modified_at
        from seismic_workspace_content_versions_raw
    """
    cur.execute(sql)
    row = cur.fetchone()
    val = row.get("max_modified_at")
    if val is None:
        val = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    return val


def main_job(repeat_interval_hours: int | None = None) -> None:
    log.info("Running the main job")

    cnx = psycopg2.connect(os.getenv("DB"), cursor_factory=psycopg2.extras.DictCursor)

    with cnx:
        with cnx.cursor() as cur:
            modified_at_start_time = get_max_modified_at(cur)

    client_id = uuid.UUID(hex=os.getenv("CLIENT_ID"))
    client_secret = uuid.UUID(hex=os.getenv("CLIENT_SECRET"))
    user_id = uuid.UUID(hex=os.getenv("USER_ID"))
    c = seismic.SeismicClient(client_id, client_secret, os.getenv("TENANT"), user_id)

    while modified_at_start_time < datetime.datetime.now(tz=datetime.UTC):
        modified_at_end_time = modified_at_start_time + datetime.timedelta(days=7)

        modified_at_end_time_s = modified_at_end_time.strftime("%Y-%m-%dT%H:%M:%S")
        modified_at_start_time_s = modified_at_start_time.strftime("%Y-%m-%dT%H:%M:%S")
        log.info(
            f"Looking for workspace content versions modified between "
            f"{modified_at_start_time_s} and {modified_at_end_time_s}"
        )
        params = {
            "modifiedAtStartTime": modified_at_start_time_s,
            "modifiedAtEndTime": modified_at_end_time_s,
        }

        records = c.workspace_content_versions(params)

        with cnx:
            with cnx.cursor() as cur:
                batch_upsert_records(cur, records)

        modified_at_start_time = modified_at_end_time

    if repeat_interval_hours:
        plural = "s"
        if repeat_interval_hours == 1:
            plural = ""
        repeat_message = f"see you again in {repeat_interval_hours} hour{plural}"
    else:
        repeat_message = "quitting"
    log.info(f"Main job complete, {repeat_message}")


def main() -> None:
    repeat = os.getenv("REPEAT", "false").lower() in ("1", "on", "true", "yes")
    if repeat:
        repeat_interval_hours = int(os.getenv("REPEAT_INTERVAL_HOURS", "6"))
        log.info(f"This job will repeat every {repeat_interval_hours} hours")
        log.info(
            "Change this value by setting the "
            "REPEAT_INTERVAL_HOURS environment variable"
        )
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(
            main_job,
            "interval",
            args=[repeat_interval_hours],
            hours=repeat_interval_hours,
        )
        scheduler.add_job(main_job, args=[repeat_interval_hours])
        scheduler.start()
    else:
        main_job()


def handle_sigterm(_signal: int, _frame: types.FrameType) -> None:
    sys.exit()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
