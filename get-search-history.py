import apscheduler.schedulers.blocking
import datetime
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import uuid

log = notch.make_log('seismic_api.get_search_history')


def batch_upsert_records(cur, records):
    sql = '''
        insert into seismic_search_history_raw (
            id, occurred_at, active_scope, application, result_count, result_count_content_manager,
            result_count_control_center, result_count_doc_center, result_count_news_center, result_count_workspace,
            search_cycle_id, search_term_normalized, search_term_raw, search_type, sort_by, user_id,
            modified_at, step_index, step_type, was_clicked, facet_values
        ) values (
            %(id)s, %(occurredAt)s, %(activeScope)s, %(application)s, %(resultCount)s, %(resultCountContentManager)s,
            %(resultCountControlCenter)s, %(resultCountDocCenter)s, %(resultCountNewsCenter)s, %(resultCountWorkspace)s,
            %(searchCycleId)s, %(searchTermNormalized)s, %(searchTermRaw)s, %(searchType)s, %(sortBy)s, %(userId)s,
            %(modifiedAt)s, %(stepIndex)s, %(stepType)s, %(wasClicked)s, %(facetValues)s
        ) on conflict (id) do update set
            occurred_at = %(occurredAt)s, active_scope = %(activeScope)s, application = %(application)s,
            result_count = %(resultCount)s, result_count_content_manager = %(resultCountContentManager)s,
            result_count_control_center = %(resultCountControlCenter)s,
            result_count_doc_center = %(resultCountDocCenter)s, result_count_news_center = %(resultCountNewsCenter)s,
            result_count_workspace = %(resultCountWorkspace)s, search_cycle_id = %(searchCycleId)s,
            search_term_normalized = %(searchTermNormalized)s, search_term_raw = %(searchTermRaw)s,
            search_type = %(searchType)s, sort_by = %(sortBy)s, user_id = %(userId)s, modified_at = %(modifiedAt)s,
            step_index = %(stepIndex)s, step_type = %(stepType)s, was_clicked = %(wasClicked)s,
            facet_values = %(facetValues)s
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} record{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def get_max_modified_at(cur):
    sql = '''
        select max(modified_at)::timestamptz max_modified_at
        from seismic_search_history_raw
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
            modified_at_start_time = get_max_modified_at(cur)

    client_id = uuid.UUID(hex=os.getenv('CLIENT_ID'))
    client_secret = uuid.UUID(hex=os.getenv('CLIENT_SECRET'))
    user_id = uuid.UUID(hex=os.getenv('USER_ID'))
    c = seismic.SeismicClient(client_id, client_secret, os.getenv('TENANT'), user_id)

    while modified_at_start_time < datetime.datetime.now(tz=datetime.timezone.utc):
        modified_at_end_time = modified_at_start_time + datetime.timedelta(days=2)

        modified_at_end_time_s = modified_at_end_time.strftime('%Y-%m-%dT%H:%M:%S')
        modified_at_start_time_s = modified_at_start_time.strftime('%Y-%m-%dT%H:%M:%S')
        log.info(f'Looking for search history modified between {modified_at_start_time_s} and {modified_at_end_time_s}')
        params = {
            'modifiedAtStartTime': modified_at_start_time_s,
            'modifiedAtEndTime': modified_at_end_time_s,
        }

        records = c.search_history(params)

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
