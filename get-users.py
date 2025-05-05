import apscheduler.schedulers.blocking
import datime
import logging
import notch
import os
import psycopg2.extras
import seismic
import signal
import sys
import time
import uuid

notch.configure()
log = logging.getLogger(__name__)


def _sync_cleanup(cur):
    sql = '''
        update seismic_users_scim
        set _deleted = true
        where _synced is false
    '''
    cur.execute(sql)


def _sync_prepare(cur):
    sql = '''
        update seismic_users_scim
        set _synced = false
        where _synced is true
    '''
    cur.execute(sql)


def batch_upsert_users(cur, records):
    sql = '''
        insert into seismic_users_scim (
            active, biography, cost_center, cost_center_ent, country, created_at,
            created_by, creator_type, deactivated_at, department, direct_reports_with_cntrcts,
            direct_reports_without_cntrcts, email_work, employee_id, external_id, family_name,
            function, function_hierarchy, given_name, hire_date, id, job_family,
            job_profile, length_of_service, location, management_level, manager_level_2,
            manager_level_3, manager_level_4, manager_level_5, manager_level_6, manager_level_7,
            manager_level_8, manager_name, modified_at, organization, preferred_language,
            role_content, role_learning, sso_id, sub_function, subregion, time_in_job_profile,
            time_zone, title, user_name, user_type, worker_status, _deleted, _synced
        ) values (
            %(active)s, %(biography)s, %(cost_center)s, %(cost_center_ent)s, %(country)s, %(created_at)s,
            %(created_by)s, %(creator_type)s, %(deactivated_at)s, %(department)s, %(direct_reports_with_cntrcts)s,
            %(direct_reports_without_cntrcts)s, %(email_work)s, %(employee_id)s, %(external_id)s, %(family_name)s,
            %(function)s, %(function_hierarchy)s, %(given_name)s, %(hire_date)s, %(id)s, %(job_family)s,
            %(job_profile)s, %(length_of_service)s, %(location)s, %(management_level)s, %(manager_level_2)s,
            %(manager_level_3)s, %(manager_level_4)s, %(manager_level_5)s, %(manager_level_6)s, %(manager_level_7)s,
            %(manager_level_8)s, %(manager_name)s, %(modified_at)s, %(organization)s, %(preferred_language)s,
            %(role_content)s, %(role_learning)s, %(sso_id)s, %(sub_function)s, %(subregion)s, %(time_in_job_profile)s,
            %(time_zone)s, %(title)s, %(user_name)s, %(user_type)s, %(worker_status)s, false, true
        ) on conflict (id) do update set
            active = excluded.active, biography = excluded.biography, cost_center = excluded.cost_center,
            cost_center_ent = excluded.cost_center_ent, country = excluded.country, created_at = excluded.created_at,
            created_by = excluded.created_by, creator_type = excluded.creator_type,
            deactivated_at = excluded.deactivated_at, department = excluded.department,
            direct_reports_with_cntrcts = excluded.direct_reports_with_cntrcts,
            direct_reports_without_cntrcts = excluded.direct_reports_without_cntrcts, email_work = excluded.email_work,
            employee_id = excluded.employee_id, external_id = excluded.external_id, family_name = excluded.family_name,
            function = excluded.function, function_hierarchy = excluded.function_hierarchy,
            given_name = excluded.given_name, hire_date = excluded.hire_date, job_family = excluded.job_family,
            job_profile = excluded.job_profile, length_of_service = excluded.length_of_service,
            location = excluded.location, management_level = excluded.management_level,
            manager_level_2 = excluded.manager_level_2, manager_level_3 = excluded.manager_level_3,
            manager_level_4 = excluded.manager_level_4, manager_level_5 = excluded.manager_level_5,
            manager_level_6 = excluded.manager_level_6, manager_level_7 = excluded.manager_level_7,
            manager_level_8 = excluded.manager_level_8, manager_name = excluded.manager_name,
            modified_at = excluded.modified_at, organization = excluded.organization,
            preferred_language = excluded.preferred_language, role_content = excluded.role_content,
            role_learning = excluded.role_learning, sso_id = excluded.sso_id, sub_function = excluded.sub_function,
            subregion = excluded.subregion, time_in_job_profile = excluded.time_in_job_profile,
            time_zone = excluded.time_zone, title = excluded.title, user_name = excluded.user_name,
            user_type = excluded.user_type, worker_status = excluded.worker_status, _deleted = false, _synced = true
    '''
    plural = 's'
    if len(records) == 1:
        plural = ''
    log.info(f'Saving {len(records)} user{plural} to database')
    psycopg2.extras.execute_batch(cur, sql, records)


def main_job(repeat_interval_hours: int = None):
    start = time.monotonic()
    log.info('Running the main job')

    cnx = psycopg2.connect(os.getenv('DB'), cursor_factory=psycopg2.extras.DictCursor)

    client_id = uuid.UUID(hex=os.getenv('CLIENT_ID'))
    client_secret = uuid.UUID(hex=os.getenv('CLIENT_SECRET'))
    user_id = uuid.UUID(hex=os.getenv('USER_ID'))
    c = seismic.SeismicClient(client_id, client_secret, os.getenv('TENANT'), user_id)

    records = []
    for u in c.scim_users():
        enterprise_user = u.get('urn:ietf:params:scim:schemas:extension:enterprise:2.0:User')
        extended_props = u.get('urn:ietf:params:scim:schemas:extension:seismic:2.0:UserExtendedProperty')
        meta = u.get('meta')
        name = u.get('name', {})
        seismic_user = u.get('urn:ietf:params:scim:schemas:extension:seismic:2.0:User')
        record = {
            'active': u.get('active'),
            'biography': extended_props.get('biography'),
            'cost_center': seismic_user.get('Cost_Center'),
            'cost_center_ent': enterprise_user.get('costCenter'),
            'country': seismic_user.get('Country'),
            'created_at': meta.get('created'),
            'created_by': extended_props.get('createdBy'),
            'creator_type': extended_props.get('creatorType'),
            'deactivated_at': extended_props.get('deactivatedTime'),
            'department': enterprise_user.get('department'),
            'direct_reports_with_cntrcts': seismic_user.get('Direct_Reports_With_Cntrcts'),
            'direct_reports_without_cntrcts': seismic_user.get('Direct_Reports_Without_Cntrcts'),
            'email_work': None,
            'employee_id': seismic_user.get('Employee_ID'),
            'external_id': u.get('externalId'),
            'family_name': name.get('familyName'),
            'function': seismic_user.get('Function'),
            'function_hierarchy': seismic_user.get('Function_Hierarchy'),
            'given_name': name.get('givenName'),
            'hire_date': extended_props.get('hireDate'),
            'id': u.get('id'),
            'job_family': seismic_user.get('Job_Family'),
            'job_profile': seismic_user.get('Job_Profile'),
            'length_of_service': seismic_user.get('Length_Of_Service'),
            'location': extended_props.get('location'),
            'management_level': seismic_user.get('Management_Level'),
            'manager_level_2': seismic_user.get('Manager_Level_2'),
            'manager_level_3': seismic_user.get('Manager_Level_3'),
            'manager_level_4': seismic_user.get('Manager_Level_4'),
            'manager_level_5': seismic_user.get('Manager_Level_5'),
            'manager_level_6': seismic_user.get('Manager_Level_6'),
            'manager_level_7': seismic_user.get('Manager_Level_7'),
            'manager_level_8': seismic_user.get('Manager_Level_8'),
            'manager_name': extended_props.get('managerName'),
            'modified_at': meta.get('lastModified'),
            'organization': enterprise_user.get('organization'),
            'preferred_language': u.get('preferredLanguage'),
            'role_content': None,
            'role_learning': None,
            'sso_id': extended_props.get('ssoId'),
            'sub_function': seismic_user.get('Sub_Function'),
            'subregion': seismic_user.get('Subregion'),
            'time_in_job_profile': seismic_user.get('Time_In_Job_Profile'),
            'time_zone': u.get('timezone'),
            'title': u.get('title'),
            'user_name': u.get('userName'),
            'user_type': u.get('userType'),
            'worker_status': seismic_user.get('Worker_Status'),
        }
        for e in u.get('emails'):
            if e.get('type') == 'work':
                record.update({'email_work': e.get('value')})
        for r in u.get('roles'):
            if r.get('value') in ('Business', 'Partner', 'Premium'):
                record.update({'role_content': r.get('value')})
            else:
                record.update({'role_learning': r.get('value')})
        records.append(record)

    with cnx:
        with cnx.cursor() as cur:
            _sync_prepare(cur)
            batch_upsert_users(cur, records)
            _sync_cleanup(cur)

    if repeat_interval_hours:
        plural = 's'
        if repeat_interval_hours == 1:
            plural = ''
        repeat_message = f'see you again in {repeat_interval_hours} hour{plural}'
    else:
        repeat_message = 'quitting'
    duration = int(time.monotonic() - start)
    log.info(f'Main job complete in {datime.pretty_duration_short(duration)}, {repeat_message}')


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
