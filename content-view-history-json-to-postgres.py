import json
import os
import psycopg2.extras


def write_records(_cnx, records):
    sql = '''
        insert into seismic_user_activity (
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
    with _cnx.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records)


DB = os.getenv('DB')
cnx = psycopg2.connect(DB)

# SOURCE_FILE is the path to a file that contains the json response from a call to the contentViewHistory API endpoint
SOURCE_FILE = os.getenv('SOURCE_FILE')
with open(SOURCE_FILE) as f:
    data = json.load(f)
    write_records(cnx, data)

cnx.commit()
cnx.close()
