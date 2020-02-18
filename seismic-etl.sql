create schema seismic;

create table seismic.user_activity (
    id uuid primary key,
    action text,
    action_type text,
    occurred_at timestamp,
    modified_at timestamp,
    user_id uuid,
    user_username text,
    instance_name text,
    instance_format text,
    is_bound_delivery boolean,
    total_pages integer,
    content_id uuid,
    library_content_id uuid,
    library_content_version_id uuid,
    workspace_content_id uuid,
    workspace_content_version_id uuid,
    livesend_link_id uuid,
    livesend_link_content_id uuid,
    generated_live_doc_id uuid,
    content_profile_id uuid,
    context_id uuid,
    context_name text,
    context_type text,
    context_system_type text
);

create table seismic.search_history (
    id uuid primary key,
    search_cycle_id uuid,
    search_term_raw text,
    search_term_normalized text,
    search_type text,
    occurred_at timestamp,
    modified_at timestamp,
    user_id uuid,
    result_count integer,
    result_count_doc_center integer,
    result_count_news_center integer,
    result_count_workspace integer,
    result_count_control_center integer,
    result_count_content_manager integer,
    sort_by text
);

create table seismic.livesend_links (
    id uuid primary key,
    is_separate_send boolean,
    notification_mode text,
    allow_download boolean,
    expire_after_view boolean,
    expires_at timestamp,
    has_password boolean,
    created_at timestamp,
    modified_at timestamp,
    created_by uuid,
    url_path text
);

create table seismic.livesend_link_contents (
    id uuid primary key,
    created_at timestamp,
    created_by uuid,
    livesend_link_id uuid,
    name text,
    modified_at timestamp,
    total_pages integer
);

create table seismic.livesend_link_members (
    livesend_link_id uuid,
    external_user_id uuid,
    modified_at timestamp,
    created_at timestamp,
    email text
);

create table seismic.livesend_page_views (
    id uuid primary key,
    livesend_link_content_id uuid,
    livesend_viewing_session_id uuid,
    occurred_at timestamp,
    modified_at timestamp,
    session_started_at timestamp,
    session_ended_at timestamp,
    duration integer,
    page_index integer
);

create table seismic.contents (
    id uuid primary key,
    context_id uuid,
    context_name text,
    context_system_type text,
    created_at timestamp,
    created_by uuid,
    doc_center_url text,
    format text,
    is_cart_content boolean,
    is_contextual_folder_content boolean,
    is_deleted boolean,
    is_published boolean,
    latest_library_content_version_created_at timestamp,
    latest_library_content_version_id uuid,
    latest_library_content_version_size integer,
    latest_workspace_content_version_created_at timestamp,
    latest_workspace_content_version_id uuid,
    library_content_id uuid,
    library_url text,
    materialized_path text,
    modified_at timestamp,
    name text,
    news_center_url text,
    origin_content_profile_id uuid,
    owner_id uuid,
    preview_image_id uuid,
    published_version_expires_at timestamp,
    teamsite_id text,
    thumbnail_image_id uuid,
    type text,
    version text
);

create table seismic.users (
    id uuid primary key,
    created_at timestamp,
    default_content_profile_id uuid,
    default_content_profile_name text,
    deleted_at timestamp,
    email text,
    email_domain text,
    first_name text,
    full_name text,
    is_deleted boolean,
    is_seismic_employee boolean,
    is_system_admin boolean,
    last_name text,
    license_type text,
    modified_at timestamp,
    organization text,
    sso_user_id text,
    title text,
    username text
);

create table seismic.user_property_assignments (
    user_id uuid,
    user_property_id uuid,
    user_property_name text,
    user_property_value text
);

create table seismic.group_members (
    group_id uuid,
    group_name text,
    user_id uuid
);

create table seismic.content_property_assignments (
    content_property_id uuid,
    content_property_name text,
    content_property_type text,
    content_property_value text,
    library_content_id uuid
);

create table seismic.generated_live_docs (
    id uuid primary key,
    created_at timestamp,
    library_content_id uuid,
    library_content_version_id uuid,
    modified_at timestamp,
    preview_image_id uuid,
    thumbnail_image_id uuid,
    user_id uuid
);

create table seismic.generated_live_doc_components (
    id uuid primary key,
    created_at timestamp,
    format text,
    generated_live_doc_id uuid,
    insert_method text,
    insert_status text,
    library_content_id uuid,
    library_content_name text,
    library_content_version_id uuid,
    modified_at timestamp,
    parent_component_id uuid,
    thumbnail_image_id uuid
);

create table seismic.generated_live_doc_slides (
    id uuid primary key,
    generated_live_doc_id uuid,
    modified_at timestamp,
    page_short_id text,
    parent_component_id uuid,
    position integer,
    title text
);

create table seismic.generated_live_doc_fields (
    id uuid primary key,
    field_name text,
    field_type text,
    field_value text,
    generated_live_doc_id uuid,
    modified_at timestamp
);

create table seismic.generated_live_doc_output_formats (
    id uuid primary key,
    compatibility text,
    file_name text,
    format text,
    generated_live_doc_id uuid,
    has_clear_notes boolean,
    has_open_password boolean,
    has_owner_options boolean,
    has_owner_password boolean,
    image_dpi integer,
    modified_at timestamp,
    name text,
    output_layout text
);
