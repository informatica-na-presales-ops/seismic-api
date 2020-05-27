create schema seismic;

create table seismic.user_activity (
    id text primary key,
    action text,
    action_type text,
    occurred_at timestamp,
    modified_at timestamp,
    user_id text,
    user_username text,
    instance_name text,
    instance_format text,
    is_bound_delivery boolean,
    total_pages integer,
    content_id text,
    content_version_id text,
    library_content_id text,
    library_content_version_id text,
    workspace_content_id text,
    workspace_content_version_id text,
    livesend_link_id text,
    livesend_link_content_id text,
    generated_live_doc_id text,
    content_profile_id text,
    content_profile_name text,
    application text,
    product_area text,
    context_id text,
    context_name text,
    context_type text,
    context_system_type text
);

create table seismic.search_history (
    id text primary key,
    search_cycle_id text,
    search_term_raw text,
    search_term_normalized text,
    search_type text,
    occurred_at timestamp,
    modified_at timestamp,
    user_id text,
    result_count integer,
    result_count_doc_center integer,
    result_count_news_center integer,
    result_count_workspace integer,
    result_count_control_center integer,
    result_count_content_manager integer,
    sort_by text
);

create table seismic.livesend_links (
    id text primary key,
    is_separate_send boolean,
    notification_mode text,
    allow_download boolean,
    expire_after_view boolean,
    expires_at timestamp,
    has_password boolean,
    created_at timestamp,
    modified_at timestamp,
    created_by text,
    url_path text
);

create table seismic.livesend_link_contents (
    id text primary key,
    created_at timestamp,
    created_by text,
    livesend_link_id text,
    name text,
    modified_at timestamp,
    total_pages integer
);

create table seismic.livesend_link_members (
    livesend_link_id text,
    external_user_id text,
    modified_at timestamp,
    created_at timestamp,
    email text,
    primary key (livesend_link_id, external_user_id)
);

create table seismic.livesend_page_views (
    id text primary key,
    livesend_link_content_id text,
    livesend_viewing_session_id text,
    occurred_at timestamp,
    modified_at timestamp,
    session_started_at timestamp,
    session_ended_at timestamp,
    duration integer,
    page_index integer
);

create table seismic.contents (
    id text primary key,
    context_id text,
    context_name text,
    context_type text,
    context_system_type text,
    created_at timestamp,
    created_by text,
    doc_center_url text,
    format text,
    is_cart_content boolean,
    is_contextual_folder_content boolean,
    is_deleted boolean,
    is_published boolean,
    latest_library_content_version_created_at timestamp,
    latest_library_content_version_id text,
    latest_library_content_version_size integer,
    latest_workspace_content_version_created_at timestamp,
    latest_workspace_content_version_id text,
    library_content_id text,
    library_url text,
    materialized_path text,
    modified_at timestamp,
    name text,
    news_center_url text,
    origin_content_profile_id text,
    owner_id text,
    preview_image_id text,
    published_version_expires_at timestamp,
    teamsite_id text,
    thumbnail_image_id text,
    type text,
    version text
);

create table seismic.users (
    id text primary key,
    created_at timestamp,
    default_content_profile_id text,
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
    user_id text,
    user_property_id text,
    user_property_name text,
    user_property_value text,
    primary key (user_id, user_property_id)
);

create table seismic.group_members (
    group_id text,
    group_name text,
    user_id text
);

create table seismic.content_property_assignments (
    content_property_id text,
    content_property_name text,
    content_property_type text,
    content_property_value text,
    library_content_id text
);

create table seismic.generated_live_docs (
    id text primary key,
    created_at timestamp,
    library_content_id text,
    library_content_version_id text,
    modified_at timestamp,
    preview_image_id text,
    thumbnail_image_id text,
    user_id text
);

create table seismic.generated_live_doc_components (
    id text primary key,
    created_at timestamp,
    format text,
    generated_live_doc_id text,
    insert_method text,
    insert_status text,
    library_content_id text,
    library_content_name text,
    library_content_version_id text,
    modified_at timestamp,
    parent_component_id text,
    thumbnail_image_id text
);

create table seismic.generated_live_doc_slides (
    id text primary key,
    generated_live_doc_id text,
    modified_at timestamp,
    page_short_id text,
    parent_component_id text,
    position integer,
    title text
);

create table seismic.generated_live_doc_fields (
    id text primary key,
    field_name text,
    field_type text,
    field_value text,
    generated_live_doc_id text,
    modified_at timestamp
);

create table seismic.generated_live_doc_output_formats (
    id text primary key,
    compatibility text,
    file_name text,
    format text,
    generated_live_doc_id text,
    has_clear_notes boolean,
    has_open_password boolean,
    has_owner_options boolean,
    has_owner_password boolean,
    image_dpi integer,
    modified_at timestamp,
    name text,
    output_layout text
);
