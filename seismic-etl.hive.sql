create table datalake.seismic_user_activity (
    id string,
    action string,
    action_type string,
    occurred_at timestamp,
    modified_at timestamp,
    user_id string,
    user_username string,
    instance_name string,
    instance_format string,
    is_bound_delivery boolean,
    total_pages int,
    content_id string,
    library_content_id string,
    library_content_version_id string,
    workspace_content_id string,
    workspace_content_version_id string,
    livesend_link_id string,
    livesend_link_content_id string,
    generated_live_doc_id string,
    content_profile_id string,
    context_id string,
    context_name string,
    context_type string,
    context_system_type string
) stored as orc;

create table datalake.seismic_search_history (
    id string,
    search_cycle_id string,
    search_term_raw string,
    search_term_normalized string,
    search_type string,
    occurred_at timestamp,
    modified_at timestamp,
    user_id string,
    result_count int,
    result_count_doc_center int,
    result_count_news_center int,
    result_count_workspace int,
    result_count_control_center int,
    result_count_content_manager int,
    sort_by string
) stored as orc;

create table datalake.seismic_livesend_links (
    id string,
    is_separate_send boolean,
    notification_mode string,
    allow_download boolean,
    expire_after_view boolean,
    expires_at timestamp,
    has_password boolean,
    created_at timestamp,
    modified_at timestamp,
    created_by string,
    url_path string
) stored as orc;

create table datalake.seismic_livesend_link_contents (
    id string,
    created_at timestamp,
    created_by string,
    livesend_link_id string,
    name string,
    modified_at timestamp,
    total_pages int
) stored as orc;

create table datalake.seismic_livesend_link_members (
    livesend_link_id string,
    external_user_id string,
    modified_at timestamp,
    created_at timestamp,
    email string
) stored as orc;

create table datalake.seismic_livesend_page_views (
    id string,
    livesend_link_content_id string,
    livesend_viewing_session_id string,
    occurred_at timestamp,
    modified_at timestamp,
    session_started_at timestamp,
    session_ended_at timestamp,
    duration int,
    page_index int
) stored as orc;

create table datalake.seismic_contents (
    id string,
    context_id string,
    context_name string,
    context_type string,
    context_system_type string,
    created_at timestamp,
    created_by string,
    doc_center_url string,
    format string,
    is_cart_content boolean,
    is_contextual_folder_content boolean,
    is_deleted boolean,
    is_published boolean,
    latest_library_content_version_created_at timestamp,
    latest_library_content_version_id string,
    latest_library_content_version_size int,
    latest_workspace_content_version_created_at timestamp,
    latest_workspace_content_version_id string,
    library_content_id string,
    library_url string,
    materialized_path string,
    modified_at timestamp,
    name string,
    news_center_url string,
    origin_content_profile_id string,
    owner_id string,
    preview_image_id string,
    published_version_expires_at timestamp,
    teamsite_id string,
    thumbnail_image_id string,
    type string,
    version string
) stored as orc;

create table datalake.seismic_users (
    id string,
    created_at timestamp,
    default_content_profile_id string,
    default_content_profile_name string,
    deleted_at timestamp,
    email string,
    email_domain string,
    first_name string,
    full_name string,
    is_deleted boolean,
    is_seismic_employee boolean,
    is_system_admin boolean,
    last_name string,
    license_type string,
    modified_at timestamp,
    organization string,
    sso_user_id string,
    title string,
    username string
) stored as orc;

create table datalake.seismic_user_property_assignments (
    user_id string,
    user_property_id string,
    user_property_name string,
    user_property_value string
) stored as orc;

create table datalake.seismic_group_members (
    group_id string,
    group_name string,
    user_id string
) stored as orc;

create table datalake.seismic_content_property_assignments (
    content_property_id string,
    content_property_name string,
    content_property_type string,
    content_property_value string,
    library_content_id string
) stored as orc;

create table datalake.seismic_generated_live_docs (
    id string,
    created_at timestamp,
    library_content_id string,
    library_content_version_id string,
    modified_at timestamp,
    preview_image_id string,
    thumbnail_image_id string,
    user_id string
) stored as orc;

create table datalake.seismic_generated_live_doc_components (
    id string,
    created_at timestamp,
    format string,
    generated_live_doc_id string,
    insert_method string,
    insert_status string,
    library_content_id string,
    library_content_name string,
    library_content_version_id string,
    modified_at timestamp,
    parent_component_id string,
    thumbnail_image_id string
) stored as orc;

create table datalake.seismic_generated_live_doc_slides (
    id string,
    generated_live_doc_id string,
    modified_at timestamp,
    page_short_id string,
    parent_component_id string,
    position int,
    title string
) stored as orc;

create table datalake.seismic_generated_live_doc_fields (
    id string,
    field_name string,
    field_type string,
    field_value string,
    generated_live_doc_id string,
    modified_at timestamp
) stored as orc;

create table datalake.seismic_generated_live_doc_output_formats (
    id string,
    compatibility string,
    file_name string,
    format string,
    generated_live_doc_id string,
    has_clear_notes boolean,
    has_open_password boolean,
    has_owner_options boolean,
    has_owner_password boolean,
    image_dpi int,
    modified_at timestamp,
    name string,
    output_layout string
) stored as orc;
