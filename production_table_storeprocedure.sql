CREATE OR REPLACE PROCEDURE creating_production_table_initial_load(OUT table_created BOOLEAN)
LANGUAGE plpgsql
AS $$
BEGIN
    table_created := FALSE;  -- Initialize the output parameter

    -- Drop the table if it exists (optional)
    DROP TABLE IF EXISTS production_google_jobs_pages;

    -- Create the new table with data
    CREATE TABLE production_google_jobs_pages AS
    SELECT
        (TO_TIMESTAMP(search_metadata_created_at, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC') AS search_metadata_created_at,
        (TO_TIMESTAMP(search_metadata_processed_at, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC') AS search_metadata_processed_at,
        search_metadata_total_time_taken,
        jobs_results_detected_extensions_paid_time_off,
        jobs_results_detected_extensions_dental_coverage,
        jobs_results_detected_extensions_health_insurance,
        search_metadata_id,
        search_metadata_status,
        search_metadata_json_endpoint,
        search_metadata_google_jobs_url,
        search_metadata_raw_html_file,
        search_parameters_q,
        search_parameters_engine,
        search_parameters_google_domain,
        search_parameters_hl,
        serpapi_pagination_next_page_token,
        serpapi_pagination_next,
        filters_name,
        filters_link,
        filters_serpapi_link,
        filters_parameters_uds,
        filters_parameters_q,
        jobs_results_title,
        jobs_results_company_name,
        jobs_results_location,
        jobs_results_via,
        jobs_results_share_link,
        jobs_results_extensions,
        jobs_results_description,
        jobs_results_job_id,
        jobs_results_detected_extensions_posted_at,
        jobs_results_detected_extensions_schedule_type,
        jobs_results_detected_extensions_qualifications,
        jobs_results_thumbnail,
        jobs_results_detected_extensions_salary,
        filters_options_name,
        filters_options_link,
        filters_options_serpapi_link,
        filters_options_parameters_uds,
        filters_options_parameters_q,
        jobs_results_job_highlights_title,
        jobs_results_job_highlights_items,
        jobs_results_apply_options_title,
        jobs_results_apply_options_link	
    FROM google_jobs_page_stg;

    -- Set output parameter to true after table creation
    table_created := TRUE;
END;
$$;


