/*
 * Logging Hooks for dbt - Werfen Data Pipeline
 * ============================================
 * 
 * Macros for integrating structured logging in dbt transformations
 * Compatible with pre-hook and post-hook for automatic tracking
 * 
 * Author: Lead Software Architect
 * Date: January 2025
 */

{% macro log_model_start() %}
  {{ log_info("Model execution started: " ~ this.name) }}
  
  -- In AWS, this would be CloudWatch custom metrics
  {% set start_time = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') %}
  
  {% set log_entry %}
  {
    "timestamp": "{{ start_time }}",
    "service": "werfen-dbt",
    "model_name": "{{ this.name }}",
    "event_type": "model_start",
    "correlation_id": "{{ var('pipeline_run_id', 'unknown') }}",
    "layer": "{{ this.schema }}",
    "materialization": "{{ config.get('materialized', 'view') }}"
  }
  {% endset %}
  
  {{ log_info("DBT_LOG: " ~ log_entry) }}
{% endmacro %}

{% macro log_model_success(rows_affected=none) %}
  {% set end_time = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') %}
  
  {% set log_entry %}
  {
    "timestamp": "{{ end_time }}",
    "service": "werfen-dbt", 
    "model_name": "{{ this.name }}",
    "event_type": "model_success",
    "correlation_id": "{{ var('pipeline_run_id', 'unknown') }}",
    "layer": "{{ this.schema }}",
    "rows_affected": {{ rows_affected or 0 }},
    "materialization": "{{ config.get('materialized', 'view') }}"
  }
  {% endset %}
  
  {{ log_info("DBT_LOG: " ~ log_entry) }}
{% endmacro %}

{% macro log_test_execution(test_name, status, details=none) %}
  {% set timestamp = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') %}
  
  {% set log_entry %}
  {
    "timestamp": "{{ timestamp }}",
    "service": "werfen-dbt",
    "test_name": "{{ test_name }}",
    "status": "{{ status }}",
    "event_type": "test_execution",
    "correlation_id": "{{ var('pipeline_run_id', 'unknown') }}",
    "details": {{ details | tojson if details else '{}' }}
  }
  {% endset %}
  
  {{ log_info("DBT_LOG: " ~ log_entry) }}
{% endmacro %}

{% macro log_quality_metric(metric_name, metric_value, model_name=none) %}
  {% set timestamp = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') %}
  
  {% set log_entry %}
  {
    "timestamp": "{{ timestamp }}",
    "service": "werfen-dbt",
    "metric_name": "{{ metric_name }}",
    "metric_value": {{ metric_value }},
    "model_name": "{{ model_name or this.name }}",
    "event_type": "quality_metric",
    "correlation_id": "{{ var('pipeline_run_id', 'unknown') }}"
  }
  {% endset %}
  
  {{ log_info("DBT_LOG: " ~ log_entry) }}
{% endmacro %}

/*
 * AWS CloudWatch Integration Example:
 * 
 * In AWS, these logs would be automatically sent to CloudWatch Logs
 * using CloudWatch agent or direct integrations:
 * 
 * aws logs put-log-events \
 *   --log-group-name "/werfen/dbt-pipeline" \
 *   --log-stream-name "transformations" \
 *   --log-events "timestamp=$(date +%s)000,message='$log_entry'"
 */ 