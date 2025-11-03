{% macro init_feature_store() %}
  
  {% set fs_config = var('feature_store', {}) %}
  
  {% if not fs_config %}
    {{ exceptions.raise_compiler_error("Feature Store configuration not found. Please set vars.feature_store in dbt_project.yml") }}
  {% endif %}
  
  {# Use target.database directly since var contains unrendered Jinja template #}
  {% set database = target.database %}
  {% set schema = fs_config.get('schema') %}
  
  {% if not database or not schema %}
    {{ exceptions.raise_compiler_error("Feature Store requires 'database' and 'schema' in vars.feature_store") }}
  {% endif %}
  
  {{ log("Initializing Feature Store at " ~ database ~ "." ~ schema ~ "...", info=True) }}
  
  -- Create schema if not exists (matches Python API)
  {% set create_schema_sql %}
    CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }}
  {% endset %}
  
  {% do run_query(create_schema_sql) %}
  
  -- Create TAG definitions (matches Python API - does NOT set tags on schema)
  {% set create_tags_sql %}
    CREATE TAG IF NOT EXISTS {{ database }}.{{ schema }}.SNOWML_FEATURE_STORE_OBJECT;
    CREATE TAG IF NOT EXISTS {{ database }}.{{ schema }}.SNOWML_FEATURE_VIEW_METADATA;
  {% endset %}
  
  {% do run_query(create_tags_sql) %}
  
  {{ log("✅ Feature Store initialized successfully at " ~ database ~ "." ~ schema, info=True) }}
  {{ log("   Created schema and TAG definitions (no tags set on schema)", info=True) }}
  
{% endmacro %}

