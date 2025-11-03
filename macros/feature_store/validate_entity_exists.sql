{% macro validate_entities_exist(entities) %}
  
  {%- set fs_config = var('feature_store', {}) -%}
  {# Use target.database directly since var contains unrendered Jinja template #}
  {%- set fs_database = target.database -%}
  {%- set fs_schema = fs_config.get('schema') -%}
  
  {%- for entity in entities -%}
    {%- set tag_name = 'SNOWML_FEATURE_STORE_ENTITY_' ~ entity | upper -%}
    
    {# Check if TAG definition exists using SHOW TAGS (like snowflake-ml-python) #}
    {# This matches: _find_object("TAGS", tag_name) #}
    {%- set query -%}
      SHOW TAGS LIKE '{{ tag_name }}' IN SCHEMA {{ fs_database }}.{{ fs_schema }}
    {%- endset -%}
    
    {%- set result = run_query(query) -%}
    
    {# SHOW TAGS returns empty result if tag doesn't exist #}
    {%- if execute and result -%}
      {%- if result | length == 0 -%}
        {{ exceptions.raise_compiler_error(
          "Entity '" ~ entity ~ "' not found in Feature Store. " ~
          "Please create it first with 'entity' materialization."
        ) }}
      {%- endif -%}
    {%- endif -%}
    
  {%- endfor -%}
  
{% endmacro %}

