{% macro list_entities() %}
  
  {% set fs_config = var('feature_store', {}) %}
  {# Use target.database directly since var contains unrendered Jinja template #}
  {% set fs_database = target.database %}
  {% set fs_schema = fs_config.get('schema') %}
  
  {# Use SHOW TAGS to match snowflake-ml-python behavior: _find_object("TAGS", ...) #}
  {% set query %}
    SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_%' IN SCHEMA {{ fs_database }}.{{ fs_schema }}
  {% endset %}
  
  {% set results = run_query(query) %}
  
  {% if execute %}
    {{ log("", info=True) }}
    {{ log("========================================", info=True) }}
    {{ log("Registered Entities in Feature Store", info=True) }}
    {{ log("========================================", info=True) }}
    
    {% if results | length == 0 %}
      {{ log("No entities found. Create one with 'entity' materialization.", info=True) }}
    {% else %}
      {% for row in results %}
        {# SHOW TAGS returns: created_on, name, database_name, schema_name, owner, comment, allowed_values, ... #}
        {% set entity_name = row[1].replace('SNOWML_FEATURE_STORE_ENTITY_', '') %}
        {% set join_keys = row[6] %}
        {% set description = row[5] %}
        {{ log("  • " ~ entity_name, info=True) }}
        {{ log("    Join Keys: " ~ join_keys, info=True) }}
        {% if description %}
        {{ log("    Description: " ~ description, info=True) }}
        {% endif %}
        {{ log("", info=True) }}
      {% endfor %}
      
      {{ log("Total: " ~ results | length ~ " entities", info=True) }}
    {% endif %}
    
    {{ log("========================================", info=True) }}
  {% endif %}
  
{% endmacro %}

