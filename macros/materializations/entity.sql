{% materialization entity, default %}
  
  {%- set entity_name = model.name -%}
  {%- set join_keys = config.get('join_keys', []) -%}
  {%- set desc = config.get('desc', '') -%}
  
  {%- if not join_keys -%}
    {{ exceptions.raise_compiler_error("Entity '" ~ entity_name ~ "' requires 'join_keys' config") }}
  {%- endif -%}
  
  {%- set fs_config = var('feature_store', {}) -%}
  {%- if not fs_config -%}
    {{ exceptions.raise_compiler_error("Feature Store configuration not found. Please set vars.feature_store in dbt_project.yml") }}
  {%- endif -%}
  
  {%- set fs_database = target.database -%}
  {%- set fs_schema = fs_config.get('schema') -%}
  
  {%- if not fs_database or not fs_schema -%}
    {{ exceptions.raise_compiler_error("Feature Store requires 'database' and 'schema' in vars.feature_store") }}
  {%- endif -%}
  
  {%- set tag_name = fs_database ~ '.' ~ fs_schema ~ '.SNOWML_FEATURE_STORE_ENTITY_' ~ entity_name | upper -%}
  {%- set join_keys_upper = [] -%}
  {%- for key in join_keys -%}
    {%- do join_keys_upper.append(key | upper) -%}
  {%- endfor -%}
  {%- set join_keys_str = join_keys_upper | join(',') -%}
  
  {% call statement('main') -%}
    CREATE TAG IF NOT EXISTS {{ tag_name }}
      ALLOWED_VALUES '{{ join_keys_str }}'
      COMMENT = '{{ desc }}'
  {%- endcall %}
  
  {{ log("✅ Registered entity: " ~ entity_name ~ " with join_keys: " ~ join_keys | join(', '), info=True) }}
  {{ return({'relations': []}) }}
  
{% endmaterialization %}

