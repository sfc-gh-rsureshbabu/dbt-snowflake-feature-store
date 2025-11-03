{% materialization feature_view, adapter='snowflake' %}
  
  {%- set feature_view_name = model.name -%}
  {%- set entities = config.get('entities', []) -%}
  {%- set feature_view_version = config.get('feature_view_version') -%}
  {%- set timestamp_col = config.get('timestamp_col', none) -%}
  {%- set refresh_freq = config.get('refresh_freq', none) -%}
  {%- set warehouse = config.get('warehouse', none) -%}
  {%- set initialize = config.get('initialize', 'ON_CREATE') -%}
  {%- set refresh_mode = config.get('refresh_mode', 'AUTO') -%}
  {%- set desc = config.get('desc', '') -%}
  {%- set on_configuration_change = config.get('on_configuration_change', 'apply') -%}
  
  {%- if not entities -%}
    {{ exceptions.raise_compiler_error("Feature View '" ~ feature_view_name ~ "' requires 'entities' config") }}
  {%- endif -%}
  
  {%- if not feature_view_version -%}
    {{ exceptions.raise_compiler_error("Feature View '" ~ feature_view_name ~ "' requires 'feature_view_version' config") }}
  {%- endif -%}
  
  {%- set is_dynamic_table = refresh_freq and refresh_freq != 'none' -%}
  
  {%- if is_dynamic_table and not warehouse -%}
    {{ exceptions.raise_compiler_error("Dynamic Table Feature View '" ~ feature_view_name ~ "' requires 'warehouse' config") }}
  {%- endif -%}
  
  {%- set fs_config = var('feature_store', {}) -%}
  {%- set fs_database = target.database -%}
  {%- set fs_schema = fs_config.get('schema') -%}
  
  {%- if not fs_database or not fs_schema -%}
    {{ exceptions.raise_compiler_error("Feature Store requires 'database' and 'schema' in vars.feature_store") }}
  {%- endif -%}
  
  {%- set version_safe = feature_view_version | replace('.', '_') -%}
  {%- set physical_name = feature_view_name ~ '$' ~ version_safe -%}
  
  {%- set target_relation = api.Relation.create(
        database=fs_database,
        schema=fs_schema,
        identifier=physical_name,
        type='view') -%}
  
  {{ validate_entities_exist(entities) }}
  
  {% call statement('create_tags') -%}
    CREATE TAG IF NOT EXISTS {{ fs_database }}.{{ fs_schema }}.SNOWML_FEATURE_STORE_OBJECT;
    CREATE TAG IF NOT EXISTS {{ fs_database }}.{{ fs_schema }}.SNOWML_FEATURE_VIEW_METADATA;
    {%- for entity in entities -%}
    CREATE TAG IF NOT EXISTS {{ fs_database }}.{{ fs_schema }}.SNOWML_FEATURE_STORE_ENTITY_{{ entity | upper }};
    {%- endfor %}
  {%- endcall %}
  
  {%- set entity_list = [] -%}
  {%- for entity in entities -%}
    {%- set entity_tag_query -%}
      SHOW TAGS LIKE 'SNOWML_FEATURE_STORE_ENTITY_{{ entity | upper }}' IN SCHEMA {{ fs_database }}.{{ fs_schema }}
    {%- endset -%}
    {%- set entity_tag_result = run_query(entity_tag_query) -%}
    {%- if execute and entity_tag_result and entity_tag_result | length > 0 -%}
      {%- set allowed_values_json = entity_tag_result[0][6] -%}
      {%- set join_keys_list = fromjson(allowed_values_json) -%}
      {%- set entity_obj = {
        'name': entity | upper,
        'joinKeys': join_keys_list
      } -%}
      {%- do entity_list.append(entity_obj) -%}
    {%- endif -%}
  {%- endfor -%}
  
  {%- set fv_metadata = {
    'entities': entity_list,
    'timestamp_col': timestamp_col if timestamp_col else 'NULL'
  } -%}
  
  {%- set fs_object_info = {
    'type': 'MANAGED_FEATURE_VIEW' if is_dynamic_table else 'EXTERNAL_FEATURE_VIEW',
    'pkg_version': var('dbt_snowflake_feature_store', {}).get('package_version', '1.0.0')
  } -%}
  
  {%- set tag_lines = [] -%}
  {%- do tag_lines.append("SNOWML_FEATURE_STORE_OBJECT = '" ~ (fs_object_info | tojson) ~ "'") -%}
  {%- do tag_lines.append("SNOWML_FEATURE_VIEW_METADATA = '" ~ (fv_metadata | tojson) ~ "'") -%}
  {%- for entity_obj in entity_list -%}
    {%- set join_keys_str = entity_obj['joinKeys'] | join(',') -%}
    {%- do tag_lines.append("SNOWML_FEATURE_STORE_ENTITY_" ~ entity_obj['name'] ~ " = '" ~ join_keys_str ~ "'") -%}
  {%- endfor -%}
  {%- set tag_clause = "TAG (\n    " ~ tag_lines | join(",\n    ") ~ "\n  )" -%}
  
  {{ run_hooks(pre_hooks) }}
  
  {% if is_dynamic_table %}
    {%- set full_refresh_mode = flags.FULL_REFRESH -%}
    {%- set existing_relation = load_cached_relation(target_relation) -%}
    {%- set dt_exists = existing_relation is not none -%}
    
    {%- if dt_exists -%}
      {%- set dt_config = adapter.describe_dynamic_table(target_relation) -%}
      {%- set current_lag = dt_config.target_lag if dt_config else none -%}
      {%- set current_wh = dt_config.warehouse if dt_config else none -%}
    {%- else -%}
      {%- set current_lag = none -%}
      {%- set current_wh = none -%}
    {%- endif -%}
    
    {% if full_refresh_mode or not dt_exists %}
      {% call statement('main') -%}
        CREATE OR REPLACE DYNAMIC TABLE {{ target_relation }}
        {%- if desc %}
        COMMENT = '{{ desc }}'
        {% endif %}TAG (
          {%- for line in tag_lines %}
          {{ line }}{{ "," if not loop.last }}
          {%- endfor %}
        )
        target_lag = '{{ refresh_freq }}' refresh_mode = {{ refresh_mode }} initialize = {{ initialize }} warehouse = {{ warehouse }} AS (
          {{ sql }}
        )
      {%- endcall %}
      {{ log("✅ " ~ ("Recreated" if dt_exists else "Created") ~ " Dynamic Table " ~ target_relation, info=True) }}
      
    {% else %}
      {%- set config_changed = false -%}
      {%- set changes = [] -%}
      
      {% if current_lag != refresh_freq %}
        {%- set config_changed = true -%}
        {%- do changes.append('TARGET_LAG: ' ~ current_lag ~ ' → ' ~ refresh_freq) -%}
      {% endif %}
      
      {% if current_wh != warehouse | upper %}
        {%- set config_changed = true -%}
        {%- do changes.append('WAREHOUSE: ' ~ current_wh ~ ' → ' ~ warehouse) -%}
      {% endif %}
      
      {% if config_changed %}
        {% if on_configuration_change == 'apply' %}
          {{ log("Configuration changes detected: " ~ changes | join(', '), info=True) }}
          
          {% if current_lag != refresh_freq %}
            {% call statement('main') -%}
              ALTER DYNAMIC TABLE {{ target_relation }} 
                SET TARGET_LAG = '{{ refresh_freq }}'
            {%- endcall %}
            {{ log("✅ Applied TARGET_LAG change", info=True) }}
          {% endif %}
          
          {% if current_wh != warehouse | upper %}
            {% call statement('alter_warehouse') -%}
              ALTER DYNAMIC TABLE {{ target_relation }} 
                SET WAREHOUSE = {{ warehouse }}
            {%- endcall %}
            {{ log("✅ Applied WAREHOUSE change", info=True) }}
          {% endif %}
          
        {% elif on_configuration_change == 'continue' %}
          {% call statement('main') -%}
            SELECT 'Skipping changes' AS status
          {%- endcall %}
          {{ exceptions.warn("Configuration changes detected but on_configuration_change='continue'. Changes: " ~ changes | join(', ')) }}
          {{ log("⚠️  Skipping configuration changes", info=True) }}
          
        {% elif on_configuration_change == 'fail' %}
          {{ exceptions.raise_compiler_error("Configuration changes detected and on_configuration_change='fail'. Changes: " ~ changes | join(', ') ~ ". Use --full-refresh to apply.") }}
        {% endif %}
      {% else %}
        {% call statement('main') -%}
          SELECT 'No changes' AS status
        {%- endcall %}
        {{ log("ℹ️  Dynamic Table exists, no configuration changes detected", info=True) }}
      {% endif %}
    {% endif %}
    
  {% else %}
    {% call statement('main') -%}
      CREATE OR REPLACE VIEW {{ target_relation }}
      COPY GRANTS
      {%- if desc %}
      COMMENT = '{{ desc }}'
      {%- endif %}
      {{ tag_clause }}
      AS
      {{ sql }}
    {%- endcall %}
    {{ log("✅ Created feature view " ~ target_relation, info=True) }}
  {% endif %}
  
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
  
{% endmaterialization %}

