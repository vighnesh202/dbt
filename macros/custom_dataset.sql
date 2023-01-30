{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
{%- if node.resource_type == 'model' -%}
            {%- if node.name.startswith('recursive_') -%} 
                {{ 'dbt_workdb'}}
    {%- endif -%}
    {%- endif -%}
    {%- endif -%}
      {%- endmacro %}