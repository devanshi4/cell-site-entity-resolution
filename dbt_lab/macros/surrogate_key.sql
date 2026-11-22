{#-
  A macro is a reusable Jinja function that RETURNS SQL TEXT.
  This mimics dbt_utils.generate_surrogate_key (in a company you would just install dbt_utils
  via packages.yml + `dbt deps`; it is written by hand here so you can see there is no magic).

  Usage:   {{ surrogate_key(['customer_id', 'valid_from']) }}
  Renders: md5(coalesce(cast(customer_id as varchar), '_null_') || '-' || coalesce(cast(valid_from as varchar), '_null_'))
-#}
{% macro surrogate_key(columns) -%}
    md5(
    {%- for col in columns %}
        coalesce(cast({{ col }} as varchar), '_null_')
        {%- if not loop.last %} || '-' || {% endif %}
    {%- endfor %}
    )
{%- endmacro %}
