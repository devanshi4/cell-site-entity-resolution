{#- Telecom billing rule: every started minute is billed (61 s -> 2 minutes).
    Business logic lives in ONE place; if pricing changes the rule, you change it here. -#}
{% macro billable_minutes(duration_col) -%}
    cast(ceil({{ duration_col }} / 60.0) as integer)
{%- endmacro %}
