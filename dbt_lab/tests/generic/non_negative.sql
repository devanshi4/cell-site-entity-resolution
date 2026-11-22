{#- A GENERIC test: a parameterised query you attach to any column in YAML.
    A test PASSES when the query returns ZERO rows (it selects the "bad" rows). -#}
{% test non_negative(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
