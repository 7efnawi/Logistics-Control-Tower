{# Cross-adapter time metric helpers (works identically on DuckDB local and Synapse Dedicated Pool). #}

{% macro hours_between(start_ts, end_ts) %}
    (cast({{ dbt.datediff(start_ts, end_ts, 'second') }} as float) / 3600.0)
{% endmacro %}
