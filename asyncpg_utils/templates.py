from jinja2 import Template

sql_create_template = Template(
    """
    INSERT INTO {{ table_name }}
    ({% for field_name in field_names %}{{ field_name }}{% if not loop.last %}, {% endif %}{% endfor %})
    VALUES
    ({% for field_name in field_names %}${{ loop.index }}{% if not loop.last %}, {% endif %}{% endfor %})
    RETURNING *
    """
)

sql_list_template = Template(
    """
    SELECT
    {% if count %}
        COUNT(1)
    {% else %}
        {% if not fields %}*{% else %}{% for field_name in fields %}{{ field_name }}{% if not loop.last %}, {% endif %}{% endfor %} {% endif %}
    {% endif %}
    FROM {{ table_name }}
    {% if joins %}
        {% for join_table, value in joins.items() %}
        {{ value['type'] }} {{ join_table }} ON {{ value['source'] }} = {{ value['target'] }}
        {% endfor %}
    {% endif %}
    {% if filters %}
        WHERE
        {% for field, value in filters.items() %}
            {% if value['lookup'] == 'exact' %}
            {{ field }} = ${{ loop.index }}
            {% elif value['lookup'] == 'like' %}
            {{ field }} LIKE ${{ loop.index }}
            {% elif value['lookup'] == 'ilike' %}
            {{ field }} ILIKE ${{ loop.index }}
            {% elif value['lookup'] == 'in' %}
            {{ field }} = any(${{ loop.index }})
            {% elif value['lookup'] == 'gt' %}
            {{ field }} > ${{ loop.index }}
            {% elif value['lookup'] == 'gte' %}
            {{ field }} >= ${{ loop.index }}
            {% elif value['lookup'] == 'lt' %}
            {{ field }} < ${{ loop.index }}
            {% elif value['lookup'] == 'lte' %}
            {{ field }} <= ${{ loop.index }}
            {% endif %}
            {% if not loop.last %}{{ filters_operator }}{% endif %}
        {% endfor %}
    {% endif %}
    {% if order_by %}ORDER BY {{ order_by }} {{ order_by_sort }}{% endif %}
    {% if limit %}LIMIT {{ limit }}{% endif %}
    {% if offset %}OFFSET {{ offset }}{% endif %}
    """
)

sql_detail_template = Template(
    """
    SELECT
    {% if not fields %}*{% else %}{% for field_name in fields %}{{ field_name }}{% if not loop.last %}, {% endif %}{% endfor %} {% endif %}
    FROM {{ table_name }}
    WHERE {{ pk_field }} = $1
    """
)

sql_update_template = Template(
    """
    UPDATE {{ table_name }}
    SET
    {% for field_name in field_names %}{{ field_name }} = ${{ loop.index }}{% if not loop.last %}, {% endif %}{% endfor %}
    WHERE {{ pk_field }} = ${{ field_names|length + 1}}
    RETURNING *
    """
)

sql_delete_template = Template(
    """
    DELETE FROM {{ table_name }}
    WHERE {{ pk_field }} = $1
    """
)
