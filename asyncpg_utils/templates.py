from jinja2 import Template

sql_create_template = Template(
    """
    INSERT INTO "{{ table_name }}"
    ({% for field_name in field_names %}"{{ field_name }}"{% if not loop.last %}, {% endif %}{% endfor %})
    VALUES
    ({% for field_name in field_names %}${{ loop.index }}{% if not loop.last %}, {% endif %}{% endfor %})
    RETURNING *
    """
)

sql_list_template = Template(
    """
    SELECT
    {% if not fields %}*{% else %}{% for field_name in fields %}"{{ field_name }}"{% if not loop.last %}, {% endif %}{% endfor %} {% endif %}
    FROM "{{ table_name }}"
    {% if filters %}
    WHERE
    {% for filter in filters %}"{{ filter }}" = ${{ loop.index }}{% endfor %}
    {% endif %}
    """
)

sql_detail_template = Template(
    """
    SELECT
    {% if not fields %}*{% else %}{% for field_name in fields %}"{{ field_name }}"{% if not loop.last %}, {% endif %}{% endfor %} {% endif %}
    FROM "{{ table_name }}"
    WHERE "{{ pk_field }}" = $1
    """
)

sql_update_template = Template(
    """
    UPDATE "{{ table_name }}"
    SET
    {% for field_name in field_names %}"{{ field_name }}" = ${{ loop.index }}{% if not loop.last %}, {% endif %}{% endfor %}
    WHERE "{{ pk_field }}" = ${{ field_names|length + 1}}
    RETURNING *
    """
)

sql_delete_template = Template(
    """
    DELETE FROM "{{ table_name }}"
    WHERE "{{ pk_field }}" = $1
    """
)
