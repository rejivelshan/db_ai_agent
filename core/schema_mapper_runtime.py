GLOBAL_SCHEMA = None


def rank_identifier_fields(fields):
    def score(field):
        return (0 if field.lower() == "id" else 1, field)

    return sorted(fields, key=score)


def get_table_key_columns(details):
    return details.get("primary_key") or details.get("inferred_primary_key") or []


def get_primary_keys_for_object(obj):
    global GLOBAL_SCHEMA

    if not GLOBAL_SCHEMA or not isinstance(obj, dict):
        return []

    object_fields = set(obj.keys())

    matches = []
    for details in GLOBAL_SCHEMA.values():
        key_columns = get_table_key_columns(details)
        if key_columns and set(key_columns).issubset(object_fields):
            matches.extend(pk for pk in key_columns if pk in obj)

    deduped = []
    seen = set()
    for field in matches:
        if field not in seen:
            deduped.append(field)
            seen.add(field)
    return deduped


def get_identifier_fields(obj):
    if not isinstance(obj, dict):
        return []

    schema_primary_keys = get_primary_keys_for_object(obj)
    if schema_primary_keys:
        return rank_identifier_fields(schema_primary_keys)

    generic_ids = [
        key for key, value in obj.items()
        if value is not None
        and not isinstance(value, (dict, list))
        and key.lower() == "id"
    ]
    return rank_identifier_fields(generic_ids)


def is_object_primary_key(obj, key):
    return key in get_primary_keys_for_object(obj)

def is_main_id_of_object(obj, key):
    id_fields = get_identifier_fields(obj)
    if not id_fields:
        return False

    non_fk_ids = [field for field in id_fields if not is_foreign_key(field)]
    if non_fk_ids:
        return non_fk_ids[0] == key

    return id_fields[0] == key




def is_primary_key_field(field):
    """
    Check if field is a primary key in ANY table
    """
    global GLOBAL_SCHEMA

    if not GLOBAL_SCHEMA:
        return False

    for table, details in GLOBAL_SCHEMA.items():
        if field in get_table_key_columns(details):
            return True

    return False





def set_schema(schema):
    global GLOBAL_SCHEMA
    GLOBAL_SCHEMA = schema

def normalize_types(value):
    """
    Normalize data types between SQL and Mongo
    """
    from decimal import Decimal
    import datetime

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, datetime.date):
        return value.isoformat()

    if isinstance(value, datetime.datetime):
        return value.isoformat()

    return value

def is_primary_key(field):
    global GLOBAL_SCHEMA

    if not GLOBAL_SCHEMA:
        return False

    for table, details in GLOBAL_SCHEMA.items():
        if field in get_table_key_columns(details):
            return True

    return False


def is_foreign_key(field):
    global GLOBAL_SCHEMA

    if not GLOBAL_SCHEMA:
        return False

    for table, details in GLOBAL_SCHEMA.items():
        for fk in details.get("foreign_keys", []):
            if field == fk["column"]:
                return True
    return False




def apply_schema_mapping(data, field_map, ignore_fields=None, depth=0):
    if ignore_fields is None:
        ignore_fields = []

    # 🔥 Handle list
    if isinstance(data, list):
        return [
            apply_schema_mapping(item, field_map, ignore_fields, depth)
            for item in data
        ]

    # 🔥 Handle dict
    if isinstance(data, dict):
        new_obj = {}

        for key, value in data.items():

            mapped_key = field_map.get(key, key)

            # ✅ FINAL FIX
            # remove FK only when deeply nested
            # ✅ REMOVE FK only if it's not main id of object
            # ✅ KEEP if it is PRIMARY KEY
            # ✅ REMOVE if it is ONLY FK
            if (
                depth > 0
                and is_foreign_key(key)
                and not is_object_primary_key(data, key)
                and not is_main_id_of_object(data, key)
            ):
                continue
            # ✅ Ignore nested unwanted fields
            if key in ignore_fields and depth > 0:
                continue

            new_value = apply_schema_mapping(
                value,
                field_map,
                ignore_fields,
                depth + 1
            )

            new_obj[mapped_key] = normalize_types(new_value)

        return new_obj

    return normalize_types(data)
