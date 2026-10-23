"""Versioned native query identities and cursor bindings for logged-in WWW."""

AUTH_RELAY_FAMILY = "instagram_authenticated_www_relay"
PARENT_ROOT_DOCUMENT = ("29631174456466007", "PolarisPostCommentsContainerQuery")
PARENT_DOCUMENT = ("28915717348095854", "PolarisPostCommentsPaginationQuery")
CHILD_ROOT_DOCUMENT = ("28723871317239557", "PolarisPostChildCommentsQuery")
CHILD_DOCUMENT = ("28694573636831179", "PolarisPostCommentsChildrenPaginationtQuery")
QUERY_KEYS = ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size")
BINDING_KEYS = ("media_id", "relay_logged_in", "relay_threads_style")


def operation(*, parent: bool, initial: bool):
    if parent:
        return CHILD_ROOT_DOCUMENT if initial else CHILD_DOCUMENT
    return PARENT_ROOT_DOCUMENT if initial else PARENT_DOCUMENT


def valid_provenance(value, *, parent: bool) -> bool:
    import re

    initial = value.get("source") == "root"
    cursor = value.get("request_cursor")
    doc, name = operation(parent=parent, initial=initial)
    size = None if initial else (5 if parent else 10)
    return (
        value.get("adapter_family") == AUTH_RELAY_FAMILY
        and type(value.get("adapter_version")) is int
        and value["adapter_version"] == 1
        and value.get("source") in {"root", "resume"}
        and (cursor is None if initial else isinstance(cursor, str) and bool(cursor.strip()))
        and value.get("relay_doc_id") == doc
        and value.get("relay_friendly_name") == name
        and value.get("sort_order") == ("native_default" if parent else "popular")
        and "page_size" in value
        and (value["page_size"] is None if initial else type(value["page_size"]) is int and value["page_size"] == size)
        and isinstance(value.get("media_id"), str)
        and re.fullmatch(r"[1-9][0-9]{0,39}", value["media_id"]) is not None
        and value.get("relay_logged_in") is True
        and type(value.get("relay_threads_style")) is bool
    )


def continuation_matches(previous, value, *, parent: bool) -> bool:
    if any(previous.get(key) != value.get(key) for key in BINDING_KEYS):
        return False
    if all(previous.get(key) == value.get(key) for key in QUERY_KEYS):
        return True
    # Only an initial-container lane can transition to its own continuation.
    root_doc, root_name = operation(parent=parent, initial=True)
    next_doc, next_name = operation(parent=parent, initial=False)
    return (
        previous.get("relay_doc_id") == root_doc
        and previous.get("relay_friendly_name") == root_name
        and previous.get("page_size") is None
        and previous.get("sort_order") == value.get("sort_order")
        and value.get("relay_doc_id") == next_doc
        and value.get("relay_friendly_name") == next_name
        and value.get("page_size") == (5 if parent else 10)
    )
