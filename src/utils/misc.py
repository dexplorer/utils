from itertools import groupby
import inspect as ins


def check_if_any_list_item_in_str(items: list, text: str) -> bool:
    """
    Check if any item in the list is in the given text string.
    """
    return any(item in text for item in items)


def dedupe_list_of_dict(records: list) -> list:
    """
    Dedupe the list of dict items by converting the dict item to a string. Return the list of unique dict items.
    """
    deduped_items_dict = {str(item): item for item in records}
    deduped_items = [item_val for item_val in deduped_items_dict.values()]
    return deduped_items


def dedupe_list_of_dict_by_value_priority(
    records: list[dict], priority_list: list, key_column: str, priority_column: str
) -> list[dict]:
    """
    Dedupe the list of dict items by grouping the records by a key column and priorities assigned to values in column 2.
    Return the list of unique dict items.
    """
    priorities = {i: idx for idx, i in enumerate(priority_list)}

    sort_key = lambda x: (x[key_column], priorities[x[priority_column]])
    groupby_key = lambda x: x[key_column]

    deduped_data_list = [
        next(i[1]) for i in groupby(sorted(records, key=sort_key), key=groupby_key)
    ]
    return deduped_data_list


def flatten_list(nested_list: list) -> list:
    return [item for inner_list in nested_list for item in inner_list]


def dump_as_str(
    obj,
    obj_types: tuple = (),
    indent_size: int = 2,
    indent_level: int = 1,
    wrap: bool = False,
) -> str:
    line_delim = " "
    if wrap:
        line_delim = "\n"

    result = f"{obj.__name__}: {obj.__class__.__name__}{line_delim}"

    # Not all objects have a __dict__ method
    items = {}
    if "__dict__" in dir(obj):
        items = obj.__dict__.items()
    else:
        for attr in dir(obj):
            items[attr] = getattr(obj, attr)

    for k, v in items:
        if (
            not k.startswith("__")
            and not ins.ismethod(getattr(obj, k))
            and not ins.isfunction(getattr(obj, k))
        ):
            if isinstance(v, obj_types):
                v_str = v.__str__(
                    obj=v,
                    obj_types=obj_types,
                    indent_size=indent_size,
                    indent_level=indent_level + 1,
                )
            else:
                v_str = str(v)
            result += f"{' ' * (indent_size * indent_level)}{k}: {v_str}{line_delim}"

    return result
