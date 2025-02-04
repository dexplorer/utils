from itertools import groupby


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
