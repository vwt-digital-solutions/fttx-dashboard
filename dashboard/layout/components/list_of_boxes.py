from layout.components.global_info import global_info
from layout.components.indicator import indicator


def global_info_list(items):
    return [global_info(**item) for item in items]


def project_indicator_list(items, **kwargs):
    return [indicator(**item, **kwargs) for item in items]
