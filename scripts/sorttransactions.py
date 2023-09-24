"""Sorts transactions in a ledger file by date and time.

requires: autobean <https://github.com/SEIAROTg/autobean/tree/master/autobean/refactor>
"""

import copy
import datetime
import sys

from autobean.refactor import models
from autobean.refactor.parser import Parser
from autobean.refactor.printer import print_model


def sort_transactions(file, start, end):
    def sortkey(d):
        if isinstance(d, models.IgnoredLine):
            return (datetime.date(1, 1, 1), "")
        return (d.date, d.meta.get("time", ""))

    if (
        sum(
            1 if isinstance(directive, models.Transaction) else 0
            for directive in file.directives[start:end]
        )
        > 1
    ):
        file.directives[start:end] = [
            copy.deepcopy(d) for d in sorted(file.directives[start:end], key=sortkey)
        ]


parser = Parser()
file = parser.parse(open(sys.argv[1]).read(), models.File)
start_range = None
for i, directive in enumerate(file.directives):
    if (
        isinstance(directive, models.Transaction)
        or isinstance(directive, models.IgnoredLine)
        or isinstance(directive, models.Balance)
    ):
        if start_range is None:
            start_range = i
    else:
        if start_range is not None:
            print(type(directive))
            sort_transactions(file, start_range, i)
            start_range = None
if start_range is not None:
    sort_transactions(file, start_range, len(file.directives))
print_model(file, open(sys.argv[1], "w"))
