import itertools
import os
from collections import defaultdict
from decimal import Decimal

from beancount.core.data import Transaction


def detect_transfers(extracted, existing_entries):
    """Merges transactions with single posting that happened close to each other
    in time and have the same absolute value.
    Catches transfers from one card to another within the same bank etc.
    """
    possible_transfers = defaultdict(list)
    for filename, new_entries, account, importer in extracted:
        for entry in new_entries[:]:
            if not isinstance(entry, Transaction):
                continue
            k, v = get_transfer_info(entry)
            found = False
            for other_v, other_entry in possible_transfers[k]:
                if are_values_oposit(v, other_v) and not same_accounts(
                    other_entry, entry
                ):
                    found = True
                    merge_transactions(other_entry, entry)
                    new_entries.remove(entry)
                    break
            if not found:
                possible_transfers[k].append((v, entry))
    return extracted


def get_transfer_info(entry):
    time = "00:00:00"
    if "time" in entry.meta:
        time = to_nearest_time(entry.meta["time"], unit="hour", rnd=1, frm="%H:%M:%S")
    s = sum((p.units.number for p in entry.postings if p.units), Decimal(0))
    return (entry.date, time), s


def are_values_oposit(v1, v2):
    """Returns if v1 and v2 have oposit signs and are within 1% tolerance value."""
    if (v1 > 0 and v2 > 0) or (v1 < 0 and v2 < 0):
        return False
    if v2 == 0:
        return False
    return round(abs(abs(v1 / v2) - 1), 2) <= 0.01


def same_accounts(t1, t2):
    return set(p.account for p in t1.postings) == set(p.account for p in t2.postings)


def merge_transactions(t1, t2):
    t1.postings.extend(t2.postings)


def to_nearest_time(ts, unit="sec", rnd=1, frm=None):
    """round to nearest Time format
    param ts = time string to round in '%H:%M:%S' or '%H:%M' format :
    param unit = specify unit wich must be rounded 'sec' or 'min' or 'hour', default is seconds :
    param rnd = to which number you will round, the default is 1 :
    param frm = the output (return) format of the time string, as default the function take the unit format
    """
    from time import gmtime, strftime

    ts = ts + ":00" if len(ts) == 5 else ts
    if "se" in unit.lower():
        frm = "%H:%M:%S" if frm is None else frm
    elif "m" in unit.lower():
        frm = "%H:%M" if frm is None else frm
        rnd = rnd * 60
    elif "h" in unit.lower():
        frm = "%H" if frm is None else frm
        rnd = rnd * 3600
    secs = sum(int(x) * 60**i for i, x in enumerate(reversed(ts.split(":"))))
    rtm = int(round(secs / rnd, 0) * rnd)
    nt = strftime(frm, gmtime(rtm))
    return nt


def sort_samebank_entries(new_entries_list, existing_entries):
    """Sorts entries that are originated from files with same prefix."""
    new_entries_list.sort(key=lambda e: e[0])
    for k, group in itertools.groupby(
        new_entries_list, key=lambda e: os.path.basename(e[0]).split("-", 1)[0]
    ):
        pass
