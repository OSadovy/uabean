"""Imports statement csv files exported from Alfabank Business Online web app.

The CSV header is the following:
Наш рахунок;Наш IBAN;Операція;Рахунок;IBAN;МФО банку контрагента;Найменування контрагента;Код контрагента;Призначення платежу;Дата проведення;Номер документа;Сума;Валюта;Час проведення;Дата документа;Дата архівування;Ід.код;Найменування;МФО
"""

import csv
from collections import defaultdict
import re

from beancount.core import flags
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.utils.date_utils import parse_date_liberally

from beancount.core.number import D
from beancount.core import data


class Importer(IdentifyMixin, ImporterProtocol):
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]
    fee_regexes = ("Погашення комісії",)
    currency_conversion_regex = r"Зарахування коштiв вiд вільного продажу (?P<amount>[\d\.]+) (?P<currency>\w+) по курсу (?P<rate>[\d\.]+).*?Комiсiя банку становить (?P<fee>[\d\.]+) грн\."
    DATE_FIELD = "Дата проведення"

    def __init__(self, accounts, fee_account, *args, **kwargs):
        self.accounts = accounts
        self.fee_account = fee_account
        super().__init__(*args, **kwargs)

    def get_csv_reader(self, file):
        return csv.DictReader(open(file.name, encoding="windows-1251"), delimiter=";")

    def get_date_from_row(self, row):
        return parse_date_liberally(row[self.DATE_FIELD], dict(dayfirst=True))

    def get_account_from_row(self, row):
        k = (row["Валюта"], row["Наш IBAN"])
        if k not in self.accounts:
            raise ValueError("Unknown account %s %s" % (row["Валюта"], row["Наш IBAN"]))
        return self.accounts[k]

    def file_date(self, file):
        "Get the maximum date from the file."
        max_date = None
        for row in self.get_csv_reader(file):
            if not row:
                continue
            date = self.get_date_from_row(row)
            if max_date is None or date > max_date:
                max_date = date
        return max_date

    def extract(self, file, existing_entries=None):
        entries = []
        for index, row in enumerate(self.get_csv_reader(file), 1):
            if not row:
                continue
            meta = data.new_metadata(file.name, index)
            entry = self.get_entry_from_row(row, meta)
            if entry is not None:
                entries.append(entry)

        return self.merge_entries(entries)

    def get_entry_from_row(self, row, meta):
        meta["time"] = row["Час проведення"]
        meta["src_doc_n"] = row["Номер документа"]
        meta["src_purpose"] = row["Призначення платежу"]
        account = self.get_account_from_row(row)
        if account is None:
            return
        payee = row["Найменування контрагента"]
        txn = data.Transaction(
            meta,
            self.get_date_from_row(row),
            flags.FLAG_OKAY,
            payee,
            "",
            data.EMPTY_SET,
            data.EMPTY_SET,
            [],
        )
        sum = D(row["Сума"])
        op = row["Операція"]
        if op == "Дебет":
            sum = -sum
        elif op == "Кредит":
            pass
        else:
            raise RuntimeError(f"unknown operation {op}")
        units = data.Amount(sum, row["Валюта"])
        txn.postings.append(data.Posting(account, units, None, None, None, None))
        for fee_str in self.fee_regexes:
            if re.search(fee_str, row["Призначення платежу"]):
                txn.postings.append(
                    data.Posting(self.fee_account, -units, None, None, None, None)
                )
                break
        return txn

    def merge_entries(self, entries):
        def find_closest(l, other, predicate):
            for e in l:
                if not e == other and predicate(e):
                    return e
            return None

        entries_by_date = defaultdict(list)
        for e in entries:
            entries_by_date[e.date].append(e)
        for _date, subentries in entries_by_date.items():
            if len(subentries) == 1:
                continue
            # find & merge same currency transfers
            for e in subentries:
                other = find_closest(
                    subentries, e, lambda x: x.meta["src_doc_n"] == e.meta["src_doc_n"]
                )
                if other is not None:
                    entries.remove(other)
                    subentries.remove(e)
                    e.postings.extend(other.postings)
            # find & merge currency convertions
            for e in subentries:
                m = re.search(self.currency_conversion_regex, e.meta["src_purpose"])
                if not m:
                    continue
                units = data.Amount(D(m.group("amount")), m.group("currency"))
                other = find_closest(
                    subentries, e, lambda x: x.postings[0].units == -units
                )
                if other is None:
                    raise RuntimeError("can't find matching         entry for %s" % e)
                other_posting = other.postings[0]._replace(
                    price=data.Amount(D(m.group("rate")), "UAH")
                )
                e.postings.insert(0, other_posting)
                e.postings.append(
                    data.Posting(
                        self.fee_account,
                        data.Amount(D(m.group("fee")), "UAH"),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                e.postings.extend(other.postings[1:])
                e.meta["other_src_doc_n"] = other.meta["src_doc_n"]
                entries.remove(other)
        return entries
