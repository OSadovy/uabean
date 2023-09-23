"""Imports statement csv files exported from Ukrsib Business Online web app.

The CSV header is the following:
"ЄДРПОУ";"МФО";"Рахунок";"Валюта";"Дата операцiї";"Код операцiї";"МФО банка";"Назва банка";"Рахунок кореспондента";"ЄДРПОУ кореспондента";"Кореспондент";"Номер документа";"Дата документа";"Дебет";"Кредит";"Призначення платежу";"Гривневе покриття";
"""

import csv
import re
from collections import defaultdict

import beangulp
from beancount.core import data, flags
from beancount.core.number import D
from beancount.utils.date_utils import parse_date_liberally

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]
    fee_regexes = ("Комісія по рах", "Списання комісії по рахунку")
    currency_conversion_regex = r"Гр.экв.продажу (?P<amount>[\d\.]+) (?P<currency>\w+) на МВР .*?, ЗГІДНО ЗАЯВИ КЛІЄНТА № \d+ \.Курс (?P<rate>[\d\.]+)\.Ком.банку (?P<fee>[\d\.]+)\."
    DATE_FIELD = "Дата операцiї"

    def __init__(self, accounts, fee_account, *args, **kwargs):
        self.accounts = accounts
        self.fee_account = fee_account
        super().__init__(*args, **kwargs)

    def get_csv_reader(self, filename):
        return csv.DictReader(open(filename, encoding="windows-1251"), delimiter=";")

    def get_date_from_row(self, row):
        return parse_date_liberally(row[self.DATE_FIELD], dict(dayfirst=True))

    def get_account_from_row(self, row):
        k = (row["Валюта"], row["Рахунок"])
        if k not in self.accounts:
            raise ValueError("Unknown account %s %s" % (row["Валюта"], row["Наш IBAN"]))
        return self.accounts[k]

    def date(self, filename):
        "Get the maximum date from the file."
        max_date = None
        for row in self.get_csv_reader(filename):
            if not row:
                continue
            date = self.get_date_from_row(row)
            if max_date is None or date > max_date:
                max_date = date
        return max_date

    def account(self, _):
        return "ukrsib"

    def extract(self, filename, existing_entries=None):
        entries = []
        for index, row in enumerate(self.get_csv_reader(filename), 1):
            if not row:
                continue
            meta = data.new_metadata(filename, -index)
            entry = self.get_entry_from_row(row, meta)
            if entry is not None:
                entries.append(entry)

        return self.merge_entries(entries)

    def get_entry_from_row(self, row, meta):
        meta["time"] = row[self.DATE_FIELD].split(" ")[-1]
        meta["src_doc_n"] = row["Номер документа"]
        meta["src_purpose"] = row["Призначення платежу"]
        account = self.get_account_from_row(row)
        if account is None:
            return
        payee = row["Кореспондент"]
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
        sum = -D(row["Дебет"]) if row["Дебет"] else D(row["Кредит"])
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
        def find_closest(lst, other, predicate):
            for e in lst:
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
                    price=data.Amount(D(m.group("rate")) / 100, "UAH")
                )
                e.postings.insert(0, other_posting)
                e.postings.extend(other.postings[1:])
                e.meta["other_src_doc_n"] = other.meta["src_doc_n"]
                entries.remove(other)
        return entries


def get_test_importer():
    return Importer(
        {
            ("UAH", "26000000000000"): "Assets:Ukrsibbank:Business:Cash:UAH",
            ("GBP", "26000000000000"): "Assets:Ukrsibbank:Business:Cash:GBP",
        },
        "Expenses:Fees:Banking:Ukrsibbank",
    )


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
