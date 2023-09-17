"""Imports csv statements from sensesuperapp.

CSV header is as follows:
Дата і час;Статус;Тип;Деталі;Адреса;MCC;Cума списання;Cума зарахування
"""

import csv
import dateutil.parser
import re
import requests
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.core import data
from beancount.core.number import D


mcc_codes_url = "https://raw.githubusercontent.com/Oleksios/Merchant-Category-Codes/main/Without%20groups/mcc-en.json"


class Importer(IdentifyMixin, ImporterProtocol):
    matchers = [
        ("content", "Виписка за рахунком;;;;;;;"),
        ("mime", "text/csv"),
    ]
    converter = lambda unused, fname: open(fname, encoding="windows-1251").read()
    DATE_COL = 0
    STATUS_COL = 1
    TYPE_COL = 2
    DETAIL_COL = 3
    ADDRESS_COL = 4
    MCC_COL = 5
    DEBIT_COL = 6
    CREDIT_COL = 7

    def __init__(self, account_config, *args, **kwargs):
        self.account_config = account_config
        self.mcc_codes = {}
        super().__init__(*args, **kwargs)

    def get_csv_reader(self, file):
        return csv.reader(open(file.name, encoding="windows-1251"), delimiter=";")

    def date_from_row(self, row):
        return dateutil.parser.parse(row[self.DATE_COL], dayfirst=True)

    def file_date(self, file):
        "Get the maximum date from the file."
        reader = self.get_csv_reader(file)
        next(reader)
        row = next(reader)
        m = re.search(r"за період (.*?) - (.*)", row[0])
        return dateutil.parser.parse(m.group(2), dayfirst=True)

    def file_account(self, file):
        reader = self.get_csv_reader(file)
        for i in range(4):
            row = next(reader)
        m = re.search(r"Рахунок (UA\d+)", row[0])
        return self.account_config[m.group(1)]

    def extract(self, file, existing_entries=None):
        entries = []
        codes = requests.get(mcc_codes_url).json()
        self.mcc_codes = {c["mcc"]: c["shortDescription"] for c in codes}
        reader = self.get_csv_reader(file)
        for _ in range(4):
            row = next(reader)
        m = re.search(r"Рахунок (\w+) \((\w+)\)", row[0])
        account = self.account_config[m.group(1)]
        currency = m.group(2)
        for row in reader:
            if row[0].startswith("Деталізація операцій") or row[0].startswith(
                "Дата і час"
            ):
                continue
            meta = data.new_metadata(file.name, reader.line_num)
            entries.append(self.entry_from_row(meta, account, currency, row))
        return entries

    def entry_from_row(self, meta, account, currency, row):
        dt = self.date_from_row(row)
        meta["time"] = dt.strftime("%H:%M")
        payee = None
        narration = None
        details = row[self.DETAIL_COL]
        type = row[self.TYPE_COL]
        if type.startswith("Покупка") or type.startswith("POS Purchase"):
            payee = details
        elif type in ("Банкомати", "Комуналка та інші платежі", "Інше", "Перекази"):
            narration = details
        if row[self.MCC_COL]:
            meta["category"] = self.mcc_codes[row[self.MCC_COL]]
        if row[self.ADDRESS_COL] and not type == "Перекази":
            meta["address"] = row[self.ADDRESS_COL]
        amount_str = (
            row[self.DEBIT_COL] if row[self.DEBIT_COL] else row[self.CREDIT_COL]
        )
        units = data.Amount(D(amount_str.replace(",", ".")), currency)
        return data.Transaction(
            meta,
            dt.date(),
            self.FLAG,
            payee,
            narration,
            data.EMPTY_SET,
            data.EMPTY_SET,
            [data.Posting(account, units, None, None, None, None)],
        )
