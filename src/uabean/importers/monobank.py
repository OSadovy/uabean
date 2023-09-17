"""Imports monobank csv statements.

CSV header is as follows:
"Дата i час операції","Деталі операції",MCC,"Сума в валюті картки (UAH)","Сума в валюті операції",Валюта,Курс,"Сума комісій (UAH)","Сума кешбеку (UAH)","Залишок після операції"
"""

import csv
import datetime
import dateutil.parser
import re
import requests
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.core import data
from beancount.core.number import D


mcc_codes_url = "https://raw.githubusercontent.com/Oleksios/Merchant-Category-Codes/main/Without%20groups/mcc-en.json"


class Importer(IdentifyMixin, ImporterProtocol):
    DATE_COL = 0
    DESCRIPTION_COL = 1
    MCC_COL = 2
    AMOUNT_COL = 3
    ORIG_AMOUNT_COL = 4
    CURRENCY_COL = 5
    EXCHANGE_RATE_COL = 6
    COMISSION_RATE_COL = 7
    CASHBACK_COL = 8
    RUNNING_BALANCE_COL = 9
    NA = "—"
    CASHBACK_RE = r"^Виведення кешбеку ([\d\.]+)"
    INTEREST_RE = r"^Відсотки за .*"

    def __init__(
        self,
        account_config,
        cashback_income_account="Income:Cashback:Monobank",
        cashback_receivable_account="Assets:Monobank:Receivable:Cashback",
        taxes_expense_account="Expenses:Taxes",
        interest_income_account="Income:Monobank:Interest",
    ):
        self.account_config = account_config
        self.cashback_income_account = cashback_income_account
        self.cashback_receivable_account = cashback_receivable_account
        self.taxes_expense_account = taxes_expense_account
        self.interest_income_account = interest_income_account
        self.matchers = [
            ("mime", "text/csv"),
            (
                "filename",
                f"monobank-({'|'.join('%s-%s' % (type, currency) for type, currency in account_config)})",
            ),
        ]
        super().__init__()

    def _download_mcc_codes(self):
        codes = requests.get(mcc_codes_url).json()
        self.mcc_codes = {c["mcc"]: c["shortDescription"] for c in codes}

    def get_csv_reader(self, file):
        return csv.reader(open(file.name))

    def file_account(self, file):
        # example: monobank-black-UAH_22-10-22_14-24-57.csv
        parts = file.name.split("_")[0].split("-")
        return self.account_config[(parts[1], parts[2])]

    def extract(self, file, existing_entries=None):
        self._download_mcc_codes()
        entries = []
        reader = self.get_csv_reader(file)
        header = next(reader)
        account = self.file_account(file)
        account_currency = re.search(r"\((\w+)\)", header[self.AMOUNT_COL]).group(1)
        cashback_currency = re.search(r"\((\w+)\)", header[self.CASHBACK_COL]).group(1)
        for i, row in enumerate(reader, 2):
            meta = data.new_metadata(file.name, i)
            entries.append(self.entry_from_row(meta, account, account_currency, cashback_currency, row))
        if entries:
            meta = data.new_metadata(file.name, i)
            entries.append(
                data.Balance(
                    meta,
                    entries[-1].date + datetime.timedelta(days=1),
                    account,
                    data.Amount(D(row[self.RUNNING_BALANCE_COL]), account_currency),
                    None,
                    None,
                )
            )
        return entries

    def entry_from_row(self, meta, account, account_currency, cashback_currency, row):
        meta["category"] = self.mcc_codes[row[self.MCC_COL]]
        postings = []
        dt = self.date_from_str(row[self.DATE_COL])
        meta["time"] = dt.time().strftime("%H:%M:%S")
        price = None
        if row[self.CURRENCY_COL] != account_currency:
            price = data.Amount(
                round(D(row[self.ORIG_AMOUNT_COL])/D(row[self.AMOUNT_COL]), 6).normalize(), row[self.CURRENCY_COL]
            )
            meta["converted"] = row[self.ORIG_AMOUNT_COL] + " " + row[self.CURRENCY_COL]
        description = row[self.DESCRIPTION_COL]
        payee = description
        narration = None
        if m := re.search(self.CASHBACK_RE, description):
            u = data.Amount(-D(m.group(1)), cashback_currency)
            postings += [
                data.Posting(self.cashback_receivable_account, u, None, None, None, None),
                data.Posting(self.taxes_expense_account, None, None, None, None, None),
            ]
            narration, payee = payee, None
            del meta["category"]
        if m := re.search(self.INTEREST_RE, description):
            u = data.Amount(-D(row[self.AMOUNT_COL]), account_currency)
            postings.append(data.Posting(self.interest_income_account, u, None, None, None, None))
            narration, payee = payee, None
            del meta["category"]
        postings.append(
            data.Posting(
                account,
                data.Amount(D(row[self.AMOUNT_COL]), account_currency),
                None,
                price,
                None,
                None,
            )
        )
        if row[self.CASHBACK_COL] != self.NA:
            postings += [
                data.Posting(
                    self.cashback_income_account,
                    data.Amount(-D(row[self.CASHBACK_COL]), cashback_currency),
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.cashback_receivable_account,
                    data.Amount(D(row[self.CASHBACK_COL]), cashback_currency),
                    None,
                    None,
                    None,
                    None,
                ),
            ]
        return data.Transaction(
            meta,
            dt.date(),
            self.FLAG,
            payee,
            narration,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def date_from_str(self, s):
        return dateutil.parser.parse(s, dayfirst=True)
