"""Imports xls statements from privatbank, optained via privat24 web interface.

The header is as follows:
Дата;Час;Категорія;Картка;Опис операції;Сума в валюті картки;Валюта картки;Сума в валюті транзакції;Валюта транзакції;Залишок на кінець періоду;Валюта залишку
"""

import xlrd
import datetime
import dateutil.parser
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.core import data
from beancount.core.number import D


class Importer(IdentifyMixin, ImporterProtocol):
    matchers = [
        ("filename", "privat"),
        ("mime", "application/vnd.ms-excel"),
    ]
    unknown_account = "Assets:Unknown"
    DATE_COL = 0
    TIME_COL = 1
    CATEGORY_COL = 2
    CARD_COL = 3
    DESCRIPTION_COL = 4
    CARD_CURRENCY_AMOUNT_COL = 5
    CARD_CURRENCY_COL = 6
    TRANSACTION_AMOUNT_COL = 7
    TRANSACTION_CURRENCY_COL = 8
    BALANCE_COL = 9
    BALANCE_CURRENCY_COL = 10
    CURRENCY_MAP = {"грн": "UAH", "дол": "USD", "євро": "EUR", "PLN": "PLN"}

    def __init__(
        self,
        card_to_account_map,
        *args,
        fee_account="Expenses:Fees:Privatbank",
        **kwargs
    ):
        self.card_to_account_map = card_to_account_map
        self.fee_account = fee_account
        super().__init__(*args, **kwargs)

    def date_from_row(self, row):
        return dateutil.parser.parse(row[self.DATE_COL].value, dayfirst=True)

    @classmethod
    def get_currency(cls, cell):
        return cls.CURRENCY_MAP[cell.value]

    @staticmethod
    def get_number(cell):
        return D(str(cell.value))

    def extract(self, file, existing_entries=None):
        entries = []
        workbook = xlrd.open_workbook(file.name)
        sheet = workbook.sheet_by_index(0)
        assert "Виписка з Ваших карток за період" in sheet.cell(0, 0).value
        max_date = None
        max_row = None
        for nrow in range(2, sheet.nrows):
            row = sheet.row(nrow)
            meta = data.new_metadata(file.name, nrow)
            entries.append(self.entry_from_row(meta, row))
            date = self.date_from_row(row)
            if max_date is None or date > max_date:
                max_date = date
                max_row = row
        if max_row is not None:
            amount = data.Amount(self.get_number(max_row[self.BALANCE_COL]), self.get_currency(max_row[self.BALANCE_CURRENCY_COL]))
            entries.append(
                data.Balance(
                    data.new_metadata(file.name, 0),
                    max_date.date() + datetime.timedelta(days=1),
                    self.card_to_account_map[max_row[self.CARD_COL].value],
                    amount,
                    None,
                    None,
                )
            )
        return entries

    def entry_from_row(self, meta, row):
        dt = self.date_from_row(row)
        meta["time"] = row[self.TIME_COL].value
        meta["category"] = row[self.CATEGORY_COL].value
        account = self.card_to_account_map.get(row[self.CARD_COL].value, self.unknown_account)
        num = self.get_number(row[self.TRANSACTION_AMOUNT_COL])
        currency = self.get_currency(row[self.TRANSACTION_CURRENCY_COL])
        card_num = self.get_number(row[self.CARD_CURRENCY_AMOUNT_COL])
        card_currency = self.get_currency(row[self.CARD_CURRENCY_COL])
        postings = [
            data.Posting(
                account, data.Amount(card_num, card_currency), None, None, None, None
            )
        ]
        if currency != card_currency:
            meta["converted"] = f"{num} {currency}"
        elif abs(card_num) != num:
            fee_amount = data.Amount(abs(card_num) - num, currency)
            postings.append(
                data.Posting(self.fee_account, fee_amount, None, None, None, None)
            )
        return data.Transaction(
            meta,
            dt.date(),
            self.FLAG,
            None,
            row[self.DESCRIPTION_COL].value,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )
