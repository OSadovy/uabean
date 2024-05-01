"""Imports xls statements from Procreditbank, optained via web interface.
"""

import re
from datetime import timedelta

import beangulp
import dateutil.parser
import xlrd
from beancount.core import data, flags
from beancount.core.number import D

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    FLAG = flags.FLAG_OKAY
    matchers = [
        ("filename", "Рух по рахунку_"),
        ("mime", "application/vnd.ms-excel"),
    ]
    deposit_regex = r"Перерахування нарахованих відсотків згідно договору вкладу"

    def __init__(
        self,
        account_config,
        *args,
        fee_account="Expenses:Fees:Procreditbank",
        deposit_income_account="Income:Investments:Deposits",
        **kwargs,
    ):
        self.account_config = account_config
        self.fee_account = fee_account
        self.deposit_income_account = deposit_income_account
        super().__init__(*args, **kwargs)

    def date_from_row(self, row):
        return dateutil.parser.parse(row[1].value, dayfirst=True)

    @staticmethod
    def get_number(cell):
        return D(str(cell.value))

    def account(self, _):
        return "procreditbank"

    def extract(self, filename, existing_entries=None):
        entries = []
        workbook = xlrd.open_workbook(filename)
        sheet = workbook.sheet_by_index(0)
        # assert 'Банк: АТ "ПроКредит Банк"' in sheet.cell(1, 12).value
        account_number = sheet.cell(4, 1).value
        assert account_number.startswith("Рахунок:")
        account_number = account_number.split(" ", 1)[1]
        account_currency = sheet.cell(6, 1).value
        assert "Валюта рахунку:" in account_currency
        account_currency = account_currency.rsplit(" ", 1)[1]
        account = self.account_config.get((account_currency, account_number))
        if account is None:
            raise ValueError(f"Unknown account {account_currency} {account_number}")
        header = None
        header_cols = None
        for nrow in range(10, sheet.nrows):
            row = sheet.row(nrow)
            meta = data.new_metadata(filename, nrow)
            if row[1].value == "Операції по рахунку":
                header = "account_ops"
                continue
            elif row[1].value == "Операції по картам":
                header = "card_ops"
                continue
            elif cell_index := find_cell_starting_with(
                row, "Вихідний залишок по рахунку на кінець періоду"
            ):
                entries.append(
                    self.balance_from_row(
                        meta, account, account_currency, row, cell_index
                    )
                )
                break
            elif str(row[1].value).startswith("Дата "):
                second_row = None
                if header == "card_ops":
                    second_row = sheet.row(nrow + 1)
                header_cols = self.header_cols_from_row(row, second_row)
                continue
            row_starts_with_date = bool(re.search(r"\d{2}\.\d{2}\.\d{4}", row[1].value))
            if row_starts_with_date:
                row_dict = self.row_to_dict(header_cols, row)
                if header == "account_ops":
                    entry = self.account_entry_from_row(
                        meta, account, account_currency, row_dict
                    )
                elif header == "card_ops":
                    entry = self.card_entry_from_row(meta, account, row_dict)
                if entry is not None:
                    entries.append(entry)
        return entries

    @classmethod
    def header_cols_from_row(cls, row, second_row=None):
        result = {}
        for i, col in enumerate(row):
            if col.value:
                if second_row is not None and second_row[i].value:
                    # take two consecutive headers from second row which is subheader row
                    result[f"{row[i].value} {second_row[i].value}"] = i
                    for j in range(i + 1, len(second_row)):
                        if second_row[j].value:
                            result[f"{row[i].value} {second_row[j].value}"] = j
                            break
                else:
                    result[col.value] = i
        return result

    @classmethod
    def row_to_dict(cls, header_cols, row):
        result = {}
        for header, col in header_cols.items():
            result[header] = row[col].value
        return result

    def balance_from_row(self, meta, account, currency, row, index):
        dt = dateutil.parser.parse(
            row[index].value.split(" ")[-1], dayfirst=True
        ) + timedelta(days=1)
        num = self.get_number(
            next(
                cell
                for cell in row[index + 1 :]
                if (cell.value is not None and cell.value != "")
            )
        )
        return data.Balance(
            meta,
            dt.date(),
            account,
            data.Amount(num, currency),
            None,
            None,
        )

    def account_entry_from_row(self, meta, account, currency, row_dict):
        date = dateutil.parser.parse(row_dict["Дата операціі"], dayfirst=True).date()
        if row_dict["Видатки"]:
            amount = data.Amount(-D(str(row_dict["Видатки"])), currency)
        else:
            amount = data.Amount(D(str(row_dict["Надходження"])), currency)
        postings = [
            data.Posting(
                account,
                amount,
                None,
                None,
                None,
                None,
            )
        ]
        if row_dict["Комісія"]:
            postings.append(
                data.Posting(
                    self.fee_account,
                    data.Amount(D(row_dict["Комісія"]), currency),
                    None,
                    None,
                    None,
                    None,
                )
            )
        narration = row_dict["Призначення платежу"]
        if re.search(self.deposit_regex, narration):
            postings.append(
                data.Posting(self.deposit_income_account, None, None, None, None, None)
            )
        payee = row_dict["Реквізити контрагента"].split("  ")[0]
        return data.Transaction(
            meta,
            date,
            self.FLAG,
            payee,
            narration,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def card_entry_from_row(self, meta, account, row_dict):
        dt = dateutil.parser.parse(row_dict["Дата і час операціі"], dayfirst=True)
        meta["time"] = dt.strftime("%H:%M")
        card_amount = data.Amount(
            D(str(row_dict["Карта сума"])), row_dict["Карта валюта"]
        )
        operation_amount = data.Amount(
            D(str(row_dict["Операція сума"])), row_dict["Операція валюта"]
        )
        if card_amount != operation_amount:
            rate = operation_amount.number / card_amount.number
            meta["converted"] = f"{operation_amount} ({rate})"
        postings = [data.Posting(account, card_amount, None, None, None, None)]
        return data.Transaction(
            meta,
            dt.date(),
            self.FLAG,
            None,
            row_dict["Призначення платежу"],
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )


def find_cell_starting_with(row, s):
    for i, cell in enumerate(row):
        if str(cell.value).startswith(s):
            return i
    return None


def get_test_importer():
    return Importer(
        {
            ("UAH", "26000000000000"): "Assets:Procreditbank:Cash:UAH",
            ("USD", "26000000000000"): "Assets:Procreditbank:Cash:USD",
        },
    )


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
