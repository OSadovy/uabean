"""Imports transactions from Kraken crypto exchange ledger csv file.

The CSV header is the following:
"txid","refid","time","type","subtype","aclass","asset","amount","fee","balance"
"""

import csv

import beangulp
import dateutil.parser
from beancount.core import data, flags
from beancount.core.amount import Amount
from beancount.core.number import D

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    FLAG = flags.FLAG_OKAY
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]

    def __init__(
        self,
        spot_account="Assets:Kraken:Spot",
        staking_account="Assets:Kraken:Staking",
        fee_account="Expenses:Fees:Kraken",
        staking_income_account="Income:Staking:Kraken",
    ):
        self.spot_account = spot_account
        self.staking_account = staking_account
        self.fee_account = fee_account
        self.staking_income_account = staking_income_account
        super().__init__()

    def account(self, _):
        return "kraken"

    def parse_date(self, s):
        return dateutil.parser.parse(s)

    def extract(self, filename, existing_entries=None):
        entries = []
        balances = {}
        for index, row in enumerate(csv.DictReader(open(filename)), 1):
            meta = data.new_metadata(filename, index)
            entry = self.get_entry_from_row(row, meta, balances)
            if entry is not None:
                entries.append(entry)
        for (account, currency), (date, balance) in balances.items():
            entries.append(
                data.Balance(
                    data.new_metadata(filename, -1),
                    date.date() + dateutil.relativedelta.relativedelta(days=1),
                    account,
                    Amount(balance, currency),
                    None,
                    None,
                )
            )
        return entries

    def get_entry_from_row(self, row, meta, balances):
        if not row["txid"]:
            return None
        date = self.parse_date(row["time"])
        meta["time"] = date.strftime("%H:%M:%S")
        postings = []
        match (row["type"], row["subtype"]):
            case ("deposit", "") | ("transfer", "spottostaking"):
                postings.append(
                    data.Posting(
                        self.spot_account,
                        self.ammount_from_row(row),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                self.update_balance(
                    balances,
                    date,
                    self.spot_account,
                    row["asset"],
                    D(row["balance"]),
                )
            case ("transfer", "stakingfromspot"):
                postings.append(
                    data.Posting(
                        self.staking_account,
                        self.ammount_from_row(row),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                self.update_balance(
                    balances,
                    date,
                    self.staking_account,
                    row["asset"],
                    D(row["balance"]),
                )
            case ("staking", ""):
                postings.append(
                    data.Posting(
                        self.staking_account,
                        self.ammount_from_row(row),
                        None,
                        None,
                        None,
                        None,
                    )
                )
                postings.append(
                    data.Posting(
                        self.staking_income_account,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                )
                self.update_balance(
                    balances,
                    date,
                    self.staking_account,
                    row["asset"],
                    D(row["balance"]),
                )
            case _:
                raise NotImplementedError(
                    f"Unknown transaction type {row['type']} row['subtype']"
                )
        if float(row["fee"]):
            postings.append(
                data.Posting(
                    self.fee_account,
                    self.ammount_from_row(row),
                    None,
                    None,
                    None,
                    None,
                )
            )
        return data.Transaction(
            meta,
            date.date(),
            self.FLAG,
            "",
            "",
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def ammount_from_row(self, row):
        return Amount(D(row["amount"]), row["asset"])

    def update_balance(self, balances, date, account, currency, balance):
        if (account, currency) not in balances:
            balances[(account, currency)] = (date, balance)
        else:
            balances[(account, currency)] = max(
                balances[(account, currency)], (date, balance)
            )


def get_test_importer():
    return Importer()


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
