"""Imports statement csv fiels exported from tronscan.org UI.

The CSV header is the following:
transaction_id,block,block_ts,from_address,to_address,confirmed,contractRet,quant
"""

import csv

import beangulp
from beancount.core import data, flags
from beancount.core.amount import Amount
from beancount.core.number import D
from dateutil.parser import parse as parse_date

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]

    def __init__(self, account_prefix, currency, *args, **kwargs):
        self.account_prefix = account_prefix
        self.currency = currency  # currently it is not exported by tronscan ui
        super().__init__()

    def get_csv_reader(self, filename):
        return csv.DictReader(open(filename))

    def get_date_from_row(self, row):
        return parse_date(row["block_ts"]).date()

    def account(self, _):
        return "tron"

    def extract(self, filename, existing_entries=None):
        self.accounts = {}
        entries = []
        for e in existing_entries:
            if isinstance(e, data.Open):
                if "tron_address" in e.meta:
                    self.accounts[e.meta["tron_address"]] = e
        for index, row in enumerate(self.get_csv_reader(filename), 1):
            meta = data.new_metadata(filename, index)
            entry = self.get_entry_from_row(row, meta)
            if entry is not None:
                entries.append(entry)
        return entries

    def get_entry_from_row(self, row, meta):
        meta["src_tx_id"] = row["transaction_id"]
        meta["src_ts"] = row["block_ts"]
        txn = data.Transaction(
            meta,
            self.get_date_from_row(row),
            flags.FLAG_OKAY,
            "",
            "",
            data.EMPTY_SET,
            data.EMPTY_SET,
            [],
        )
        from_account = self.get_account_for_address(row["from_address"])
        from_account_exists = row["from_address"] in self.accounts
        to_account = self.get_account_for_address(row["to_address"])
        to_account_exists = row["to_address"] in self.accounts
        txn.postings.append(
            data.Posting(
                from_account,
                Amount(-D(row["quant"]), self.currency),
                None,
                None,
                None if from_account_exists else "!",
                None,
            )
        )
        txn.postings.append(
            data.Posting(
                to_account,
                Amount(D(row["quant"]), self.currency),
                None,
                None,
                None if to_account_exists else "!",
                None,
            )
        )
        return txn

    def get_account_for_address(self, address):
        if address in self.accounts:
            return self.accounts[address].account
        return f"{self.account_prefix}:{address}"


def get_test_importer():
    return Importer("Assets:Crypto:Tron", "USDT")


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
