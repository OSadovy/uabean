"""Imports statement csv fiels exported from tronscan.org UI.

The CSV header is the following:
transaction_id,block,block_ts,from_address,to_address,confirmed,contractRet,quant
"""

import csv

from beancount.core import flags
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.utils.date_utils import parse_date_liberally

from beancount.core.amount import Amount
from beancount.core.number import ZERO, D
from beancount.core import data


class Importer(IdentifyMixin, ImporterProtocol):
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]
    def __init__(self, account_prefix, currency, *args, **kwargs):
        self.account_prefix = account_prefix
        self.currency = currency  # currently it is not exported by tronscan ui
        super().__init__()

    def get_csv_reader(self, file):
        return csv.DictReader(open(file.name))

    def get_date_from_row(self, row):
        return parse_date_liberally(row["block_ts"])

    def extract(self, file, existing_entries=None):
        self.accounts = {}
        entries = []
        for e in existing_entries:
            if isinstance(e, data.Open):
                if "tron_address" in e.meta:
                    self.accounts[e.meta["tron_address"]] = e
        for index, row in enumerate(self.get_csv_reader(file), 1):
            meta = data.new_metadata(file.name, index)
            entry = self.get_entry_from_row(row, meta)
            if entry is not None:
                entries.append(entry)
        return entries

    def get_entry_from_row(self, row, meta):
        meta["src_tx_id"] = row["transaction_id"]
        meta["src_ts"] = row["block_ts"]
        txn = data.Transaction(meta, self.get_date_from_row(row), flags.FLAG_OKAY, "", "", data.EMPTY_SET, data.EMPTY_SET, [])
        from_account = self.get_account_for_address(row["from_address"])
        from_account_exists = row["from_address"] in self.accounts
        to_account = self.get_account_for_address(row["to_address"])
        to_account_exists = row["to_address"] in self.accounts
        txn.postings.append(data.Posting(from_account, Amount(-D(row["quant"]), self.currency), None, None, None if from_account_exists else "!", None))
        txn.postings.append(data.Posting(to_account, Amount(D(row["quant"]), self.currency), None, None, None if to_account_exists else "!", None))
        return txn

    def get_account_for_address(self, address):
        if address in self.accounts:
            return self.accounts[address].account
        return f"{self.account_prefix}:{address}"
