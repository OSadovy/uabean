"""Imports transactions csv files exported from Nexo crypto platform.

The CSV header is the following:
Transaction,Type,Input Currency,Input Amount,Output Currency,Output Amount,USD Equivalent,Details,(?:Outstanding Loan,)?Date / Time
"""

import csv

from beancount.core import flags
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.utils.date_utils import parse_date_liberally

from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core import data


class Importer(IdentifyMixin, ImporterProtocol):
    matchers = [
        ("content", __doc__.split("\n")[-2]),
        ("mime", "text/csv"),
    ]
    def __init__(self, asset_account, interest_account, *args, **kwargs):
        self.asset_account = asset_account
        self.interest_account = interest_account
        super().__init__(*args, **kwargs)

    def get_csv_reader(self, file):
        return csv.DictReader(open(file.name))

    def get_date_from_row(self, row):
        return parse_date_liberally(row["Date / Time"])

    def extract(self, file, existing_entries=None):
        entries = []
        for index, row in   enumerate(self.get_csv_reader(file), 1):
            meta = data.new_metadata(file.name, index)
            entry = self.get_entry_from_row(row, meta)
            if entry is not None:
                entries.append  (entry)
        return entries

    def get_entry_from_row(self, row, meta):
        meta["src_tx_id"] = row["Transaction"]
        meta["src_ts"] = row["Date / Time"]
        txn = data.Transaction(meta, self.get_date_from_row(row), flags.FLAG_OKAY, "", "", data.EMPTY_SET, data.EMPTY_SET, [])
        if row["Type"] == "Interest":
            txn.postings.append(data.Posting(self.asset_account, Amount(D(row["Input Amount"]), row["Input Currency"]), None, None, None, None))
            txn.postings.append(data.Posting(self.interest_account, Amount(-D(row["Output Amount"]), row["Output Currency"]), None, None, None, None))
        elif row["Type"] == "Deposit":
            txn.postings.append(data.Posting(self.asset_account, Amount(D(row["Input Amount"]), row["Input Currency"]), None, None, None, None))
            meta["src_details"] = row["Details"]
        elif row["Type"] == "Withdrawal":
            txn.postings.append(data.Posting(self.asset_account, Amount(D(row["Input Amount"]), row["Input Currency"]), None, None, None, None))
            meta["src_details"] = row["Details"]
        elif row["Type"] == "Exchange":
            txn.postings.append(data.Posting(self.asset_account, Amount(D(row["Input Amount"]), row["Input Currency"]), None, None, None, None))
            txn.postings.append(data.Posting(self.asset_account, Amount(D(row["Output Amount"]), row["Output Currency"]), None, None, None, None))
            meta["src_details"] = row["Details"]
        else:
            raise RuntimeError("Unknown row: %s" % row)
        return txn
