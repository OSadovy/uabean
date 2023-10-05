"""Imports balance statements output by Wise API in json format.
"""

import decimal
import json
from datetime import timedelta

import beangulp
import dateutil.parser
from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.data import Balance, Posting, Transaction, new_metadata

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    FLAG = flags.FLAG_OKAY

    def __init__(self, account, fees_account="Expenses:Fees:Wise", **kwargs):
        self._account = account
        self.fees_account = fees_account
        if "business" in account.lower():
            type_pattern = "business"
        elif "personal" in account.lower():
            type_pattern = "personal"
        else:
            type_pattern = ".*?"
        fname_pattern = "wise-" + type_pattern + r"-.*-\w{3}"
        kwargs["matchers"] = [("filename", fname_pattern)]
        super().__init__(**kwargs)

    def file_account(self, filename):
        # example: wise-business-2022-01-01_2022-10-01-USD.json
        parts = filename.split("-")
        account_type = parts[1].capitalize()
        currency = parts[-1].split(".", 1)[0]
        return self._account.format(type=account_type, currency=currency)

    def account(self, _):
        return "wise"

    @staticmethod
    def date_from_str(s):
        return dateutil.parser.parse(s)

    def data_from_file(self, filename):
        return json.load(open(filename, encoding="utf-8"), parse_float=decimal.Decimal)

    def date(self, filename):
        data = self.data_from_file(filename)
        return data["query"]["intervalEnd"]

    def extract(self, filename, existing_entries=None):
        data = self.data_from_file(filename)
        entries = []
        account = self.file_account(filename)
        for i, t in enumerate(data["transactions"]):
            meta = new_metadata(filename, -(i + 1))
            entries.append(self.entry_from_transaction(meta, account, t))
        if data["transactions"]:
            entries.append(
                Balance(
                    new_metadata(filename, -(i + 1)),
                    (
                        self.date_from_str(data["query"]["intervalEnd"])
                        + timedelta(days=1)
                    ).date(),
                    account,
                    amount_from_obj(data["endOfStatementBalance"]),
                    None,
                    None,
                )
            )
        return entries

    def entry_from_transaction(self, meta, account, t):
        dt = self.date_from_str(t["date"])
        meta["time"] = dt.time().strftime("%H:%M:%S")
        meta["src_id"] = t["referenceNumber"]
        if t["exchangeDetails"] is not None:
            meta[
                "converted"
            ] = f'{t["exchangeDetails"]["toAmount"]["value"]} {t["exchangeDetails"]["toAmount"]["currency"]} ({t["exchangeDetails"]["rate"]})'
        postings = [
            Posting(account, amount_from_obj(t["amount"]), None, None, None, None)
        ]
        if not t["totalFees"]["zero"]:
            postings.append(
                Posting(
                    self.fees_account,
                    amount_from_obj(t["totalFees"]),
                    None,
                    None,
                    None,
                    None,
                )
            )
        match t["details"]["type"]:
            case "TRANSFER":
                payee = t["details"]["recipient"]["name"]
            case "CARD":
                payee = t["details"]["merchant"]["name"]
                meta["src_category"] = t["details"]["category"]
            case "DEPOSIT":
                payee = t["details"]["senderName"]
            case "UNKNOWN":
                payee = None
            case "MONEY_ADDED":
                payee = "self"
            case _:
                raise ValueError(f"unknown transaction type: {t['details']['type']}")
        return Transaction(
            meta, dt.date(), self.FLAG, payee, None, set(), set(), postings
        )


def amount_from_obj(obj):
    return Amount(obj["value"], obj["currency"])


def get_test_importer():
    return Importer(account="Assets:Wise:personal:{currency}")


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
