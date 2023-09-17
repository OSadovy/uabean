"""Imports balance statements output by Wise API in json format.
"""

from datetime import timedelta
import decimal
import dateutil.parser
import json
from beancount.ingest.importer import ImporterProtocol
from beancount.ingest.importers.mixins.identifier import IdentifyMixin
from beancount.core.amount import Amount
from beancount.core.data import Balance, Transaction, Posting, new_metadata


class Importer(IdentifyMixin, ImporterProtocol):
    def __init__(self, account, fees_account="Expenses:Fees:Wise", **kwargs):
        self.account = account
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

    def file_account(self, file):
        # example: wise-business-2022-01-01_2022-10-01-USD.json
        parts = file.name.split("-")
        account_type = parts[1].capitalize()
        currency = parts[-1].split(".", 1)[0]
        return self.account.format(type=account_type, currency=currency)

    @staticmethod
    def date_from_str(s):
        return dateutil.parser.parse(s)

    def data_from_file(self, file):
        return json.load(open(file.name, encoding="utf-8"), parse_float=decimal.Decimal)

    def file_date(self, file):
        data = self.data_from_file(file)
        return data["query"]["intervalEnd"]

    def extract(self, file, existing_entries=None):
        data = self.data_from_file(file)
        entries = []
        account = self.file_account(file)
        for i, t in enumerate(data["transactions"]):
            meta = new_metadata(file.name, -(i + 1))
            entries.append(self.entry_from_transaction(meta, account, t))
        if data["transactions"]:
            entries.append(
                Balance(
                    new_metadata(file.name, -(i + 1)),
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
