"""Imports xml statements from privatbank, optained via p24-cli.

See https://github.com/dimboknv/p24-cli

The xml format is identical to that of p24 merchant api.
"""

from xml.etree import ElementTree as ET
import datetime
import dateutil.parser
import beangulp
from uabean.importers.mixins import IdentifyMixin
from beancount.core import data, flags
from beancount.core.number import D


class Importer(IdentifyMixin, beangulp.Importer):
    FLAG = flags.FLAG_OKAY
    matchers = [
        ("content", "<statements status="),
        ("mime", "application/xml"),
    ]
    unknown_account = "Assets:Unknown"

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

    def date_from_elem(self, elem):
        return dateutil.parser.parse(" ".join(elem.attrib["trandate"].split(" ")[:2]))

    def account(self, _):
        return "privatbank"

    def extract(self, filename, existing_entries=None):
        entries = []
        tree = ET.parse(filename)
        root = tree.getroot()
        assert root.tag == "statements"
        max_date = None
        max_elem = None
        for elem in root:
            meta = data.new_metadata(filename, 0)
            entries.append(self.entry_from_elem(meta, elem))
            date = self.date_from_elem(elem)
            if max_date is None or date > max_date:
                max_date = date
                max_elem = elem
        if max_elem is not None:
            rest_num, rest_currency = max_elem.attrib["rest"].split(" ", 1)
            amount = data.Amount(D(rest_num), rest_currency)
            entries.append(
                data.Balance(
                    data.new_metadata(filename, 0),
                    max_date.date() + datetime.timedelta(days=1),
                    self.card_to_account_map[max_elem.attrib["card"]],
                    amount,
                    None,
                    None,
                )
            )
        return entries

    def entry_from_elem(self, meta, elem):
        dt = self.date_from_elem(elem)
        meta["time"] = dt.strftime("%H:%M:%S")
        account = self.card_to_account_map.get(
            elem.attrib["card"], self.unknown_account
        )
        num, currency = elem.attrib["amount"].split(" ", 1)
        num = D(num)
        card_num, card_currency = elem.attrib["cardamount"].split(" ", 1)
        card_num = D(card_num)
        postings = [
            data.Posting(
                account, data.Amount(card_num, card_currency), None, None, None, None
            )
        ]
        if currency != card_currency:
            meta["converted"] = elem.attrib["amount"]
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
            elem.attrib["description"],
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )


def get_test_importer():
    return Importer(
        {
            "1234": "Assets:Privatbank:Universal",
            "5678": "Assets:Privatbank:Social",
        }
    )


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
