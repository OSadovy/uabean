from beancount.core import data
from beancount.core.amount import Amount
from beancount.core.number import ZERO, D
from beangulp.importers.csv import Col, CSVImporter


class Importer(CSVImporter):
    def __init__(self, account, currency, *args, **kwargs):
        if "business" in account.lower():
            fname_pattern = f"wise-business-.*-{currency}.csv"
        else:
            fname_pattern = f"wise-personal-.*-{currency}.csv"
        kwargs["matchers"] = [("filename", fname_pattern)]
        kwargs["dateutil_kwds"] = {"dayfirst": True}
        self.currency = currency
        super().__init__(
            {
                Col.AMOUNT: "Amount",
                Col.BALANCE: "Running Balance",
                Col.DATE: "Date",
                Col.NARRATION: "Description",
                Col.PAYEE: "Payee Name",
            },
            account,
            currency,
            categorizer=self.call_categorizer,
            *args,
            acount=account,
            **kwargs,
        )

    def call_categorizer(self, txn, row):
        txn.meta["lineno"] = -txn.meta["lineno"]
        # currency exchange
        if row[8] != row[7]:
            txn.meta["converted"] = f"{row[19]} {row[8]} ({row[9]})"
        if txn.narration == "No information" or txn.narration.startswith(
            "Card transaction of"
        ):
            txn = txn._replace(narration="")
        if row[13]:  # Merchant
            txn = txn._replace(payee=row[13])
        if row[4] and not row[4].startswith("Card transaction of"):  # Description
            txn.meta["src_desc"] = row[4]
        txn.meta["src_id"] = row[0]  # TransferWise ID
        total_fees = D(row[18])
        if not total_fees == ZERO:
            txn.postings.append(
                data.Posting(
                    "Expenses:Fees:Wise",
                    Amount(total_fees, self.currency),
                    None,
                    None,
                    None,
                    None,
                )
            )
        return txn


def get_test_importer():
    return Importer(account="Assets:Wise:EUR", currency="EUR")


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
