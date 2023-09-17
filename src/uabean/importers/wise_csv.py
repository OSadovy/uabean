import beancount.ingest.importers.csv
from beancount.core.amount import Amount
from beancount.core.number import ZERO, D
from beancount.core import data
from beancount.ingest.importers.csv import Col


class Importer(beancount.ingest.importers.csv.Importer):
    def __init__(self, *args, **kwargs):
        if "business" in kwargs["account"].lower():
            fname_pattern = f"wise-business-.*-{kwargs['currency']}"
        else:
            fname_pattern = f"wise-personal-.*-{kwargs['currency']}"
        kwargs["matchers"] = [("filename", fname_pattern)]
        kwargs["dateutil_kwds"] = {"dayfirst": True}
        super().__init__({
            Col.AMOUNT: "Amount",
            Col.BALANCE: "Running Balance",
            Col.DATE: "Date",
            Col.NARRATION: "Description",
            Col.PAYEE: "Payee Name",
        }, *args, **kwargs)

    def call_categorizer(self, txn, row):
        txn.meta["lineno"] = -txn.meta["lineno"]
        if txn.narration == "No information":
            txn = txn._replace(narration="")
        if row[13]:  # Merchant
            txn = txn._replace(payee=row[13])
        if row[4]:  # Description
            txn.meta["src_desc"] = row[4]
        txn.meta["src_id"] = row[0]  # TransferWise ID
        total_fees = D(row[18])
        if not total_fees == ZERO:
            txn.postings.append(data.Posting("Expenses:Fees:Wise", Amount(total_fees, self.currency), None, None, None, None))
        return super().call_categorizer(txn, row)
