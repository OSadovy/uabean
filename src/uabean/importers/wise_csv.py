import dateutil.parser
from beancount.core import data
from beancount.core.amount import Amount
from beancount.core.number import ZERO, D
from beangulp import cache
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

        is_new_format = len(row) >= 23

        if is_new_format:
            col_date_time = 2
            col_description = 5
            col_exchange_from = 8
            col_exchange_to = 9
            col_exchange_rate = 10
            col_merchant = 14
            col_total_fees = 19
            col_exchange_to_amount = 20
        else:
            col_description = 4
            col_exchange_from = 7
            col_exchange_to = 8
            col_exchange_rate = 9
            col_merchant = 13
            col_total_fees = 18
            col_exchange_to_amount = 19

        col_transferwise_id = 0

        if is_new_format and row[col_date_time]:
            try:
                dt = dateutil.parser.parse(row[col_date_time], dayfirst=True)
                txn.meta["time"] = dt.strftime("%H:%M:%S")
            except (ValueError, AttributeError):
                pass

        if row[col_exchange_from] != row[col_exchange_to]:
            txn.meta["converted"] = (
                f"{row[col_exchange_to_amount]} {row[col_exchange_to]} ({row[col_exchange_rate]})"
            )

        if txn.narration == "No information" or txn.narration.startswith(
            "Card transaction of"
        ):
            txn = txn._replace(narration="")

        if row[col_merchant]:
            txn = txn._replace(payee=row[col_merchant])

        if row[col_description] and not row[col_description].startswith(
            "Card transaction of"
        ):
            txn.meta["src_desc"] = row[col_description]

        txn.meta["src_id"] = row[col_transferwise_id]

        total_fees = D(row[col_total_fees])
        if total_fees != ZERO:
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

    def account(self, _):
        return "wise_csv"

    def extract(self, filepath, existing=None):
        account = self.filing.filing_account
        return self.base._do_extract(cache.get_file(filepath), account, existing)


def get_test_importer():
    return Importer(account="Assets:Wise:EUR", currency="EUR")


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
