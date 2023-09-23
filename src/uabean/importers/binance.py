"""Imports Binance statements from archive downloaded at wallet -> Transaction History -> generate all statements.

CSV Header:
User_ID,UTC_Time,Account,Operation,Coin,Change,Remark
"""

import csv
import io
import itertools
import tarfile

import beangulp
import dateutil.parser
from beancount.core import data, flags
from beancount.core.amount import Amount
from beancount.core.number import D


class Importer(beangulp.Importer):
    FLAG = flags.FLAG_OKAY

    def __init__(
        self,
        spot_wallet_account="Assets:Binance:Spot",
        fees_account="Expenses:Fees:Binance",
        p2p_account="Assets:Binance:P2P",
        savings_account="Assets:Binance:Savings",
        income_interest_account="Income:Binance:Interest",
        income_referal_account="Income:Binance:Referal",
        income_distributions_account="Income:Binance:Distribution",
        **kwargs,
    ):
        self.spot_wallet_account = spot_wallet_account
        self.fees_account = fees_account
        self.p2p_account = p2p_account
        self.savings_account = savings_account
        self.income_interest_account = income_interest_account
        self.income_referal_account = income_referal_account
        self.income_distributions_account = income_distributions_account
        super().__init__(**kwargs)

    def identify(self, filename):
        if not (filename.endswith(".tar.gz") or filename.endswith(".csv")):
            return False
        return self.get_csv_reader(filename).fieldnames == [
            "User_ID",
            "UTC_Time",
            "Account",
            "Operation",
            "Coin",
            "Change",
            "Remark",
        ]

    def get_csv_reader(self, filename):
        if filename.endswith(".csv"):
            return csv.DictReader(open(filename, errors="ignore"))
        elif filename.endswith(".tar.gz"):
            tar = tarfile.open(filename, "r:gz")
            for tarinfo in tar:
                f = tar.extractfile(tarinfo.name)
                return csv.DictReader(io.TextIOWrapper(f, "utf-8"))

    def account(self, _):
        return "_".join(p.lower() for p in self.spot_wallet_account.split(":")[1:-1])

    def parse_date(self, s):
        return dateutil.parser.parse(s)

    def extract(self, filename, existing_entries=None):
        entries = []
        prev_rows = []
        reader = self.get_csv_reader(filename)
        for _, rows in itertools.groupby(reader, key=lambda r: r["UTC_Time"]):
            lst_rows = list(rows)
            if {row["Operation"] for row in lst_rows} == {"Transaction Related"}:
                lst_rows = prev_rows + lst_rows
                entries.pop(-1)
            meta = data.new_metadata(filename, reader.line_num)
            transaction = self.transaction_from_rows(lst_rows, meta)
            prev_rows = lst_rows
            entries.append(transaction)
        return entries

    def transaction_from_rows(self, rows, meta):
        ops = {row["Operation"] for row in rows}
        ops = {
            "Small assets exchange" if o.startswith("Small assets exchange") else o
            for o in ops
        }
        accounts = {row["Account"].capitalize() for row in rows}
        dt = self.parse_date(rows[0]["UTC_Time"])
        if (
            accounts == {"Spot"}
            and "Deposit" in ops
            and ops.issubset({"Deposit", "Transaction Related"})
        ):
            narration = "Deposit"
            postings = [
                data.Posting(
                    self.spot_wallet_account,
                    amount_from_row(row),
                    None,
                    None,
                    None,
                    None,
                )
                for row in rows
            ]
        elif accounts == {"Spot"} and all(
            is_buy_row(r)
            or is_sell_row(r)
            or is_transaction_related_row(r)
            or is_fee_row(r)
            or is_referal_row(r)
            for r in rows
        ):
            quantity = D("0")
            coin = None
            sell = any(is_sell_row(r) for r in rows)
            other_leg = filter(lambda r: is_counter_row(r, sell), rows)
            postings = []
            for row in rows:
                units = amount_from_row(row)
                if (is_buy_row(row) and units.number > 0) or (
                    is_sell_row(row) and units.number < 0
                ):
                    quantity += D(row["Change"])
                    if coin is not None and not coin == row["Coin"]:
                        raise RuntimeError(
                            f"multiple coins bought: {coin} and {row['Coin']}. {row}"
                        )
                    coin = row["Coin"]
                    other_row = next(other_leg)
                    price = Amount(
                        abs(
                            round(D(other_row["Change"]) / D(row["Change"]), 6)
                        ).normalize(),
                        other_row["Coin"],
                    )
                    postings.append(
                        data.Posting(
                            self.spot_wallet_account, units, None, price, None, None
                        )
                    )
                elif is_fee_row(row):
                    postings += [
                        data.Posting(
                            self.spot_wallet_account, units, None, None, None, None
                        ),
                        data.Posting(
                            self.fees_account,
                            Amount(-D(row["Change"]), row["Coin"]),
                            None,
                            None,
                            None,
                            None,
                        ),
                    ]
                elif is_referal_row(row):
                    postings += [
                        data.Posting(
                            self.spot_wallet_account, units, None, None, None, None
                        ),
                        data.Posting(
                            self.income_referal_account,
                            Amount(-D(row["Change"]), row["Coin"]),
                            None,
                            None,
                            None,
                            None,
                        ),
                    ]
                else:
                    postings.append(
                        data.Posting(
                            self.spot_wallet_account, units, None, None, None, None
                        )
                    )
            quantity = quantity.normalize()
            if sell:
                narration = f"Sell {-quantity} {coin}"
            else:
                narration = f"Buy {quantity} {coin}"
        elif accounts == {"P2p"}:
            narration = rows[0]["Operation"]
            postings = [
                data.Posting(
                    self.p2p_account,
                    amount_from_row(row),
                    None,
                    None,
                    None,
                    None,
                )
                for row in rows
            ]
        elif accounts == {"Spot"} and ops.issubset(
            {"POS savings purchase", "Simple Earn Locked Subscription"}
        ):
            narration = "Savings"
            postings = []
            for row in rows:
                units = amount_from_row(row)
                postings += [
                    data.Posting(
                        self.spot_wallet_account, units, None, None, None, None
                    ),
                    data.Posting(
                        self.savings_account,
                        Amount(-units.number, units.currency),
                        None,
                        None,
                        None,
                        None,
                    ),
                ]
        elif accounts == {"Earn"} and ops == {"Simple Earn Flexible Redemption"}:
            narration = "Redemption"
            postings = []
            assert len(rows) == 2
            for row in rows:
                units = amount_from_row(row)
                if units.number < 0:
                    acc = self.savings_account
                else:
                    acc = self.spot_wallet_account
                postings.append(data.Posting(acc, units, None, None, None, None))
        elif accounts == {"Spot"} and ops == {"Simple Earn Locked Redemption"}:
            narration = "Redemption"
            assert len(rows) == 1
            units = amount_from_row(rows[0])
            postings = [
                data.Posting(self.savings_account, -units, None, None, None, None),
                data.Posting(self.spot_wallet_account, units, None, None, None, None),
            ]
        elif ops.issubset(
            {
                "POS savings interest",
                "Savings Interest",
                "Simple Earn Locked Rewards",
                "Simple Earn Flexible Interest",
            }
        ):
            narration = "Interest"
            postings = []
            for row in rows:
                units = amount_from_row(row)
                postings += [
                    data.Posting(
                        self.spot_wallet_account, units, None, None, None, None
                    ),
                    data.Posting(
                        self.income_interest_account,
                        Amount(-units.number, units.currency),
                        None,
                        None,
                        None,
                        None,
                    ),
                ]
        elif ops == {"Savings purchase"}:
            narration = "Savings"
            postings = []
            for row in rows:
                units = amount_from_row(row)
                postings.append(
                    data.Posting(
                        self.spot_wallet_account
                        if units.number < 0
                        else self.savings_account,
                        units,
                        None,
                        None,
                        None,
                        None,
                    )
                )
        elif accounts == {"Spot"} and ops == {"Withdraw"}:
            narration = "Withdraw"
            postings = [
                data.Posting(
                    self.spot_wallet_account,
                    amount_from_row(row),
                    None,
                    None,
                    None,
                    None,
                )
                for row in rows
            ]
        elif ops == {"Distribution"}:
            narration = "Distribution"
            postings = []
            for row in rows:
                units = amount_from_row(row)
                postings += [
                    data.Posting(
                        self.spot_wallet_account, units, None, None, None, None
                    ),
                    data.Posting(
                        self.income_distributions_account,
                        Amount(-units.number, units.currency),
                        None,
                        None,
                        None,
                        None,
                    ),
                ]
        else:
            raise RuntimeError(f"don't know how to process rows with operations {ops}")
        meta["time"] = dt.time().strftime("%H:%M:%S")
        return data.Transaction(
            meta,
            dt.date(),
            self.FLAG,
            None,
            narration,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )


def is_counter_row(row, sell=False):
    if sell:
        return (is_sell_row(row) or is_transaction_related_row(row)) and D(
            row["Change"]
        ) > 0
    return (is_buy_row(row) or is_transaction_related_row(row)) and D(row["Change"]) < 0


def is_transaction_related_row(row):
    return row["Operation"] in {"Transaction Related", "Transaction Revenue"}


def amount_from_row(row):
    return Amount(D(row["Change"]).normalize(), row["Coin"])


def is_buy_row(row):
    return row["Operation"] == "Buy" or row["Operation"].startswith(
        "Small assets exchange"
    )


def is_sell_row(row):
    return row["Operation"] in {"Sell", "Transaction Sold"}


def is_referal_row(row):
    return row["Operation"] in {
        "Referral Commission",
        "Commission Fee Shared With You",
        "Referral Kickback",
    }


def is_fee_row(row):
    return row["Operation"] == "Fee"


def get_test_importer():
    return Importer()


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
