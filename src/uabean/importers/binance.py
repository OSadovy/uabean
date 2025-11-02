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
            (
                "Small assets exchange"
                if o.lower().startswith("small assets exchange")
                else o
            )
            for o in ops
        }
        accounts = {row["Account"].capitalize() for row in rows}
        # Normalize "Funding" to "P2p" since Funding is the new name for P2P wallet
        accounts = {"P2p" if acc == "Funding" else acc for acc in accounts}
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
            sell = any(is_sell_row(r) for r in rows)
            narration, postings = self._handle_buy_sell_transaction(rows, sell)
        elif accounts == {"Spot", "P2p"} and ops == {
            "Transfer Between Main and Funding Wallet"
        }:
            narration = "Transfer"
            postings = []
            for row in rows:
                # Determine which account based on the original row Account field
                original_account = row["Account"].capitalize()
                if original_account == "Spot":
                    account = self.spot_wallet_account
                else:  # Funding (normalized to P2p)
                    account = self.p2p_account
                postings.append(
                    data.Posting(
                        account,
                        amount_from_row(row),
                        None,
                        None,
                        None,
                        None,
                    )
                )
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
            {
                "POS savings purchase",
                "Simple Earn Locked Subscription",
                "Simple Earn Flexible Subscription",
            }
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
        elif accounts == {"Spot"} and ops.issubset(
            {"Simple Earn Locked Redemption", "Simple Earn Flexible Redemption"}
        ):
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
                        (
                            self.spot_wallet_account
                            if units.number < 0
                            else self.savings_account
                        ),
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
        elif accounts == {"Spot"} and ops.issubset(
            {"Asset Recovery", "Token Swap - Distribution"}
        ):
            # Single-posting transactions for asset recovery and token swaps
            # User will manually balance with corresponding entries
            narration = rows[0]["Operation"]
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
        elif accounts == {"Spot"} and ops.issubset({"Merchant Acquiring", "Send"}):
            # Binance Pay merchant payments and sends - use Remark as narration
            remark = rows[0].get("Remark", "").strip()
            narration = remark if remark else rows[0]["Operation"]
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

    def _handle_buy_sell_transaction(self, rows, sell):
        """Handle Buy, Sell, and Small Assets Exchange operations."""
        try:
            # Separate exchange rows from fee/referral rows
            exchange_rows = [
                r
                for r in rows
                if is_buy_row(r) or is_sell_row(r) or is_transaction_related_row(r)
            ]

            # If there are no exchange rows, only fee/referral rows,
            # treat as standalone income/expense transaction
            if not exchange_rows:
                fee_referral_rows = [
                    r for r in rows if is_fee_row(r) or is_referal_row(r)
                ]
                if fee_referral_rows:
                    # Use the operation name as narration
                    narration = fee_referral_rows[0]["Operation"]
                    postings = self._build_fee_referral_postings(fee_referral_rows)
                    return narration, postings
                else:
                    raise RuntimeError(
                        "No exchange rows and no fee/referral rows found. "
                        "All rows were filtered out."
                    )

            # Pair the exchange rows
            pairs = self._pair_exchange_rows(exchange_rows, sell)

            # Build a mapping from buy rows to their paired sell rows for price lookup
            pair_map = {id(buy_row): sell_row for buy_row, sell_row in pairs}

            # Track which rows are exchange-related (buy or counter)
            exchange_row_ids = {id(r) for r in exchange_rows}

            # Build postings in original row order
            postings = []
            for row in rows:
                if is_fee_row(row) or is_referal_row(row):
                    # Add fee/referral postings inline
                    postings.extend(self._build_fee_referral_postings([row]))
                elif id(row) in pair_map:
                    # This is a buy row - add posting with price
                    sell_row = pair_map[id(row)]
                    buy_units = amount_from_row(row)
                    price = Amount(
                        abs(
                            round(D(sell_row["Change"]) / D(row["Change"]), 6)
                        ).normalize(),
                        sell_row["Coin"],
                    )
                    postings.append(
                        data.Posting(
                            self.spot_wallet_account, buy_units, None, price, None, None
                        )
                    )
                elif id(row) in exchange_row_ids:
                    # This is a counter/exchange row - add posting without price
                    units = amount_from_row(row)
                    postings.append(
                        data.Posting(
                            self.spot_wallet_account, units, None, None, None, None
                        )
                    )

            # Build narration
            narration = self._build_exchange_narration(pairs, sell)

            return narration, postings
        except Exception as e:
            # Add context about which rows failed
            rows_str = "\n".join(
                f"  {row['UTC_Time']} {row['Account']} {row['Operation']} "
                f"{row['Coin']} {row['Change']} Remark='{row.get('Remark', '')}'"
                for row in rows
            )
            raise RuntimeError(
                f"Failed to process buy/sell transaction:\n{rows_str}\n\nOriginal error: {e}"
            ) from e

    def _pair_exchange_rows(self, rows, sell):
        """Pair exchange rows using Remark field when available, else sequential."""
        # Check if Remarks are populated
        has_remarks = any(row.get("Remark") and row["Remark"].strip() for row in rows)
        has_empty_remarks = any(
            not row.get("Remark") or not row["Remark"].strip() for row in rows
        )

        # Strict: either all have remarks or all are empty
        if has_remarks and has_empty_remarks:
            rows_str = "\n".join(
                f"  {row['UTC_Time']} {row['Operation']} {row['Coin']} {row['Change']} Remark='{row.get('Remark', '')}'"
                for row in rows
            )
            raise RuntimeError(
                f"Mixed Remark usage: some rows have Remarks, others don't. "
                f"All rows must consistently use or not use Remarks.\nRows:\n{rows_str}"
            )

        if has_remarks:
            return self._pair_by_remark(rows, sell)
        else:
            return self._pair_sequentially(rows, sell)

    def _pair_by_remark(self, rows, sell):
        """Group and pair rows by Remark field."""
        # Group by remark
        groups = {}
        for row in rows:
            remark = row.get("Remark", "").strip()
            if not remark:
                continue
            groups.setdefault(remark, []).append(row)

        pairs = []
        for remark, group_rows in groups.items():
            # Identify buy and sell sides
            buy_rows = [
                r
                for r in group_rows
                if (is_buy_row(r) or (is_transaction_related_row(r) and not sell))
                and D(r["Change"]) > 0
            ]
            sell_rows = [
                r
                for r in group_rows
                if (is_buy_row(r) or is_sell_row(r) or is_transaction_related_row(r))
                and D(r["Change"]) < 0
            ]

            # Validate pairing
            if len(buy_rows) != 1 or len(sell_rows) != 1:
                group_rows_str = "\n".join(
                    f"    {r['UTC_Time']} {r['Operation']} {r['Coin']} {r['Change']}"
                    for r in group_rows
                )
                raise RuntimeError(
                    f"Remark '{remark}' has mismatched legs: "
                    f"{len(buy_rows)} buy rows, {len(sell_rows)} sell rows. "
                    f"Expected 1:1 pairing.\n"
                    f"  Rows with Remark '{remark}':\n{group_rows_str}"
                )

            pairs.append((buy_rows[0], sell_rows[0]))

        return pairs

    def _pair_sequentially(self, rows, sell):
        """Legacy sequential pairing for rows without Remark field."""
        buy_rows = []
        counter_rows = []

        for row in rows:
            # Determine buy vs counter based on Change sign
            # For operations that can be both buy and sell (like Binance Convert),
            # use the sign of Change to determine the role
            change = D(row["Change"])

            if sell:
                # For sell operations: negative Change is primary (being sold)
                if change < 0:
                    buy_rows.append(row)
                elif change > 0:
                    counter_rows.append(row)
            else:
                # For buy operations: positive Change is primary (being bought)
                if change > 0:
                    buy_rows.append(row)
                elif change < 0:
                    counter_rows.append(row)

        pairs = list(zip(buy_rows, counter_rows))
        return pairs

    def _build_fee_referral_postings(self, fee_referral_rows):
        """Build postings for fee and referral rows."""
        postings = []
        for row in fee_referral_rows:
            units = amount_from_row(row)
            if is_fee_row(row):
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
        return postings

    def _build_exchange_narration(self, pairs, sell):
        """Build narration from exchange pairs."""
        # Check if we have any pairs
        if not pairs:
            raise RuntimeError(
                "No pairs found for exchange transaction. "
                "This likely means all rows were filtered out or misclassified. "
                "Check that rows have correct Operation types."
            )

        # Check if this is a Binance Convert operation
        is_convert = any(
            row["Operation"] == "Binance Convert"
            for buy_row, sell_row in pairs
            for row in [buy_row, sell_row]
        )

        # Check if all buy sides are the same coin
        buy_coins = set(buy_row["Coin"] for buy_row, _ in pairs)

        if len(buy_coins) == 1:
            # Single target coin - sum quantities
            buy_coin = buy_coins.pop()
            total_quantity = sum(
                D(buy_row["Change"]) for buy_row, _ in pairs
            ).normalize()

            if is_convert:
                # For convert operations, show "Convert X to Y"
                # Find the source (negative change) and target (positive change)
                source_rows = [
                    (buy_row, sell_row)
                    for buy_row, sell_row in pairs
                    if D(buy_row["Change"]) < 0
                ]
                target_rows = [
                    (buy_row, sell_row)
                    for buy_row, sell_row in pairs
                    if D(buy_row["Change"]) > 0
                ]

                if not source_rows:
                    source_rows = [
                        (buy_row, sell_row)
                        for buy_row, sell_row in pairs
                        if D(sell_row["Change"]) < 0
                    ]
                if not target_rows:
                    target_rows = [
                        (buy_row, sell_row)
                        for buy_row, sell_row in pairs
                        if D(sell_row["Change"]) > 0
                    ]

                if source_rows and target_rows:
                    source_coin = (
                        source_rows[0][0]["Coin"]
                        if D(source_rows[0][0]["Change"]) < 0
                        else source_rows[0][1]["Coin"]
                    )
                    source_amount = abs(
                        D(source_rows[0][0]["Change"])
                        if D(source_rows[0][0]["Change"]) < 0
                        else D(source_rows[0][1]["Change"])
                    ).normalize()
                    target_coin = (
                        target_rows[0][0]["Coin"]
                        if D(target_rows[0][0]["Change"]) > 0
                        else target_rows[0][1]["Coin"]
                    )
                    return f"Convert {source_amount} {source_coin} to {target_coin}"
                else:
                    return f"Convert to {buy_coin}"
            elif sell:
                return f"Sell {-total_quantity} {buy_coin}"
            else:
                return f"Buy {total_quantity} {buy_coin}"
        else:
            # Multiple different target coins - not anticipated
            pairs_str = "\n".join(
                f"  Buy: {buy_row['UTC_Time']} {buy_row['Coin']} {buy_row['Change']} | "
                f"Sell: {sell_row['Coin']} {sell_row['Change']}"
                for buy_row, sell_row in pairs
            )
            raise RuntimeError(
                f"Multiple different target coins not supported.\n"
                f"Found {len(buy_coins)} different buy-side coins: {buy_coins}\n"
                f"Pairs:\n{pairs_str}"
            )


def is_counter_row(row, sell=False):
    if sell:
        return (is_sell_row(row) or is_transaction_related_row(row)) and D(
            row["Change"]
        ) > 0
    return (is_buy_row(row) or is_transaction_related_row(row)) and D(row["Change"]) < 0


def is_transaction_related_row(row):
    return row["Operation"] in {
        "Transaction Related",
        "Transaction Revenue",
        "Transaction Spend",
    }


def amount_from_row(row):
    return Amount(D(row["Change"]).normalize(), row["Coin"])


def is_buy_row(row):
    return row["Operation"] in {"Buy", "Transaction Buy", "Binance Convert"} or row[
        "Operation"
    ].lower().startswith("small assets exchange")


def is_sell_row(row):
    return row["Operation"] in {"Sell", "Transaction Sold", "Binance Convert"}


def is_referal_row(row):
    return row["Operation"] in {
        "Referral Commission",
        "Commission Fee Shared With You",
        "Referral Kickback",
    }


def is_fee_row(row):
    return row["Operation"] in {"Fee", "Transaction Fee"}


def get_test_importer():
    return Importer()


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
