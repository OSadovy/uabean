"""
This is a beancount importer for Interactive Brokers.

Inspired by https://github.com/Dr-Nuke/drnuke-bean/blob/master/src/drnukebean/importer/ibkr.py

Supports selling short, corporate actions and more.

It takes FlexQuery xml as input. The report should include all fields for these sections:
* Trades
 * Options:Executions, Closed Lots
* Cash Transactions
 * Options:Dividends, Payment in Lieu of Dividends, Withholding Tax, 871(m) Withholding, Advisor Fees, Other Fees, Deposits/Withdrawals, Carbon Credits, Bill Pay, Broker Interest Paid, Broker Interest Received, Broker Fees, Bond Interest Paid, Bond Interest Received, Price Adjustments, Commission Adjustments, Detail
* Cash Report
 * Options:Currency Breakout
* Corporate Actions
 * Options:Detail

The XML can be downloaded from interactive brokers manually or via an API.
"""

import functools
import re
import warnings
from collections import defaultdict
from datetime import timedelta

import beangulp
from beancount.core import amount, data, flags, position, realization
from beancount.core.number import Decimal
from ibflex import Types, parser
from ibflex.enums import BuySell, CashAction, OpenClose, Reorg

from uabean.importers.mixins import IdentifyMixin


class Importer(IdentifyMixin, beangulp.Importer):
    """
    Beancount Importer for the Interactive Brokers XML FlexQueries
    """

    matchers = [
        ("mime", "application/xml"),
        ("content", "<FlexQueryResponse "),
    ]

    def __init__(
        self,
        cash_account="Assets:Investments:IB:Cash",
        assets_account="Assets:Investments:IB:{symbol}",
        div_account="Income:Investments:IB:{symbol}:Div",
        interest_account="Income:Investments:IB:Interest",
        wht_account="Expenses:Investments:IB:WithholdingTax",
        fees_account="Expenses:IB:Fees",
        pnl_account="Income:Investments:IB:{symbol}:PnL",
        document_archiving_account="ib",
        use_existing_holdings=True,
        **kwargs,
    ):
        self.cash_account = cash_account
        self.assets_account = assets_account
        self.div_account = div_account
        self.interest_account = interest_account
        self.wht_account = wht_account
        self.fees_account = fees_account
        self.bnl_account = pnl_account
        self.document_archiving_account = document_archiving_account
        self.use_existing_holdings = use_existing_holdings
        self.holdings_map = defaultdict(list)
        super().__init__(**kwargs)

    def get_account_name(self, account_str, symbol=None, currency=None):
        if symbol is not None:
            account_str = account_str.replace("{symbol}", symbol.replace(" ", ""))
        if currency is not None:
            account_str = account_str.replace("{currency}", currency)
        return account_str

    def get_liquidity_account(self, currency):
        return self.get_account_name(self.cash_account, currency=currency)

    def get_div_income_account(self, currency, symbol):
        return self.get_account_name(self.div_account, symbol=symbol, currency=currency)

    def get_interest_income_account(self, currency):
        return self.get_account_name(self.interest_account, currency=currency)

    def get_asset_account(self, symbol):
        return self.get_account_name(self.assets_account, symbol=symbol)

    def get_wht_account(self, symbol):
        return self.get_account_name(self.wht_account, symbol=symbol)

    def get_fees_account(self, currency):
        return self.get_account_name(self.fees_account, currency=currency)

    def get_pnl_account(self, symbol):
        return self.get_account_name(self.bnl_account, symbol=symbol)

    def account(self, filename):
        return self.document_archiving_account

    def extract(self, filename, existing_entries=None):
        if self.use_existing_holdings and existing_entries is not None:
            self.holdings_map = self.get_holdings_map(existing_entries)
        else:
            self.holdings_map = defaultdict(list)
        statement = parser.parse(open(filename))
        assert isinstance(statement, Types.FlexQueryResponse)
        poi = statement.FlexStatements[0]  # point of interest
        transactions = (
            self.Trades(poi.Trades)
            + self.cash_transactions(poi.CashTransactions)
            + self.Balances(poi.CashReport)
            + self.corporate_actions(poi.CorporateActions)
        )

        transactions = self.merge_dividend_and_withholding(transactions)
        # self.adjust_closing_trade_cost_basis(transactions)
        return self.autoopen_accounts(transactions, existing_entries) + transactions

    def cash_transactions(self, ct):
        transactions = []
        for index, row in enumerate(ct):
            if row.type == CashAction.DEPOSITWITHDRAW:
                transactions.append(self.deposit_from_row(index, row))
            elif row.type in (CashAction.BROKERINTRCVD, CashAction.BROKERINTPAID):
                transactions.append(self.Interest_from_row(index, row))
            elif row.type in (CashAction.FEES, CashAction.COMMADJ):
                transactions.append(self.fee_from_row(index, row))
            elif row.type in (
                CashAction.WHTAX,
                CashAction.DIVIDEND,
                CashAction.PAYMENTINLIEU,
            ):
                transactions.append(
                    self.dividends_and_withholding_tax_from_row(index, row)
                )
            else:
                raise RuntimeError(f"Unknown cash transaction type: {row.type}")
        return transactions

    def fee_from_row(self, idx, row):
        amount_ = amount.Amount(row.amount, row.currency)
        text = row.description
        try:
            month = re.findall(r"\w{3} \d{4}", text)[0]
            narration = " ".join(["Fee", row.currency, month])
        except IndexError:
            narration = text

        # make the postings, two for fees
        postings = [
            data.Posting(
                self.get_fees_account(row.currency), -amount_, None, None, None, None
            ),
            data.Posting(
                self.get_liquidity_account(row.currency),
                amount_,
                None,
                None,
                None,
                None,
            ),
        ]
        meta = data.new_metadata(__file__, 0, {"descr": text})
        return data.Transaction(
            meta,
            row.reportDate,
            flags.FLAG_OKAY,
            "IB",  # payee
            narration,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def dividends_and_withholding_tax_from_row(self, idx, row):
        """Converts dividends, payment inlieu of dividends and withholding tax to a beancount transaction.
        Stores div type in metadata for the merge step to be able to match tax withdrawals to the correct div.
        """
        amount_ = amount.Amount(row.amount, row.currency)

        text = row.description
        # Find ISIN in description in parentheses
        isin = re.findall(r"\(([a-zA-Z]{2}[a-zA-Z0-9]{9}\d)\)", text)[0]
        pershare_match = re.search(r"(\d*[.]\d*)(\D*)(PER SHARE)", text, re.IGNORECASE)
        # payment in lieu of a dividend does not have a PER SHARE in description
        pershare = pershare_match.group(1) if pershare_match else ""

        meta = {"isin": isin, "per_share": pershare}
        if row.type == CashAction.WHTAX:
            account = self.get_wht_account(row.symbol)
            type_ = (
                CashAction.PAYMENTINLIEU
                if re.search("payment in lieu of dividend", text, re.IGNORECASE)
                else CashAction.DIVIDEND
            )
        else:
            account = self.get_div_income_account(row.currency, row.symbol)
            type_ = row.type
            meta["div"] = True
        meta["div_type"] = type_.value
        postings = [
            data.Posting(account, -amount_, None, None, None, None),
            data.Posting(
                self.get_liquidity_account(row.currency),
                amount_,
                None,
                None,
                None,
                None,
            ),
        ]
        meta = data.new_metadata(
            "dividend",
            0,
            meta,
        )

        return data.Transaction(
            meta,
            row.reportDate,
            flags.FLAG_OKAY,
            row.symbol,  # payee
            text,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def Interest_from_row(self, idx, row):
        amount_ = amount.Amount(row.amount, row.currency)
        text = row.description
        month = re.findall(r"\w{3}-\d{4}", text)[0]

        # make the postings, two for interest payments
        # received and paid interests are booked on the same account
        postings = [
            data.Posting(
                self.get_interest_income_account(row.currency),
                -amount_,
                None,
                None,
                None,
                None,
            ),
            data.Posting(
                self.get_liquidity_account(row.currency),
                amount_,
                None,
                None,
                None,
                None,
            ),
        ]
        meta = data.new_metadata("Interest", 0)
        return data.Transaction(
            meta,
            row.reportDate,
            flags.FLAG_OKAY,
            "IB",  # payee
            " ".join(["Interest ", row.currency, month]),
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def deposit_from_row(self, idx, row):
        amount_ = amount.Amount(row.amount, row.currency)
        postings = [
            data.Posting(
                self.get_liquidity_account(row.currency),
                amount_,
                None,
                None,
                None,
                None,
            ),
        ]
        meta = data.new_metadata("deposit/withdrawal", 0)
        return data.Transaction(
            meta,
            row.reportDate,
            flags.FLAG_OKAY,
            "self",  # payee
            row.description,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def Trades(self, tr):
        # forex transactions
        fx = [t for t in tr if is_forex(t.symbol)]
        # Stocks transactions
        stocks = [t for t in tr if not is_forex(t.symbol)]

        return self.forex(fx) + self.stock_trades(stocks)

    def forex(self, fx):
        transactions = []
        for idx, row in enumerate(fx):
            symbol = row.symbol
            curr_prim, curr_sec = get_forex_currencies(symbol)
            currency_IBcommision = row.ibCommissionCurrency
            proceeds = amount.Amount(row.proceeds, curr_sec)
            quantity = amount.Amount(row.quantity, curr_prim)
            price = amount.Amount(row.tradePrice, curr_sec)
            commission = amount.Amount(row.ibCommission, currency_IBcommision)
            buysell = row.buySell.name

            postings = [
                data.Posting(
                    self.get_liquidity_account(curr_prim),
                    quantity,
                    None,
                    price,
                    None,
                    None,
                ),
                data.Posting(
                    self.get_liquidity_account(curr_sec),
                    proceeds,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.get_liquidity_account(currency_IBcommision),
                    commission,
                    None,
                    None,
                    None,
                    None,
                ),
                data.Posting(
                    self.get_fees_account(currency_IBcommision),
                    minus(commission),
                    None,
                    None,
                    None,
                    None,
                ),
            ]

            transactions.append(
                data.Transaction(
                    data.new_metadata("FX Transaction", idx),
                    row.tradeDate,
                    flags.FLAG_OKAY,
                    symbol,  # payee
                    " ".join([buysell, quantity.to_string(), "@", price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )
        return transactions

    def stock_trades(self, trades):
        """Generates transactions for IB stock trades.
        Tries to keep track of available holdings to disambiguate sales when lots are not enough,
        e.g. when there were multiple buys of the same symbol on the specific date.
        Currently, it does not take into account comission when calculating cost for stocks,
        just the trade price. It keeps the "real" cost as "ib_cost" metadata field though, which might be utilized in the future.
        It is mostly because I find the raw unafected price nicer to see in my beancount file.
        It also creates the fee posting for comission with "C" flag to distinguish it from other postings.
        """
        transactions = []
        for row, lots in iter_trades_with_lots(trades):
            if row.buySell in (BuySell.SELL, BuySell.CANCELSELL):
                op = "SELL"
            elif row.buySell in (BuySell.BUY, BuySell.CANCELBUY):
                op = "BUY"
            else:
                raise RuntimeError(f"Unknown buySell value: {row.buySell}")
            currency = row.currency
            currency_IBcommision = row.ibCommissionCurrency
            symbol = row.symbol
            net_cash = amount.Amount(row.netCash, currency)
            commission = amount.Amount(row.ibCommission, currency_IBcommision)
            quantity = amount.Amount(row.quantity, get_currency_from_symbol(symbol))
            price = amount.Amount(row.tradePrice, currency)
            date = row.dateTime.date()

            if row.openCloseIndicator == OpenClose.OPEN:
                self.add_holding(row)
                cost = position.CostSpec(
                    number_per=price.number,
                    number_total=None,
                    currency=currency,
                    date=row.tradeDate,
                    label=None,
                    merge=False,
                )
                lotpostings = [
                    data.Posting(
                        self.get_asset_account(symbol),
                        quantity,
                        cost,
                        price,
                        None,
                        {"ib_cost": row.cost},
                    ),
                ]
            else:
                lotpostings = []
                for clo in lots:
                    try:
                        clo_price = self.get_and_reduce_holding(clo)
                    except ValueError as e:
                        warnings.warn(str(e))
                        clo_price = None
                    cost = position.CostSpec(
                        clo_price,
                        number_total=None,
                        currency=clo.currency,
                        date=clo.openDateTime.date(),
                        label=None,
                        merge=False,
                    )

                    lotpostings.append(
                        data.Posting(
                            self.get_asset_account(symbol),
                            amount.Amount(
                                -clo.quantity, get_currency_from_symbol(clo.symbol)
                            ),
                            cost,
                            price,
                            None,
                            {"ib_cost": clo.cost},
                        )
                    )

                lotpostings.append(
                    data.Posting(
                        self.get_pnl_account(symbol), None, None, None, None, None
                    )
                )
            postings = (
                [
                    data.Posting(
                        self.get_liquidity_account(currency),
                        net_cash,
                        None,
                        None,
                        None,
                        None,
                    )
                ]
                + lotpostings
                + [
                    data.Posting(
                        self.get_fees_account(currency_IBcommision),
                        minus(commission),
                        None,
                        None,
                        "C",
                        None,
                    )
                ]
            )

            transactions.append(
                data.Transaction(
                    data.new_metadata("trade", 0),
                    date,
                    flags.FLAG_OKAY,
                    symbol,  # payee
                    " ".join([op, quantity.to_string(), "@", price.to_string()]),
                    data.EMPTY_SET,
                    data.EMPTY_SET,
                    postings,
                )
            )

        return transactions

    def add_holding(self, row):
        holdings = self.holdings_map[
            (row.dateTime.date(), get_currency_from_symbol(row.symbol))
        ]
        for holding in holdings:
            if holding[2] == row.cost / row.quantity:
                holding[0] += row.quantity
                return
        holdings.append([row.quantity, row.tradePrice, row.cost / row.quantity])

    def get_and_reduce_holding(self, lot):
        holdings = self.holdings_map[
            (lot.openDateTime.date(), get_currency_from_symbol(lot.symbol))
        ]
        for i, holding in enumerate(holdings):
            quantity, price, real_price = holding
            if not (
                round(real_price, 4) == round(lot.cost / lot.quantity, 4)
                or (
                    quantity == lot.quantity
                    and round(real_price, 2) == round(lot.cost / lot.quantity, 2)
                )
            ):
                continue
            if (quantity < 0 and quantity > lot.quantity) or (
                quantity > 0 and quantity < lot.quantity
            ):
                raise ValueError(
                    f"not enough holdings of {lot.symbol} at {lot.openDateTime.date()}: have {quantity}, want {lot.quantity}"
                )
            if quantity == lot.quantity:
                holdings.pop(i)
            else:
                holding[0] -= lot.quantity
            return price
        raise ValueError(
            f"do not have {lot.symbol} bought at {lot.openDateTime.date()}: want {lot.quantity} at {lot.cost} ({lot.cost/lot.quantity} per unit). have {holdings}"
        )

    def Balances(self, cr):
        transactions = []
        for row in cr:
            if row.currency == "BASE_SUMMARY":
                continue  # this is a summary balance that is not needed for beancount
            amount_ = amount.Amount(row.endingCash, row.currency)

            transactions.append(
                data.Balance(
                    data.new_metadata("balance", 0),
                    row.toDate + timedelta(days=1),
                    self.get_liquidity_account(row.currency),
                    amount_,
                    None,
                    None,
                )
            )
        return transactions

    def merge_dividend_and_withholding(self, entries):
        """This merges together transactions for earned dividends with the witholding tax ones,
        as they can be on different lines in the cash transactions statement.
        """
        grouped = defaultdict(list)
        for e in entries:
            if not isinstance(e, data.Transaction):
                continue
            if "div_type" in e.meta and "isin" in e.meta:
                grouped[(e.date, e.payee, e.meta["div_type"])].append(e)
        for group in grouped.values():
            if len(group) < 2:
                continue
            # merge
            try:
                d = [e for e in group if "div" in e.meta][0]
            except IndexError:
                continue
            for e in group:
                if e != d:
                    d.postings.extend(e.postings)
                    entries.remove(e)
            del d.meta["div_type"]
            del d.meta["div"]
            # merge postings with the same account
            grouped_postings = defaultdict(list)
            for p in d.postings:
                grouped_postings[p.account].append(p)
            d.postings.clear()
            for account, postings in grouped_postings.items():
                d.postings.append(
                    data.Posting(
                        account,
                        functools.reduce(amount_add, (p.units for p in postings)),
                        None,
                        None,
                        None,
                        None,
                    )
                )
        return entries

    def corporate_actions(self, actions):
        transactions = []
        actions_map = defaultdict(list)
        for row in actions:
            actions_map[row.actionID].append(row)
        for action_group in actions_map.values():
            row = action_group[0]
            if row.type == Reorg.FORWARDSPLIT:
                assert len(action_group) == 1
                transactions.append(self.process_stock_forwardsplit(row))
            elif row.type == Reorg.MERGER:
                assert len(action_group) == 2
                transactions.append(self.process_stock_merger(action_group))
            elif row.type == Reorg.ISSUECHANGE:
                assert len(action_group) == 2
                transactions.append(self.process_issue_change(action_group))
            else:
                raise RuntimeError(f"unknown corporate action type: {row.type}")
        return transactions

    def process_stock_forwardsplit(self, row):
        symbol = get_currency_from_symbol(row.symbol)
        m = re.search(r"SPLIT (\d+) FOR (\d+)", row.description)
        factor = Decimal(int(m.group(1)) / int(m.group(2)))
        holdings = [(k[0], v) for k, v in self.holdings_map.items() if k[1] == symbol]
        postings = []
        for date, lst in holdings:
            for quantity, price, real_price in lst:
                postings.append(
                    data.Posting(
                        self.get_asset_account(row.symbol),
                        amount.Amount(-quantity, symbol),
                        data.CostSpec(price, None, row.currency, date, None, False),
                        None,
                        None,
                        None,
                    )
                )
                postings.append(
                    data.Posting(
                        self.get_asset_account(row.symbol),
                        amount.Amount(quantity * factor, symbol),
                        data.CostSpec(
                            price / factor, None, row.currency, date, None, False
                        ),
                        None,
                        None,
                        {"ib_cost": round(real_price / factor * quantity, 6)},
                    )
                )
        for date, lst in holdings:
            for i in lst:
                i[0] *= Decimal(factor)
                i[1] /= Decimal(factor)
                i[2] /= Decimal(factor)
        return data.Transaction(
            data.new_metadata("corporateactions", 0),
            row.reportDate,
            flags.FLAG_OKAY,
            row.symbol,
            row.description,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def process_stock_merger(self, action_group):
        # This is almost certainly wrong for tax accounting
        row = action_group[0]
        symbol = get_currency_from_symbol(row.symbol)
        holdings = [(k[0], v) for k, v in self.holdings_map.items() if k[1] == symbol]
        postings = []
        for date, lst in holdings:
            for quantity, price, _real_price in lst:
                postings.append(
                    data.Posting(
                        self.get_asset_account(row.symbol),
                        amount.Amount(-quantity, symbol),
                        data.CostSpec(price, None, row.currency, date, None, False),
                        None,
                        None,
                        None,
                    )
                )
        for k in list(self.holdings_map.keys()):
            if k[1] == symbol:
                del self.holdings_map[k]
        postings.append(
            data.Posting(
                self.get_liquidity_account(row.currency),
                amount.Amount(row.proceeds, row.currency),
                None,
                None,
                None,
                None,
            )
        )
        postings.append(
            data.Posting(self.get_pnl_account(symbol), None, None, None, None, None)
        )
        row = action_group[1]
        symbol = get_currency_from_symbol(row.symbol)
        postings.append(
            data.Posting(
                self.get_asset_account(row.symbol),
                amount.Amount(row.quantity, get_currency_from_symbol(row.symbol)),
                data.CostSpec(
                    row.value / row.quantity,
                    None,
                    row.currency,
                    row.reportDate,
                    None,
                    None,
                ),
                None,
                None,
                None,
            )
        )
        self.holdings_map[(row.reportDate, symbol)].append(
            (row.quantity, row.value / row.quantity, row.value / row.quantity)
        )
        return data.Transaction(
            data.new_metadata("corporateactions", 0),
            row.reportDate,
            flags.FLAG_OKAY,
            row.symbol,
            row.description,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def process_issue_change(self, action_group):
        row = action_group[0]
        if row.symbol.endswith(".OLD"):
            row = action_group[1]
        old_symbol = re.search(r"(.*?)\(", row.description).group(1)
        holdings = [
            (k[0], v) for k, v in self.holdings_map.items() if k[1] == old_symbol
        ]
        postings = []
        for date, lst in holdings:
            for quantity, price, real_price in lst:
                postings.append(
                    data.Posting(
                        self.get_asset_account(old_symbol),
                        amount.Amount(-quantity, old_symbol),
                        data.CostSpec(price, None, row.currency, date, None, False),
                        None,
                        None,
                        None,
                    )
                )
                postings.append(
                    data.Posting(
                        self.get_asset_account(row.symbol),
                        amount.Amount(quantity, get_currency_from_symbol(row.symbol)),
                        data.CostSpec(price, None, row.currency, date, None, False),
                        None,
                        None,
                        {"ib_cost": quantity * real_price},
                    )
                )
            del self.holdings_map[(date, old_symbol)]
            self.holdings_map[(date, row.symbol)] = lst
        return data.Transaction(
            data.new_metadata("corporateactions", 0),
            row.reportDate,
            flags.FLAG_OKAY,
            row.symbol,
            row.description,
            data.EMPTY_SET,
            data.EMPTY_SET,
            postings,
        )

    def autoopen_accounts(self, entries, existing_entries):
        """Adds open directives for unseen accounts.
        Mostly useful if one prefers to have each stock on a separate beancount subaccount.
        """
        opened_accounts = set()
        if existing_entries is None:
            return []
        for e in existing_entries:
            if not isinstance(e, data.Transaction):
                continue
            for p in e.postings:
                opened_accounts.add(p.account)
        open_entries = []
        for e in entries:
            if not isinstance(e, data.Transaction):
                continue
            for p in e.postings:
                if p.account not in opened_accounts:
                    min_entry = min(
                        (
                            e
                            for e in entries
                            if isinstance(e, data.Transaction)
                            and any(i.account == p.account for i in e.postings)
                        ),
                        key=lambda e: e.date,
                    )
                    if p.units is not None:
                        currency = get_currency_from_symbol(p.units.currency)
                    else:
                        currency = "USD"
                    open_entries.append(
                        data.Open(
                            meta=data.new_metadata("open", 0),
                            date=min_entry.date,
                            account=p.account,
                            currencies=[currency],
                            booking=None,
                        )
                    )
                    opened_accounts.add(p.account)
        return open_entries

    def get_holdings_map(self, entries):
        root = realization.realize(entries)
        account_parts = self.assets_account.split(":")
        for part in account_parts:
            if "{" in part:
                break
            if part not in root:
                return defaultdict(list)
            root = root[part]
        result = defaultdict(list)
        for account in realization.iter_children(root, leaf_only=True):
            for pos in account.balance:
                if pos.cost is None:
                    continue
                for tx in account.txn_postings:
                    real_price = None
                    if not isinstance(tx, data.TxnPosting):
                        continue
                    if (
                        tx.posting.units.currency == pos.units.currency
                        and tx.posting.cost.date == pos.cost.date
                        and tx.posting.cost.number == pos.cost.number
                        and "ib_cost" in tx.posting.meta
                    ):
                        real_price = abs(
                            tx.posting.meta["ib_cost"] / tx.posting.units.number
                        )
                    if real_price is None:
                        continue
                    self._adjust_holding(
                        result,
                        pos.cost.date,
                        pos.units.currency,
                        tx.posting.units.number,
                        tx.posting.cost.number,
                        real_price,
                    )
        return result

    def _adjust_holding(self, holdings_map, date, symbol, quantity, price, real_price):
        lst = holdings_map[(date, symbol)]
        for i, (u, _, rp) in enumerate(lst):
            if round(rp, 4) == round(real_price, 4) or (
                u == quantity and round(rp, 2) == round(real_price, 2)
            ):
                lst[i][0] += quantity
                if lst[i][0] == 0:
                    lst.pop(i)
                return
        holdings_map[(date, symbol)].append([quantity, price, real_price])


def is_forex(symbol):
    # returns True if a transaction is a forex transaction.
    b = re.search(r"(\w{3})[.](\w{3})", symbol)  # find something lile "USD.CHF"
    if b is None:  # no forex transaction, rather a normal stock transaction
        return False
    else:
        return True


def get_forex_currencies(symbol):
    b = re.search(r"(\w{3})[.](\w{3})", symbol)
    c = b.groups()
    return [c[0], c[1]]


def amount_add(A1, A2):
    # add two amounts
    if A1.currency == A2.currency:
        quant = A1.number + A2.number
        return amount.Amount(quant, A1.currency)
    else:
        raise (
            "Cannot add amounts of differnent currencies: {} and {}".format(
                A1.currency, A2.currency
            )
        )


def minus(A):
    # a minus operator
    return amount.Amount(-A.number, A.currency)


def get_currency_from_symbol(symbol):
    symbol = symbol.replace(" ", ".")
    if len(symbol) < 2:
        symbol = symbol + "STOCK"
    return symbol


def iter_trades_with_lots(trades):
    """Yields pairs of (trade, lots)."""
    it = iter(trades)
    trade = None
    lots = []
    while True:
        try:
            t = next(it)
        except StopIteration:
            break
        if isinstance(t, Types.Trade):
            if trade is not None:
                yield trade, lots
                lots = []
            trade = t
        elif isinstance(t, Types.Lot):
            lots.append(t)
        else:
            raise ValueError(f"Unknown trade element: {t}")
    if trade is not None:
        yield trade, lots


def get_test_importer():
    return Importer()


if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
