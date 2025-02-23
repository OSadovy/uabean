"""Downloads statements via monobank API and dumps them in CSV.

Expects MONOBANK_TOKEN env var to be set to the valid monobank API token.

Run with --help for usage.
"""

import argparse
import csv
import datetime
import os
import time
from dataclasses import dataclass
from decimal import Decimal

import requests

CURRENCY_CODES = {
    "784": "AED",
    "971": "AFN",
    "008": "ALL",
    "051": "AMD",
    "532": "ANG",
    "973": "AOA",
    "032": "ARS",
    "036": "AUD",
    "533": "AWG",
    "944": "AZN",
    "977": "BAM",
    "052": "BBD",
    "050": "BDT",
    "975": "BGN",
    "048": "BHD",
    "108": "BIF",
    "060": "BMD",
    "096": "BND",
    "068": "BOB",
    "984": "BOV",
    "986": "BRL",
    "044": "BSD",
    "064": "BTN",
    "072": "BWP",
    "933": "BYN",
    "084": "BZD",
    "124": "CAD",
    "976": "CDF",
    "947": "CHE",
    "756": "CHF",
    "948": "CHW",
    "990": "CLF",
    "152": "CLP",
    "156": "CNY",
    "170": "COP",
    "970": "COU",
    "188": "CRC",
    "931": "CUC",
    "192": "CUP",
    "132": "CVE",
    "203": "CZK",
    "262": "DJF",
    "208": "DKK",
    "214": "DOP",
    "012": "DZD",
    "818": "EGP",
    "232": "ERN",
    "230": "ETB",
    "978": "EUR",
    "242": "FJD",
    "238": "FKP",
    "826": "GBP",
    "981": "GEL",
    "936": "GHS",
    "292": "GIP",
    "270": "GMD",
    "324": "GNF",
    "320": "GTQ",
    "328": "GYD",
    "344": "HKD",
    "340": "HNL",
    "191": "HRK",
    "332": "HTG",
    "348": "HUF",
    "360": "IDR",
    "376": "ILS",
    "356": "INR",
    "368": "IQD",
    "364": "IRR",
    "352": "ISK",
    "388": "JMD",
    "400": "JOD",
    "392": "JPY",
    "404": "KES",
    "417": "KGS",
    "116": "KHR",
    "174": "KMF",
    "408": "KPW",
    "410": "KRW",
    "414": "KWD",
    "136": "KYD",
    "398": "KZT",
    "418": "LAK",
    "422": "LBP",
    "144": "LKR",
    "430": "LRD",
    "426": "LSL",
    "434": "LYD",
    "504": "MAD",
    "498": "MDL",
    "969": "MGA",
    "807": "MKD",
    "104": "MMK",
    "496": "MNT",
    "446": "MOP",
    "478": "MRO",
    "929": "MRU",
    "480": "MUR",
    "462": "MVR",
    "454": "MWK",
    "484": "MXN",
    "979": "MXV",
    "458": "MYR",
    "943": "MZN",
    "516": "NAD",
    "566": "NGN",
    "558": "NIO",
    "578": "NOK",
    "524": "NPR",
    "554": "NZD",
    "512": "OMR",
    "590": "PAB",
    "604": "PEN",
    "598": "PGK",
    "608": "PHP",
    "586": "PKR",
    "985": "PLN",
    "600": "PYG",
    "634": "QAR",
    "946": "RON",
    "941": "RSD",
    "643": "RUB",
    "646": "RWF",
    "682": "SAR",
    "090": "SBD",
    "690": "SCR",
    "938": "SDG",
    "752": "SEK",
    "702": "SGD",
    "654": "SHP",
    "694": "SLL",
    "706": "SOS",
    "968": "SRD",
    "728": "SSP",
    "930": "STN",
    "222": "SVC",
    "760": "SYP",
    "748": "SZL",
    "764": "THB",
    "972": "TJS",
    "795": "TMM",
    "934": "TMT",
    "788": "TND",
    "776": "TOP",
    "949": "TRY",
    "780": "TTD",
    "901": "TWD",
    "834": "TZS",
    "980": "UAH",
    "800": "UGX",
    "840": "USD",
    "997": "USN",
    "940": "UYI",
    "858": "UYU",
    "927": "UYW",
    "860": "UZS",
    "928": "VES",
    "704": "VND",
    "548": "VUV",
    "882": "WST",
    "950": "XAF",
    "961": "XAG",
    "959": "XAU",
    "955": "XBA",
    "956": "XBB",
    "957": "XBC",
    "958": "XBD",
    "951": "XCD",
    "960": "XDR",
    "952": "XOF",
    "964": "XPD",
    "953": "XPF",
    "962": "XPT",
    "994": "XSU",
    "963": "XTS",
    "965": "XUA",
    "999": "XXX",
    "886": "YER",
    "710": "ZAR",
    "894": "ZMK",
    "967": "ZMW",
    "932": "ZWL",
}


@dataclass
class Account:
    id: str
    send_id: str
    currency: str
    cashback_type: str
    balance: Decimal
    credit_limit: Decimal
    type: str
    masked_ban: list[str]
    iban: str


@dataclass
class ClientInfo:
    client_id: str
    name: str
    webhook_url: str
    permissions: str
    accounts: list[Account]


@dataclass
class StatementItem:
    id: str
    time: datetime.datetime
    description: str
    mcc: int
    original_mcc: int
    amount: Decimal
    operation_amount: Decimal
    currency: str
    commission_rate: Decimal
    cashback_amount: Decimal
    balance: Decimal
    hold: bool


class MonobankClient:
    BASE_URL = "https://api.monobank.ua"

    def __init__(self):
        self.headers = {"X-Token": os.getenv("MONOBANK_TOKEN")}

    @staticmethod
    def get_currency(code: int) -> str:
        return CURRENCY_CODES[str(code)]

    def client_info(self) -> ClientInfo:
        info = requests.get(
            self.BASE_URL + "/personal/client-info", headers=self.headers
        ).json()
        return ClientInfo(
            info["clientId"],
            info["name"],
            info["webHookUrl"],
            info["permissions"],
            accounts=[self._account(account) for account in info["accounts"]],
        )

    def _account(self, obj: dict) -> Account:
        return Account(
            obj["id"],
            obj["sendId"],
            self.get_currency(obj["currencyCode"]),
            obj.get("cashbackType"),
            self.get_decimal(obj["balance"]),
            self.get_decimal(obj["creditLimit"]),
            obj["type"],
            obj["maskedPan"],
            obj["iban"],
        )

    @staticmethod
    def get_decimal(s: str) -> Decimal:
        return Decimal(s) / 100

    def personal_statement(self, account_id, from_date, to_date) -> list[StatementItem]:
        response = requests.get(
            self.BASE_URL
            + f"/personal/statement/{account_id}/{int(from_date.timestamp())}/{int(to_date.timestamp())}",
            headers=self.headers,
        )
        response.raise_for_status()
        info = response.json()
        return [
            StatementItem(
                i["id"],
                datetime.datetime.fromtimestamp(i["time"]),
                i["description"],
                i["mcc"],
                i["originalMcc"],
                self.get_decimal(i["amount"]),
                self.get_decimal(i["operationAmount"]),
                self.get_currency(i["currencyCode"]),
                self.get_decimal(i["commissionRate"]),
                self.get_decimal(i["cashbackAmount"]),
                self.get_decimal(i["balance"]),
                i["hold"],
            )
            for i in info
        ]


class MonobankCSVWriter:
    headers = [
        "Дата i час операції",
        "Деталі операції",
        "MCC",
        "Сума в валюті картки ({currency})",
        "Сума в валюті операції",
        "Валюта",
        "Курс",
        "Сума комісій ({currency})",
        "Сума кешбеку ({cashback_type})",
        "Залишок після операції",
    ]

    def __init__(self, writer):
        self.writer = writer

    def write_header(self, account: Account):
        header = [
            h.format(currency=account.currency, cashback_type=account.cashback_type)
            for h in self.headers
        ]
        self.writer.writerow(header)

    def write_statement_item(self, account: Account, i: StatementItem):
        exchange_rate = "—"
        if i.currency != account.currency:
            exchange_rate = round(i.operation_amount / i.amount, 6)
        comission = "—"
        if i.commission_rate != 0:
            comission = i.commission_rate
        cashback = "—"
        if i.cashback_amount != 0:
            cashback = i.cashback_amount
        self.writer.writerow(
            [
                i.time.strftime("%d.%m.%Y %H:%M:%S"),
                i.description.replace("\n", " ").replace("\r", ""),
                i.mcc,
                i.amount,
                i.operation_amount,
                i.currency,
                exchange_rate,
                comission,
                cashback,
                i.balance,
            ]
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        required=True,
    )
    parser.add_argument(
        "-e",
        "--end-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        default=datetime.date.today(),
    )
    parser.add_argument("-c", "--currency")
    parser.add_argument("-t", "--account-type")
    parser.add_argument("-o", "--output-dir")
    args = parser.parse_args()
    client = MonobankClient()
    info = client.client_info()
    accounts = info.accounts
    if args.currency:
        accounts = [a for a in accounts if a.currency == args.currency]
    if args.account_type:
        accounts = [a for a in accounts if a.type == args.account_type]
    start_time = datetime.datetime.fromordinal(args.start_date.toordinal())
    end_time = datetime.datetime(
        args.end_date.year, args.end_date.month, args.end_date.day, 23, 59, 59
    )
    end_time = min(end_time, datetime.datetime.now())
    writers = {}
    for account in accounts:
        dt = end_time.strftime("%d-%m-%y_%H-%M-%S")
        writer = csv.writer(
            open(
                os.path.join(
                    args.output_dir,
                    f"monobank-{account.type}-{account.currency}_{dt}.csv",
                ),
                "w",
                encoding="utf-8",
                newline="",
            ),
            quoting=csv.QUOTE_MINIMAL,
        )
        writers[account.id] = MonobankCSVWriter(writer)
        writers[account.id].write_header(account)

    while start_time < end_time:
        for account in accounts:
            print(
                f"Downloading {account.type} {account.currency} for {start_time.isoformat()}"
            )
            while True:
                try:
                    statement = client.personal_statement(
                        account.id,
                        start_time,
                        min(end_time, start_time + datetime.timedelta(days=31)),
                    )
                    break
                except requests.exceptions.HTTPError:
                    print("Rate limit encountered, waiting")
                    time.sleep(15)
            for i in sorted(statement, key=lambda s: s.time):
                writers[account.id].write_statement_item(account, i)
            time.sleep(15)
        start_time += datetime.timedelta(days=31)


if __name__ == "__main__":
    main()
