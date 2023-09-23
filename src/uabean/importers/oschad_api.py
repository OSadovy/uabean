"""
Imports transactions from Oschadbank online API.

This module supports two types of authorizations:
1. Using OSCHAD_JSESSIONID_COOKIE environment variable: 
   The value can be obtained by logging in to Oschadbank online and copying the
   cookie value from the browser.
   
2. Using manual login credentials:
   If the OSCHAD_JSESSIONID_COOKIE is not found, the module will prompt the user 
   to input their username and password. If the session requires OTP 
   (One-Time Password), the user will be asked to provide it. 
   Environment variables OSCHAD_LOGIN and OSCHAD_PASSWORD can also be used to 
   provide the username and password, respectively.
"""


import dateutil.parser
from datetime import date, timedelta
import getpass

# import json
import os
import sys
import re
import requests
import beangulp
from uabean.importers.mixins import IdentifyMixin
from beancount.core import data, flags
from beancount.core.number import D


API_BASE_URL = "https://online.oschadbank.ua/wb/api/v2"


class Importer(IdentifyMixin, beangulp.Importer):
    FLAG = flags.FLAG_OKAY
    matchers = [
        ("filename", "oschadbank"),
    ]
    DESCRIPTION_CLASSIFIERS = [
        (r"реестров УПСЗН", "Income:SocialInsurance"),
        (r"Видача готівки", "Expenses:Mum"),
    ]

    def __init__(
        self,
        account_config: dict[str, str],
        fees_account="Expenses:Fees:Oschadbank",
        min_date=date(2019, 1, 1),
    ):
        self.account_config = account_config
        self.fees_account = fees_account
        self.min_date = min_date
        self._contracts = {}
        self._accounts = {}
        self._contract_id_to_number = {}
        super().__init__()
        self._session = requests.Session()

    def _login(self):
        if os.getenv("OSCHAD_JSESSIONID_COOKIE"):
            self._session.cookies.set(
                "JSESSIONID", os.getenv("OSCHAD_JSESSIONID_COOKIE")
            )
            return
        response = self._session.get("https://online.oschadbank.ua/wb/")
        response.raise_for_status()

        login = os.getenv("OSCHAD_LOGIN") or input("Enter your username: ")
        password = os.getenv("OSCHAD_PASSWORD") or getpass.getpass(
            "Enter your password: "
        )
        initial_data = {"login": login, "password": password, "captcha": ""}

        headers = {
            "authority": "online.oschadbank.ua",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "ua",
            "content-type": "application/json",
            "origin": "https://online.oschadbank.ua",
            "ow-client-browser": "Chrome",
            "referer": "https://online.oschadbank.ua/wb/",
            "sec-ch-ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "x-xsrf-token": response.cookies["XSRF-TOKEN"],
        }
        response = self._session.post(
            f"{API_BASE_URL}/session",
            json=initial_data,
            headers=headers,
        )
        if response.status_code == 401:
            response_data = response.json()

            otp = input("Enter your OTP: ")
            otp_data = {
                "login": login,
                "password": password,
                "captcha": "",
                "_status": None,
                "confirmation": response_data["confirmation"],
            }
            otp_data["confirmation"]["response"] = otp

            response = self._session.post(
                f"{API_BASE_URL}/session",
                json=otp_data,
                headers=headers,
            )
            response.raise_for_status()

        if "JSESSIONID" in response.cookies:
            self._session.cookies.set("JSESSIONID", response.cookies["JSESSIONID"])
            print(
                f"export OSCHAD_JSESSIONID_COOKIE=f{response.cookies['JSESSIONID']}",
                file=sys.stderr,
            )
        else:
            raise Exception(f"Failed to login: {response.text}")

    def _get_contracts(self):
        # return json.load(open("contracts.json"))
        response = self._session.get(f"{API_BASE_URL}/contracts")
        response.raise_for_status()
        return response.json()

    def _get_history(self, date_from, date_to=None):
        # return json.load(open("history.json"))
        params = {
            "from": date_from.strftime("%Y-%m-%d"),
        }
        if date_to is not None:
            params["to"] = date_to.strftime("%Y-%m-%d")
        response = self._session.get(
            f"{API_BASE_URL}/history",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def account(self, _):
        return "oschadbank"

    def extract(self, filename, existing_entries=None):
        self._login()
        self._contracts = self._get_contracts()
        contract_number_to_id = {c["number"]: c["id"] for c in self._contracts}
        self._contract_id_to_number = {c["id"]: c["number"] for c in self._contracts}
        self._accounts = {
            contract_number_to_id[k]: v for k, v in self.account_config.items()
        }
        entries = []
        max_date = self.min_date

        # Existing entries are considered for determining the max_date
        if existing_entries is not None:
            for entry in existing_entries:
                if isinstance(entry, data.Transaction) and entry.date > max_date:
                    for posting in entry.postings:
                        if posting.account in self._accounts.values():
                            max_date = entry.date

        # Get history from max_date onwards
        records = self._get_history(max_date)
        for i, record in enumerate(records[::-1]):
            meta = data.new_metadata("API", i)
            transaction = self.entry_from_record(meta, record)
            if transaction is None:
                continue
            self.classify_posting(transaction)
            entries.append(transaction)
        max_date = (entries[-1].date + timedelta(days=1)) if entries else date.today()
        meta = data.new_metadata("API", 0)
        processed_accounts = set()
        for contract_number, account in self.account_config.items():
            if (
                contract_number not in contract_number_to_id
                or account in processed_accounts
            ):
                continue
            contract = [
                c
                for c in self._contracts
                if c["id"] == contract_number_to_id[contract_number]
            ][0]
            entries.append(
                data.Balance(
                    meta,
                    max_date,
                    account,
                    self.amount_from_record(contract["balances"]["available"]),
                    None,
                    None,
                )
            )
            processed_accounts.add(account)
        return entries

    def entry_from_record(self, meta, record):
        if record.get("status") == "failed":
            return
        try:
            account = self._accounts[record["contractId"]]
        except KeyError:
            contract_number = self._contract_id_to_number[record["contractId"]]
            raise ValueError(
                f"Encountered transaction with unknown contract: {contract_number}"
            )
        dt = dateutil.parser.parse(record["operationTime"])
        # convert dt into local timezone
        dt = dt.astimezone()
        meta["time"] = dt.time().strftime("%H:%M:%S")
        narration = record["description"].replace("\\$", ";")
        postings = [
            data.Posting(
                account,
                self.amount_from_record(record["totalAmount"]),
                None,
                None,
                None,
                None,
            )
        ]
        if record["fees"]:
            postings.append(
                data.Posting(
                    self.fees_account,
                    -self.amount_from_record(record["fees"]["totalFee"]),
                    None,
                    None,
                    None,
                    None,
                )
            )
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

    def amount_from_record(self, r):
        return data.Amount(D(r["value"]), r["currency"])

    def classify_posting(self, transaction):
        for pattern, account in self.DESCRIPTION_CLASSIFIERS:
            if re.search(pattern, transaction.narration, re.I):
                transaction.postings.append(
                    data.Posting(account, None, None, None, None, None)
                )


def get_test_importer():
    return Importer(
        {
            "1234567890": "Assets:Oschadbank:Cash:UAH",
        }
    )

if __name__ == "__main__":
    from beangulp.testing import main

    main(get_test_importer())
