import argparse
import base64
import json
import os
from datetime import date, datetime, timezone

import requests
import rsa
from dateutil.parser import isoparse


def do_sca_challenge(one_time_token):
    with open("wise-private.pem", "rb") as f:
        private_key_data = f.read()

    private_key = rsa.PrivateKey.load_pkcs1(private_key_data, "PEM")

    # Use the private key to sign the one-time-token that was returned
    # in the x-2fa-approval header of the HTTP 403.
    signed_token = rsa.sign(one_time_token.encode("ascii"), private_key, "SHA-256")
    signature = base64.b64encode(signed_token).decode("ascii")
    return signature


BASE_URL = "https://api.transferwise.com"


class WiseClient:
    def __init__(self, api_token, base_url=BASE_URL):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {api_token}"}

    def list_profiles(self):
        return requests.get(self.base_url + "/v1/profiles", headers=self.headers).json()

    def list_accounts(self, profile_id):
        return requests.get(
            self.base_url + "/v1/borderless-accounts",
            params={"profileId": profile_id},
            headers=self.headers,
        ).json()

    def list_balance_accounts(self, profile_id):
        return requests.get(
            self.base_url + f"/v4/profiles/{profile_id}/balances",
            params={"types": "STANDARD"},
            headers=self.headers,
        ).json()

    def get_account_statement(
        self, profile_id, account_id, currency, start_date, end_date, format="csv"
    ):
        headers = dict(self.headers)
        params = {
            "currency": currency,
            "intervalStart": start_date,
            "intervalEnd": end_date,
            "type": "COMPACT",
        }
        url = (
            self.base_url
            + f"/v3/profiles/{profile_id}/borderless-accounts/{account_id}/statement.{format}"
        )
        r = requests.get(url, params=params, headers=headers)
        if r.status_code == 403:
            one_time_token = r.headers["x-2fa-approval"]
            signature = do_sca_challenge(one_time_token)
            headers["x-2fa-approval"] = one_time_token
            headers["x-signature"] = signature
            r = requests.get(url, params=params, headers=headers)
        return r.text

    def get_balance_statement(
        self, profile_id, account_id, currency, start_date, end_date, format="csv"
    ):
        headers = dict(self.headers)
        params = {
            "currency": currency,
            "intervalStart": start_date,
            "intervalEnd": end_date,
            "type": "COMPACT",
        }
        url = (
            self.base_url
            + f"/v1/profiles/{profile_id}/balance-statements/{account_id}/statement.{format}"
        )
        r = requests.get(url, params=params, headers=headers)
        if r.status_code == 403:
            one_time_token = r.headers["x-2fa-approval"]
            signature = do_sca_challenge(one_time_token)
            headers["x-2fa-approval"] = one_time_token
            headers["x-signature"] = signature
            r = requests.get(url, params=params, headers=headers)
        return r.text


def main():
    parser = argparse.ArgumentParser(__name__)
    parser.add_argument(
        "--start-date",
        required=False,
        default=None,
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    )
    parser.add_argument(
        "--end-date",
        required=False,
        default=date.today(),
        type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    )
    parser.add_argument("-t", "--account-type", choices=("business", "personal"))
    parser.add_argument("-f", "--format", choices=("csv", "json"))
    parser.add_argument("-c", "--currency")
    parser.add_argument("-o", "--out-dir", default="wise")
    args = parser.parse_args()
    wise = WiseClient(os.getenv("WISE_API_TOKEN"))
    profiles = wise.list_profiles()
    if args.account_type is not None:
        profiles = [p for p in profiles if p["type"] == args.account_type]
    for profile in profiles:
        profile_id = profile["id"]
        accounts = wise.list_accounts(profile_id)
        balance_accounts = wise.list_balance_accounts(profile_id)
        if (
            args.start_date is None
            or isoparse(accounts[0]["creationTime"]).date() > args.start_date
        ):
            start_date = accounts[0]["creationTime"]
        else:
            start_date = datetime.combine(
                args.start_date, datetime.min.time(), timezone.utc
            ).isoformat()
        end_date = datetime.combine(
            args.end_date, datetime.max.time(), timezone.utc
        ).isoformat()
        if isoparse(end_date) < isoparse(start_date):
            continue
        # for account in accounts[0]["balances"]:
        for balance_account in balance_accounts:
            if (
                args.currency is not None
                and args.currency.upper() != balance_account["currency"]
            ):
                continue
            # statement = wise.get_csv_statement(profile_id, account_id, account["currency"], start_date, end_date)
            statement = wise.get_balance_statement(
                profile_id,
                balance_account["id"],
                balance_account["currency"],
                start_date,
                end_date,
                format=args.format,
            )
            if not statement.strip():
                continue
            if args.format == "json":
                statement = json.dumps(json.loads(statement), indent=4)
            sd = isoparse(start_date).strftime("%Y-%m-%d")
            ed = isoparse(end_date).strftime("%Y-%m-%d")
            # fname = f"wise-{profile['type']}-{sd}_{ed}-{account['currency']}.csv"
            fname = f"wise-{profile['type']}-{sd}_{ed}-{balance_account['currency']}.{args.format}"
            with open(os.path.join(args.out_dir, fname), "w") as f:
                f.write(statement)
            print(f"Wrote {fname}")


if __name__ == "__main__":
    main()
