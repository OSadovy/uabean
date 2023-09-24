<!-- These are examples of badges you might want to add to your README:

[![Coveralls](https://img.shields.io/coveralls/github/osadovy/uabean/main.svg)](https://coveralls.io/r/<USER>/uabean)
[![PyPI-Server](https://img.shields.io/pypi/v/uabean.svg)](https://pypi.org/project/uabean/)
-->

[![Project generated with PyScaffold](https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold)](https://pyscaffold.org/)

# uabean

A set of Beancount importers and scripts for popular Ukrainian banks and more

This repository contains various goodies for [Beancount], a text-based double-entry bookkeeping tool for personal finances. To learn more about Beancount, you can start by reading this [official getting started guide][guide].

## Installation
```
$ pip install git+https://github.com/osadovy/uabean
```
Extra scripts are not installed by default. If you want to use them, look into each script docstring to see its dependencies.

## Importers
These importers allow you to produce Beancount transactions from exported account statements of various financial institutions:
* Wise
* Interactive Brokers
* Binance
* Tronscan
* Nexo
* Sensebank (business and personal)
* Privatbank
* Monobank
* Ukrsibbank
* Procreditbank (business and personal)
* Pumb
* Oschadbank (imports transactions received through web API)

The importers are created using [Beangulp] framework. To use them, you need to install this library and reference them from within your importer config file. Each importer requires some configuration to work - usually, the mapping of bank account numbers to Beancount account names. [Here is](my_import.py.sample)the sample importer config file that shows configuration options for each importer. To see what kind of files you need as input and where to get them, look into each importer's module docstring.

## Downloaders
These automate receiving of account statements to be further processed by importers.

### Wise
Requires `WISE_API_TOKEN` environment variable (details [here][wise-api-token]), as well as presence of `wise-private.pem` file containing private key registered with Wise for signing SCA requests . [See here][wise-signing] for instructions how to generate your key and register its public part with Wise.
```
usage: uabean.downloaders.wise [-h] [--start-date START_DATE] [--end-date END_DATE] [-t {business,personal}]
                               [-f {csv,json}] [-c CURRENCY] [-o OUT_DIR]

options:
  -h, --help            show this help message and exit
  --start-date START_DATE
  --end-date END_DATE
  -t {business,personal}, --account-type {business,personal}
  -f {csv,json}, --format {csv,json}
  -c CURRENCY, --currency CURRENCY
  -o OUT_DIR, --out-dir OUT_DIR```
```
Example:
```bash
$ uabean-wise-downloader --start-date 2023-09-01 -t personal -f json -o downloads/
```

### Monobank
Requires presence of `MONOBANK_TOKEN` environment variable. Get your token [here][monobank-api].
```
usage: uabean-monobank-downloader [-h] -s START_DATE [-e END_DATE] [-c CURRENCY] [-t ACCOUNT_TYPE] [-o OUTPUT_DIR]

options:
  -h, --help            show this help message and exit
  -s START_DATE, --start-date START_DATE
  -e END_DATE, --end-date END_DATE
  -c CURRENCY, --currency CURRENCY
  -t ACCOUNT_TYPE, --account-type ACCOUNT_TYPE
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
```

Example:
```bash
$ uabean-monobank-downloader -s 2023-09-01 -o downloads/
```

## Scripts
This directory includes a few useful scripts that I use to speedup my importing process. They are not installed as executables for now, but you can download them and tweak to your liking:
* [predict_inplace](scripts/predict_inplace.py) - predicts postings for existing ledger using machine learning. Based on [smart_importer] but works with existing ledger files and adds predicted postings inplace. Supports Wise, Monobank, Sensebank and Privatbank but can be easily extended for others.
* [sorttransactions](scripts/sorttransactions.py) - sorts transactions inside a ledger file. Useful when you keep transactions in a bank-specific file and there are multiple statements for the same month but different bank accounts that need to be merged in order.

[Beancount]: https://github.com/beancount/beancount
[guide]: https://beancount.github.io/docs/getting_started_with_beancount.html
[Beangulp]: https://github.com/beancount/beangulp
[wise-api-token]: https://docs.wise.com/api-docs/features/authentication-access/personal-tokens
[wise-signing]: https://docs.wise.com/api-docs/features/strong-customer-authentication-2fa/personal-token-sca
[monobank-api]: https://api.monobank.ua/
[smart_importer]: https://github.com/beancount/smart_importer
