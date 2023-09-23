import argparse
import copy
import datetime
import logging
import re

from autobean.refactor import models, parser, printer
from sklearn.pipeline import FeatureUnion, make_pipeline
from sklearn.svm import SVC
from smart_importer.pipelines import get_pipeline

logger = logging.getLogger(__name__)


class Predictor:
    FLAG = "P"
    ignored_accounts = ["Expenses:Fees"]
    weights: dict[str, float] = {}
    attribute: str | None = None

    def __init__(self, main_accounts: list[str]):
        self.main_accounts = main_accounts
        if not getattr(self, "blacklisted_accounts", None):
            self.blacklisted_accounts = [
                "Equity:Opening-Balances",
                "Expenses:Fraud",
            ] + self.ignored_accounts

        self.training_data = []
        self.string_tokenizer = None
        self.is_fitted = False
        self.pipeline = None

    def load_training_data(self, directives):
        """Load training data, i.e., a list of Beancount entries."""
        training_data = directives or []
        training_data = list(
            filter(lambda d: isinstance(d, models.Transaction), directives)
        )
        length_all = len(training_data)
        training_data = [txn for txn in training_data if self.training_data_filter(txn)]
        if not training_data:
            if length_all > 0:
                logger.warning(
                    "Cannot train the machine learning model"
                    "None of the training data matches the accounts"
                )
            else:
                logger.warning(
                    "Cannot train the machine learning model: No training data found"
                )
        else:
            logger.debug(
                "Filtered training data to %s of %s entries.",
                len(training_data),
                length_all,
            )
        self.training_data = training_data

    def training_data_filter(self, txn):
        if "fraud" in txn.tags:
            return False
        num_accounts = 0
        found_import_account = False
        accounts = set()
        for pos in txn.postings:
            accounts.add(pos.account)
            if pos.account in self.main_accounts:
                found_import_account = True
                num_accounts += 1
            elif (
                any(bl in pos.account for bl in self.blacklisted_accounts)
                or pos.flag == self.FLAG
            ):
                num_accounts += 1
        return found_import_account and len(accounts) > num_accounts

    def predict_data_filter(self, txn):
        """Returns True if this transaction needs prediction."""
        if "fraud" in txn.tags:
            return False
        non_ignored_accounts_count = 0
        for p in txn.postings:
            if not any(i in p.account for i in self.ignored_accounts):
                non_ignored_accounts_count += 1
            if (
                p.account not in self.main_accounts
                and not any(i in p.account for i in self.ignored_accounts)
                and not p.flag == self.FLAG
            ):
                return False
        return non_ignored_accounts_count < 2

    def define_pipeline(self):
        """Defines the machine learning pipeline based on given weights."""

        transformers = []
        for attribute in self.weights:
            transformers.append(
                (attribute, get_pipeline(attribute, self.string_tokenizer))
            )

        self.pipeline = make_pipeline(
            FeatureUnion(
                transformer_list=transformers, transformer_weights=self.weights
            ),
            SVC(kernel="linear"),
        )

    def train_pipeline(self):
        """Train the machine learning pipeline."""

        targets = self.targets
        self.is_fitted = False

        if len(set(targets)) == 0:
            logger.warning(
                "Cannot train the machine learning model "
                "because there are no targets."
            )
        elif len(set(targets)) == 1:
            self.is_fitted = True
            logger.debug("Only one target possible.")
        else:
            self.pipeline.fit(self.training_data, targets)
            self.is_fitted = True
            logger.debug("Trained the machine learning model.")

    @property
    def targets(self):
        if not self.attribute:
            raise NotImplementedError
        return [getattr(entry, self.attribute) or "" for entry in self.training_data]

    def __call__(self, directives, predict_start_date=None):
        self.load_training_data(directives)
        self.define_pipeline()
        self.train_pipeline()
        return self.process_entries(directives, predict_start_date=predict_start_date)

    def process_entries(self, directives, predict_start_date=None):
        transactions = [
            d
            for d in directives
            if isinstance(d, models.Transaction)
            and self.predict_data_filter(d)
            and (predict_start_date is None or d.date >= predict_start_date)
        ]
        if len(transactions) == 0:
            logger.info("No transactions that need prediction")
            return []
        predictions = self.pipeline.predict(transactions)
        logger.info("got %d predictions.", len(predictions))
        transactions = [
            self.apply_prediction(entry, prediction)
            for entry, prediction in zip(transactions, predictions)
        ]
        return transactions

    def apply_prediction(self, directive, prediction):
        if not self.attribute:
            raise NotImplementedError
        setattr(directive, self.attribute, prediction)
        return directive


class PostingPredictor(Predictor):
    @property
    def targets(self):
        return [
            " ".join(
                p.account
                for p in txn.postings
                if not (
                    p.account in self.main_accounts
                    or any(bl in p.account for bl in self.blacklisted_accounts)
                )
            )
            for txn in self.training_data
        ]

    def apply_prediction(self, directive: models.Transaction, prediction: str):
        existing_posting = None
        for i, p in enumerate(directive.postings):
            if p.flag == self.FLAG:
                existing_posting = copy.deepcopy(p)
                directive.postings[i] = existing_posting
                break
        if existing_posting is None:
            existing_posting = copy.deepcopy(directive.postings[0])
            directive.postings.append(existing_posting)
        existing_posting.account = str(prediction)
        existing_posting.number = None
        existing_posting.currency = None
        existing_posting.flag = self.FLAG
        existing_posting.price = None
        existing_posting.cost = None
        return directive


class WisePredictor(PostingPredictor):
    ignored_accounts = ["Expenses:Fees:Wise"]
    weights = {"meta.src_category": 0.8, "payee": 0.5, "date.day": 0.1}


class MonobankPredictor(PostingPredictor):
    ignored_accounts = [
        "Expenses:Fees:Wise",
        "Assets:Monobank:Receivable:Cashback",
        "Income:Cashback:Monobank",
        "Expenses:Taxes",
        "Income:Monobank:Interest",
    ]
    weights = {"meta.category": 0.8, "payee": 0.5, "date.day": 0.1}


class SensebankPredictor(PostingPredictor):
    weights = {"meta.category": 0.8, "payee": 0.5, "narration": 0.5, "date.day": 0.1}


class PrivatbankPredictor(PostingPredictor):
    weights = {"meta.category": 0.6, "narration": 0.5, "date.day": 0.1}


PREDICTORS_CONFIG = [
    (r"Wise:Personal", WisePredictor),
    (r"Monobank", MonobankPredictor),
    (r"Alfabank", SensebankPredictor),
    (r"Privat", PrivatbankPredictor),
]


def get_predictor(accounts):
    for regex, predictor in PREDICTORS_CONFIG:
        for account in accounts:
            if re.search(regex, account):
                return predictor


def main():
    logging.basicConfig(level=logging.DEBUG)
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-a", "--account", action="append")
    argparser.add_argument(
        "-d", "--predict-start-date", type=datetime.date.fromisoformat
    )
    argparser.add_argument("fname", type=argparse.FileType("r"))
    args = argparser.parse_args()
    PredictorClass = get_predictor(args.account)
    if PredictorClass is None:
        argparser.error("No predictor found for the given accounts")
    predictor = PredictorClass(args.account)
    beancount_parser = parser.Parser()
    file = beancount_parser.parse(args.fname.read(), models.File)
    predictor(file.directives, predict_start_date=args.predict_start_date)
    printer.print_model(file, open(args.fname.name, "w"))


if __name__ == "__main__":
    main()
