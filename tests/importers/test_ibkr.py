from common import run_importer_test_with_existing_entries

from uabean.importers.ibkr import get_test_importer


def test_ibkr_importer():
    run_importer_test_with_existing_entries(
        get_test_importer(), "beancount.xml", "existing.beancount"
    )
