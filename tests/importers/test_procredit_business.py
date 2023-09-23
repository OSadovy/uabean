from common import run_importer_test

from uabean.importers.procredit_business import get_test_importer


def test_procreditbank_business_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
