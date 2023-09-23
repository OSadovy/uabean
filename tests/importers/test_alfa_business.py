from common import run_importer_test

from uabean.importers.alfa_business import get_test_importer


def test_alfabank_business_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
