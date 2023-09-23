from uabean.importers.alfa_business import get_test_importer
from common import run_importer_test


def test_alfabank_business_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
