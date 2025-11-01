from common import run_importer_test

from uabean.importers.wise_csv import get_test_importer


def test_wise_csv_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
