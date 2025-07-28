from common import run_importer_test

from uabean.importers.privatbank_xlsx import get_test_importer


def test_privatbank_xlsx_importer(capsys):
    run_importer_test(get_test_importer(), capsys)