from uabean.importers.binance import get_test_importer
from common import run_importer_test


def test_binance_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
