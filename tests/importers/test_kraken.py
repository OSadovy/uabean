from common import run_importer_test

from uabean.importers.kraken import get_test_importer


def test_kraken_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
