from common import run_importer_test

from uabean.importers.nexo import get_test_importer


def test_nexo_importer(capsys):
    run_importer_test(get_test_importer(), capsys)
