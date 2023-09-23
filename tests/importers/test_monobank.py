from common import run_importer_test

from uabean.importers.monobank import get_test_importer


def test_monobank_importer(capsys):
    importer = get_test_importer()
    importer._download_mcc_codes = lambda: None
    importer.mcc_codes = {
        "5817": "Applications",
        "4121": "Taxi",
        "4829": "Money transfer",
    }
    run_importer_test(importer, capsys)
