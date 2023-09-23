from uabean.importers.sensebank import get_test_importer
from common import run_importer_test


def test_sensebank_importer(capsys):
    importer= get_test_importer()
    importer._download_mcc_codes = lambda: None
    importer.mcc_codes = {
        "4829": "Money transfer",
        "4900": "Utilities",
        "7311": "Advertising",
        "6011": "Сashier's office",
    }
    run_importer_test(importer, capsys)
