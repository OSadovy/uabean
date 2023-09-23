import os
from collections import namedtuple

from beancount import loader
from beangulp import extract
from beangulp.testing import _run, compare_expected

Context = namedtuple("Context", ["importers"])


def run_importer_test(importer, capsys):
    _run(
        Context([importer]),
        [os.path.abspath(f"tests/importers/{importer.account('')}/")],
        "",
        0,
        0,
    )
    captured = capsys.readouterr()
    assert "PASSED" in captured.out
    assert "ERROR" not in captured.out


def run_importer_test_with_existing_entries(
    importer, document, existing_entries_filename
):
    base_path = os.path.abspath(f"tests/importers/{importer.account('')}")
    expected_filename = os.path.join(base_path, f"{document}.beancount")
    document = os.path.join(base_path, document)
    existing_entries = loader.load_file(
        os.path.join(base_path, existing_entries_filename)
    )[0]
    account = importer.account(document)
    date = importer.date(document)
    name = importer.filename(document)
    entries = extract.extract_from_file(importer, document, existing_entries)
    diff = compare_expected(expected_filename, account, date, name, entries)
    assert diff == []
