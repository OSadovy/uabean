import sys
from autobean.refactor import models, parser, printer

def is_target_transaction(d: models.Transaction) -> bool:
    for p in d.postings:
        if p.account == "Expenses:Fraud":
            return True
    return False

def modify_transaction(t: models.Transaction) -> None:
    for p in t.postings:
        if p.account == "Expenses:Fees:Wise":
            t.postings.remove(p)
            break

f = parser.Parser().parse(open(sys.argv[1]).read(), models.File)
modified = 0
for d in f.directives:
    if isinstance(d, models.Transaction) and is_target_transaction(d):
        modify_transaction(d)
        modified += 1
print(f"Modified {modified} transactions")
printer.print_model(f, open(sys.argv[1] + ".new", "w"))