import csv
from datetime import datetime, timedelta
import re
import sys


def redact_dates_in_description(desc):
    return re.sub(
        r"від \d+ (січня|лютого|березня|квітня|травня|червня|липня|серпня|вересня|жовтня|листопада|грудня) \d+ р\.",
        "REDACTED",
        desc,
    )


data = open(sys.argv[1], encoding="cp1251").read()

# Convert the data to rows using csv
lines = list(csv.reader(data.splitlines(), delimiter=";"))

# Initialize dictionaries and counters for mappings
account_mappings = {}
correspondent_mappings = {}
edrpou_mappings = {}
nabu_id_mappings = {}
iban_counter = 1
correspondent_counter = 1
edrpou_counter = 1
nabu_id_counter = 1


# Functions to get new values
def get_new_iban():
    global iban_counter
    iban = f"UA{iban_counter:08}"
    iban_counter += 1
    return iban


def get_new_correspondent():
    global correspondent_counter
    name = f"Correspondent{correspondent_counter}"
    correspondent_counter += 1
    return name


def get_new_edrpou():
    global edrpou_counter
    edrpou = f"{edrpou_counter:010}"  # Assuming it's a 10-digit number
    edrpou_counter += 1
    return edrpou


def get_new_nabu_id():
    global nabu_id_counter
    nabu_id = f"{nabu_id_counter:06}"  # Assuming it's a 6-digit number
    nabu_id_counter += 1
    return nabu_id


# Calculate date offset based on the earliest date in CSV
def get_offset_date(original_date, offset_days):
    if not original_date:
        return ""
    if " " in original_date:
        original_datetime = datetime.strptime(original_date, "%d.%m.%Y %H:%M:%S")
    else:
        original_datetime = datetime.strptime(original_date, "%d.%m.%Y")
    offset_datetime = original_datetime - offset_days
    return offset_datetime.strftime("%d.%m.%Y %H:%M:%S")


dates = [line[4] for line in lines[1:] if line[4]]
min_date = min(datetime.strptime(date, "%d.%m.%Y %H:%M:%S") for date in dates)
date_offset = min_date - datetime(2000, 1, 1)

# Anonymize data
for i, line in enumerate(lines[1:], 1):  # Skip header
    if line[0]:  # If the line is not empty
        line[0] = edrpou_mappings.setdefault(line[0], get_new_edrpou())
        line[2] = account_mappings.setdefault(line[2], get_new_iban())
        line[8] = account_mappings.setdefault(line[8], get_new_iban())
        line[4] = get_offset_date(line[4], date_offset)
        line[12] = get_offset_date(line[12], date_offset)
        line[10] = correspondent_mappings.setdefault(line[10], get_new_correspondent())
        line[9] = edrpou_mappings.setdefault(line[9], get_new_edrpou())
        line[1] = nabu_id_mappings.setdefault(line[1], get_new_nabu_id())
        # Redact dates in payment description
        line[15] = redact_dates_in_description(line[15])
        if "nvoice" in line[15]:
            line[15] = "REDACTED"

# Convert the data back to string
output_data = "\n".join([";".join(line) for line in lines])
sys.stdout.buffer.write(output_data.encode("cp1251"))
