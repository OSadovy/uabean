import re
import sys
from datetime import datetime
from xml.etree import ElementTree as ET


def adjust_date(date_str, offset, date_format="%Y%m%d"):
    return (datetime.strptime(date_str, date_format) + offset).strftime(date_format)


def adjust_datetime(datetime_str, offset, date_format="%Y%m%d"):
    if ";" not in datetime_str:
        return adjust_date(datetime_str, offset, date_format)
    date_part, time_part = datetime_str.split(";")
    adjusted_date = adjust_date(date_part, offset, date_format)
    return f"{adjusted_date};{time_part}"


def adjust_account_id(element, id_mapping):
    account_id = element.get("accountId")
    if account_id not in id_mapping:
        id_mapping[account_id] = f"DUMMYID{len(id_mapping)+1}"
    element.set("accountId", id_mapping[account_id])


def adjust_unique_refs(element, unique_refs, id_mapping):
    for ref in unique_refs:
        original_id = element.get(ref)
        if original_id:
            if original_id not in id_mapping:
                id_mapping[original_id] = f"DUMMYTRADEID{len(id_mapping)+1}"
            element.set(ref, id_mapping[original_id])


def adjust_dates_and_times(element, offset):
    for attr in element.attrib:
        if not element.get(attr):
            continue
        if "datetime" in attr.lower():
            element.set(attr, adjust_datetime(element.get(attr), offset))
        elif "date" in attr.lower():
            element.set(attr, adjust_date(element.get(attr), offset))


def anonymize_dates_in_description(description, offset):
    # Match common date patterns e.g. APR-2020, 2020-04-03, 2020/04/03 etc.
    date_patterns = [
        (r"(\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-\d{4}\b)", "%b-%Y"),
        (r"(\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) \d{4}\b)", "%b %Y"),
        (r"(\b\d{4}-\d{2}-\d{2}\b)", "%Y-%m-%d"),
        (r"(\b\d{4}/\d{2}/\d{2}\b)", "%Y/%m/%d"),
    ]

    for pattern, date_format in date_patterns:
        matches = re.findall(pattern, description, re.IGNORECASE)
        for match in matches:
            try:
                original_date_str = match[
                    0
                ]  # Match tuple's first element is the complete matched string
                adjusted_date_str = adjust_date(original_date_str, offset, date_format)
                description = description.replace(original_date_str, adjusted_date_str)
            except ValueError:
                # If a date string does not match the expected format, we'll skip it.
                pass
    return description


def adjust_cash_transaction(cash_transaction, id_mapping, trade_id_mapping, offset):
    adjust_account_id(cash_transaction, id_mapping)
    adjust_unique_refs(
        cash_transaction, ["tradeID", "transactionID", "actionID"], trade_id_mapping
    )
    adjust_dates_and_times(cash_transaction, offset)
    # Anonymize date references in the description
    description = cash_transaction.get("description", "")
    cash_transaction.set(
        "description", anonymize_dates_in_description(description, offset)
    )


def anonymize_xml(xml_data):
    tree = ET.ElementTree(ET.fromstring(xml_data))
    root = tree.getroot()

    earliest_date = min(
        [elem.get("fromDate") for elem in root.findall(".//FlexStatement")]
        + [elem.get("toDate") for elem in root.findall(".//FlexStatement")]
    )
    base_date = "20000101"
    date_format = "%Y%m%d"
    offset = datetime.strptime(base_date, date_format) - datetime.strptime(
        earliest_date, date_format
    )

    id_mapping = {}
    trade_id_mapping = {}

    for flex_statement in root.findall(".//FlexStatement"):
        adjust_account_id(flex_statement, id_mapping)
        adjust_dates_and_times(flex_statement, offset)

    for report_currency in root.findall(".//CashReportCurrency"):
        adjust_account_id(report_currency, id_mapping)
        adjust_dates_and_times(report_currency, offset)

    trade_and_lot_elements = root.findall(".//Trade") + root.findall(".//Lot")
    for trade in trade_and_lot_elements:
        adjust_account_id(trade, id_mapping)
        adjust_unique_refs(
            trade,
            [
                "tradeID",
                "transactionID",
                "ibOrderID",
                "ibExecID",
                "brokerageOrderID",
                "extExecID",
            ],
            trade_id_mapping,
        )
        adjust_dates_and_times(trade, offset)

    for corp_action in root.findall(".//CorporateAction"):
        adjust_account_id(corp_action, id_mapping)
        adjust_unique_refs(corp_action, ["transactionID", "actionID"], trade_id_mapping)
        adjust_dates_and_times(corp_action, offset)

    for cash_transaction in root.findall(".//CashTransaction"):
        adjust_cash_transaction(cash_transaction, id_mapping, trade_id_mapping, offset)

    return ET.tostring(root).decode()


anonymized_xml = anonymize_xml(open(sys.argv[1]).read())
print(anonymized_xml)
