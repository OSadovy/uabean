import csv
import datetime
import random

fname = 'binance.csv'

with open(fname ) as f:
    data = f.read()

lines = data.strip().split('\n')
reader = csv.reader(lines)
headers = next(reader)
data_rows = list(reader)

# Replace User_ID
dummy_id = "123456789"
for row in data_rows:
    row[0] = dummy_id

# Map original dates to new dates
original_dates = {row[1] for row in data_rows}
base_date = datetime.datetime(2000, 1, 1)
date_mapping = {date: (base_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d %H:%M:%S')
                for i, date in enumerate(sorted(original_dates))}

for row in data_rows:
    row[1] = date_mapping[row[1]]

# Change amounts
for row in data_rows:
    try:
        change = float(row[5])
        scale = abs(change) * 0.05  # Up to 5% of original amount
        random_change = random.uniform(-scale, scale)
        new_change = change + random_change
        
        # Ensure signs stay consistent
        if (change < 0 and new_change > 0) or (change > 0 and new_change < 0):
            new_change = -new_change
            
        row[5] = "{:.8f}".format(new_change)
    except ValueError:
        pass

# Combine the data and print
output = [headers] + data_rows
with open(fname , 'w') as f:
    for row in output:
        print(','.join(row), file=f)
