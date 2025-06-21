import csv

input_file = "data.csv"
output_file = "cleaned_data.csv"
expected_columns = 19
bad_lines = 0

with open(input_file, newline='', encoding='utf-8') as infile, \
     open(output_file, "w", newline='', encoding='utf-8') as outfile:
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    header = next(reader)
    writer.writerow(header)

    for i, row in enumerate(reader, start=2):
        if len(row) != expected_columns:
            print(f"Skipping line {i}: Expected {expected_columns} columns, got {len(row)}")
            bad_lines += 1
            continue
        writer.writerow(row)

print(f"Done. Skipped {bad_lines} bad line(s). Cleaned file saved to {output_file}.")
