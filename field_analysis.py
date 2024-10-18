import looker_sdk
from looker_sdk import models40 as mdls
import json
import os
from collections import defaultdict
import pandas as pd

sdk = looker_sdk.init40('looker.ini')

if not os.path.exists('query_results.json'):
    print('running sys activity history query')
    response = sdk.run_inline_query(
        result_format="json",
        body=mdls.WriteQuery(
            model="system__activity",
            view="history",
            fields=[
                "query.model",
                "query.view",
                "query.formatted_fields",
                "query.filters",
                "history.query_run_count"
            ],
            filters={
                "history.created_date": "180 days",
                "query.formatted_fields": "-NULL",
                "history.workspace_id": "production"
            },
            limit=-1
            )
        )
    data = json.loads(response)

    with open('query_results.json', 'w') as f:
        json.dump(data, f, indent=4)  # Use indent for pretty printing
    print("JSON data saved to query_results.json") 
else:
    print('grabbing json data')
    with open('query_results.json', 'r') as f:
        data = json.load(f)

    print(len(data))

unique_fields_by_model_view = {}

for row in data:
    model = row['query.model']
    view = row['query.view']
    model_view_key = f"{model}.{view}"  # Create a key combining model and view

    try:
        fields = json.loads(row['query.formatted_fields'])
        if model_view_key not in unique_fields_by_model_view:
            unique_fields_by_model_view[model_view_key] = set()
        unique_fields_by_model_view[model_view_key].update(fields)
    except json.JSONDecodeError:
        print(f"Error parsing query.formatted_fields in row: {row}")

# Print the unique fields for each model and view

# model_view_key = "thelook.order_items"
# if model_view_key in unique_fields_by_model_view:
#     print(f"\nUnique fields for {model_view_key}:\n{list(unique_fields_by_model_view[model_view_key])}")
# else:
#     print(f"\nNo data found for {model_view_key}")

# for model_view_key, fields in unique_fields_by_model_view.items():
    # print(f"{model_view_key}: {list(fields)}") 

field_frequencies_by_model_view = defaultdict(lambda: defaultdict(int)) 
use_run_count_as_freq = False
# Iterate through the data and count field occurrences
for row in data:
    model = row['query.model']
    view = row['query.view']
    model_view_key = f"{model}.{view}" 
    query_run_count = row['history.query_run_count']  # Get the query run count


    try:
        fields = json.loads(row['query.formatted_fields'])
        for field in fields:
            if use_run_count_as_freq:
                field_frequencies_by_model_view[model_view_key][field] += query_run_count  
            else: 
                field_frequencies_by_model_view[model_view_key][field] += 1 


    except json.JSONDecodeError:
        print(f"Error parsing query.formatted_fields in row: {row}")

# Print the results in a tabular format
print("Model View\tField\tFrequency")
print("-" * 40)  # Print a separator line

data_for_df = []
for model_view_key, field_counts in field_frequencies_by_model_view.items():
    for field, count in field_counts.items():
        data_for_df.append([model_view_key, field, count])

# Create the pandas DataFrame
df = pd.DataFrame(data_for_df, columns=["Model View", "Field", "Frequency"])

# Print the DataFrame (this will display nicely in VS Code)
print(df)

# Optionally, save the DataFrame to a CSV file
if use_run_count_as_freq:
    df.to_csv("field_frequencies_run_count.csv", index=False) 
else: 
    df.to_csv("field_frequencies.csv", index=False) 
