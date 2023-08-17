from elasticsearch import Elasticsearch
import csv
import sys

from fixtures import tracked_user_list, included_fields


# Set the encoding to handle Unicode characters
if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding('utf-8')

# Connect to Elasticsearch with authentication
# Connect to Elasticsearch
es = Elasticsearch(['host'],
                   http_auth=('username', 'password')
                )  # Replace with your Elasticsearch server details

# Define the index and query
index_name = 'index'
query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"platform.keyword": "Twitter"}},
                {"terms": {"language.keyword": ["en", "fr", "es", "it"]}},
                {"term": {"is_abusive": False}}
            ],
            "filter": {
                "range": {
                    "created_at": {
                        "gte": "2023-05-12T07:00:00.000Z",
                        "lte": "2023-05-30T07:00:00.000Z"
                    }
                }
            }
        }
    }
}

# Execute the search query
response = es.search(index=index_name, body=query, scroll='5m', size=10)
scroll_id = response['_scroll_id']
hits = response['hits']['hits']

# Open the CSV file for writing
csv_file = open('exported_data.csv', 'w', newline='', encoding='utf-8')  # Specify UTF-8 encoding
csv_writer = csv.writer(csv_file)

# Write the header row
csv_writer.writerow(included_fields)
x = 1
# Write the data rows
# Write the data rows
while len(hits) > 0:
    for hit in hits:
        data_row = []
        for field in included_fields:
            try:
                value = hit["_source"][field]
                cleaned_value = str(value).replace('\uFFFD', '') if value is not None else None
                data_row.append(cleaned_value)
            except KeyError as e:
                print(f"KeyError: {e} - Skipping field '{field}'")
                data_row.append(None)  # Append None for missing fields
        data_row.append(hit["_id"])
        if data_row[22] in tracked_user_list:
            print("writing row")
            print(x)
            x += 1
            csv_writer.writerow(data_row)
    response = es.scroll(scroll_id=scroll_id, scroll='5m')
    hits = response['hits']['hits']


# Close the CSV file
csv_file.close()
