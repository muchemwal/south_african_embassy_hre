from google.cloud import bigquery

dataset = bigquery.Dataset(embasy_pipeline)

dataset.location = "US"

# Send the dataset to the API for creation.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset)  # API request
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
