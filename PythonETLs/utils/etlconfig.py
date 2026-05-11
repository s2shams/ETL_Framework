from google.cloud import bigquery

EMAIL_LIST = [
    's2shams@uwaterloo.ca'
]

def get_project_id(target):
    if target == 'dev':
        return 'ingestion-dev-495504'
    elif target == 'prod':
        return 'ingestion-prod-495504'
    
def get_BQ_client(target):
    project_id = get_project_id(target)
    return bigquery.Client(project=project_id)
