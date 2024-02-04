# src/main.py

import os
import json
from extract import get_repos, get_pull_requests
from spark_process import transform_and_save

def main():
    repos = get_repos()
    for repo in repos:
        repo_name = repo.get('name')
        owner = repo.get('owner').get('login')
        prs = get_pull_requests(owner, repo_name)
        
        # Assuming data directory exists
        with open(f'../data/{repo_name}_prs.json', 'w') as f:
            json.dump(prs, f)
    
    # Assuming the JSON files are saved correctly in the data directory
    transform_and_save('../data/*.json', '../output/prs_aggregated.parquet')

if __name__ == "__main__":
    main()
