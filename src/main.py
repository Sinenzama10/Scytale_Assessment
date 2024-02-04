# src/main.py

import os
import json
from extract import get_repos, get_pull_requests
from spark_process import transform_and_save
# Import the validation function
from parquet_check import check_parquet_file

def main():
    try:
        repos = get_repos()
        for repo in repos:
            repo_name = repo.get('name')
            owner = repo.get('owner').get('login')
            prs = get_pull_requests(owner, repo_name)
            
            with open(f'../data/{repo_name}_prs.json', 'w') as f:
                json.dump(prs, f)
        
        output_parquet_path = '../output/prs_aggregated.parquet'
        transform_and_save('../data/*.json', output_parquet_path)
        
        # After saving the Parquet file, validate it
        check_parquet_file(output_parquet_path)
        
        # Print completion message
        print("ETL pipeline has been run successfully.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
