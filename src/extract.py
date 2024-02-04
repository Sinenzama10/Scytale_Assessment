# src/extract.py

import time
import requests  # Ensure requests is imported if not already
from config import GITHUB_TOKEN, ORG_NAME  # Import variables from config.py

def safe_request(url, headers):
    """
    Makes a safe HTTP GET request to the specified URL, handling GitHub's rate limiting by waiting or retrying as necessary.
    
    Parameters:
    - url (str): The URL to make the request to.
    - headers (dict): The headers to include in the request, typically for authentication.
    
    Returns:
    - response: The response object from the requests library.
    
    This function first checks the current rate limit status. If it's close to being exceeded,
    the function waits until the reset time. If a request is made and the rate limit is exceeded,
    it waits for the specified 'Retry-After' time before retrying the request.
    """
    # Check the current rate limit status
    remaining, reset_time = check_rate_limit()
    if remaining < 10:
        wait_time = reset_time - time.time() + 10  # Calculate wait time, add buffer
        print(f"Approaching rate limit. Waiting for {wait_time} seconds.")
        time.sleep(max(wait_time, 0))  # Wait until the reset time

    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 60))  # Use default if not specified
        print(f"Rate limit exceeded. Retrying after {retry_after} seconds.")
        time.sleep(retry_after)
        return safe_request(url, headers)  # Retry the request

    return response

def get_repos():
    """
    Fetches all repositories for a given organization from GitHub.
    
    Returns:
    - repos (list): A list of repository data in JSON format.
    
    Utilizes the safe_request function to make paginated API calls to GitHub, fetching all
    repositories associated with the specified organization. It handles pagination by following
    the 'next' link provided by GitHub's API until there are no more pages left.
    """
    repos = []
    url = f"https://api.github.com/orgs/{ORG_NAME}/repos"
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}

    while url:
        response = safe_request(url, headers)
        repos.extend(response.json())
        url = response.links.get('next', {}).get('url', None)

    return repos

def get_pull_requests(owner, repo):
    """
    Fetches all pull requests for a specific repository from GitHub.
    
    Parameters:
    - owner (str): The username of the repository owner.
    - repo (str): The repository name.
    
    Returns:
    - prs (list): A list of pull request data in JSON format.
    
    This function makes paginated API calls to GitHub to fetch all pull requests for the specified
    repository, utilizing the safe_request function to respect rate limits and handle pagination.
    """
    prs = []
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=all"
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}

    while url:
        response = safe_request(url, headers)
        prs.extend(response.json())
        url = response.links.get('next', {}).get('url', None)

    return prs

def check_rate_limit():
    """
    Checks the current rate limit status with GitHub's API.
    
    Returns:
    - remaining (int): The number of requests remaining in the current rate limit window.
    - reset_time (int): The time at which the current rate limit window resets, in Unix epoch seconds.
    
    This function makes an API call to GitHub's rate limit endpoint to check the current status of
    the core rate limit. It returns the number of remaining requests and the reset time to be used
    for managing subsequent API calls within rate limits.
    """
    url = "https://api.github.com/rate_limit"
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    response = requests.get(url, headers=headers).json()
    core_limit = response['resources']['core']
    remaining = core_limit['remaining']
    reset_time = core_limit['reset']

    return remaining, reset_time
