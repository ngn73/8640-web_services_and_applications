'''
assignment04-github.py
Author  Niall Naughton
Date    01/03/2026

----------------------------------------------------------------------------------
Description     Weekly Task #4 Authentication with GitHub API

Write a program in python that will read a file from a repository, 
The program should then replace all the instances of the text "Andrew" with your name. 
The program should then commit those changes and push the file back to the repository (You will need authorisation to do this).
I do not need to show keys (see lab2)
----------------------------------------------------------------------------------
'''
import base64
import requests
from config import apikeys as cfg

repo_user = "ngn73"
private_repo = "8640-web_services_and_applications_private"
filename = "sample_page.txt"
branch = "main"

gh_token = cfg["github_token"]

#old_word = "Andrew"
old_word = "Niall"

#new_word = "Niall"
new_word = "Andrew"


headers = {
    "Authorization": "token " + gh_token
    }

url = f"https://api.github.com/repos/{repo_user}/{private_repo}/contents/{filename}"

# 1. Read current file
resp = requests.get(url, headers=headers)
print(f"GET file response: {resp.status_code}")

#Get Decoded content and SHA for file
resp.raise_for_status()
file_info = resp.json()
current_sha = file_info["sha"]
encoded_content = file_info["content"]
decoded_content = base64.b64decode(encoded_content).decode("utf-8")


# 2. Replace text
updated_content = decoded_content.replace(old_word, new_word)

if updated_content == decoded_content:
    print("No changes made.")
    raise SystemExit(0)

# 3. Push updated file
new_encoded_content = base64.b64encode(updated_content.encode("utf-8")).decode("utf-8")

payload = {
    "message": f"Replaced '{old_word}' with '{new_word}' in {filename}",
    "content": new_encoded_content,
    "sha": current_sha,
    "branch": branch,
}

update_resp = requests.put(url, headers=headers, json=payload)
update_resp.raise_for_status()

print("File updated and committed successfully.")
print(update_resp.json()["commit"]["html_url"])
