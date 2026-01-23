#!/usr/bin/env python3.12
"""Test the settlement layer endpoint with background processing"""
import requests
import json
import time

BASE_URL = "http://127.0.0.1:8000"
PROJECT_ID = "8ddfbcf2-bba9-4ad0-be0c-f86274087528"

# Get authentication token
print("Getting authentication token...")
response = requests.post(
    f"{BASE_URL}/auth/token",
    data={"username": "admin", "password": "admin"}
)
token = response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
print("✓ Authenticated\n")

# Trigger settlement layer generation
print("=== Triggering settlement layer generation ===")
response = requests.post(
    f"{BASE_URL}/projects/{PROJECT_ID}/layers/settlements",
    headers=headers,
    json={"building_buffer": 10, "settlement_eps": 50, "min_buildings": 5}
)

print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    print(json.dumps(response.json(), indent=2))
else:
    print(f"Error: {response.text}")
    exit(1)

# Poll for status updates
print("\n=== Polling for layer status ===")
for i in range(6):  # Poll for 30 seconds
    time.sleep(5)
    response = requests.get(
        f"{BASE_URL}/projects/{PROJECT_ID}/layers",
        headers=headers
    )
    print(f"\n--- Poll #{i+1} (after {(i+1)*5} seconds) ---")
    layers = response.json()
    all_done = True
    for layer in layers:
        status_icon = "✓" if layer['status'] == "successful" else "⚠" if layer['status'] == "failed" else "⏳"
        print(f"{status_icon} {layer['name']}: {layer['status']}")
        print(f"   └─ {layer['details']}")
        if layer['status'] == 'in_progress':
            all_done = False
    
    if all_done:
        print("\n✅ All layers completed!")
        break
else:
    print("\n⏰ Polling timeout - layers still processing")
