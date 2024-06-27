
# Getting started with using Unity Catalog

# Prerequisites
# An Azure Databricks workspace associated with a Unity Catalog metastore

def assign_workspace_to_metastore(databricks_instance, token, workspace_id, metastore_id):
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = {
        "metastore_id": metastore_id,
        "workspace_id": workspace_id,
    }

    response = requests.post(f"{databricks_instance}/api/2.0/unity-catalog/workspaces", headers=headers, json=data)
    return response.json()

# Example usage:
# assignment = assign_workspace_to_metastore('https://databricks-instance', 'your_token', 'your_workspace_id', 'your_metastore_id')
        