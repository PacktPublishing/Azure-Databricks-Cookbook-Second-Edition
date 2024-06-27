
# Managing privileges in Unity Catalog

# Prerequisites
# Administrative privileges in the metastore

def grant_privilege(databricks_instance, token, privilege, object_name, user_or_group):
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = {
        "privilege": privilege,
        "object": object_name,
        "principal": user_or_group,
    }

    response = requests.post(f"{databricks_instance}/api/2.0/unity-catalog/permissions", headers=headers, json=data)
    return response.json()

# Example usage:
# privilege_grant = grant_privilege('https://databricks-instance', 'your_token', 'SELECT', 'sales.default.Customers', 'sales_analysts')
        