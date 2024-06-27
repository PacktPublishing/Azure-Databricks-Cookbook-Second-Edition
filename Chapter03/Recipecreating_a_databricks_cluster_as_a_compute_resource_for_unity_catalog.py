
# Creating a Databricks cluster as a compute resource for Unity Catalog

# Prerequisites
# Workspace admin privileges or permission to create clusters

def create_databricks_cluster(databricks_instance, token, cluster_name, runtime_version):
    import requests

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = {
        "cluster_name": cluster_name,
        "spark_version": runtime_version,
        "node_type_id": "Standard_DS3_v2",
        "autoscale": {"min_workers": 2, "max_workers": 8},
        "spark_conf": {
            "spark.databricks.delta.preview.enabled": "true",
            "spark.databricks.passthrough.enabled": "true"
        },
        "custom_tags": {
            "ResourceClass": "SingleNode"
        }
    }

    response = requests.post(f"{databricks_instance}/api/2.0/clusters/create", headers=headers, json=data)
    return response.json()

# Example usage:
# cluster = create_databricks_cluster('https://databricks-instance', 'your_token', 'your_cluster_name', '11.3.x-scala2.12')
        