
# Creating and configuring storage for Unity Catalog

# Prerequisites
# An Azure subscription with Contributor access
# A resource group within the subscription

def create_storage_account(subscription_id, resource_group, storage_account_name, region):
    from azure.mgmt.storage import StorageManagementClient
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    storage_client = StorageManagementClient(credential, subscription_id)

    async_storage_account_create = storage_client.storage_accounts.begin_create(
        resource_group,
        storage_account_name,
        {
            "location": region,
            "sku": {"name": "Standard_LRS"},
            "kind": "StorageV2",
            "properties": {"isHnsEnabled": True},
        },
    )
    storage_account = async_storage_account_create.result()
    return storage_account

# Example usage:
# storage_account = create_storage_account('your_subscription_id', 'your_resource_group', 'your_storage_account_name', 'your_region')
        