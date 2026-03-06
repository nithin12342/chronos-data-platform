# Azure Synapse Analytics Configuration
# Chronos Data Platform - Data Warehouse

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

# Resource Group
resource "azurerm_resource_group" "synapse" {
  name     = "rg-chronos-synapse-${var.environment}"
  location = "eastus"

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Virtual Network
resource "azurerm_virtual_network" "synapse_vnet" {
  name                = "vnet-chronos-synapse-${var.environment}"
  location            = azurerm_resource_group.synapse.location
  resource_group_name = azurerm_resource_group.synapse.name
  address_space       = ["10.2.0.0/22"]

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Subnet for Synapse
resource "azurerm_subnet" "synapse_subnet" {
  name                 = "snet-synapse"
  resource_group_name  = azurerm_resource_group.synapse.name
  virtual_network_name = azurerm_virtual_network.synapse_vnet.name
  address_prefixes    = ["10.2.0.0/24"]

  delegation {
    name = "Microsoft.Synapse/workspaces"
    service_delegation {
      name    = "Microsoft.Synapse/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

# Storage Account for Synapse Lake
resource "azurerm_storage_account" "synapse_lake" {
  name                     = "stchronosynapselk${var.environment}"
  location                 = azurerm_resource_group.synapse.location
  resource_group_name      = azurerm_resource_group.synapse.name
  account_tier            = "Standard"
  account_replication_type = "LRS"
  account_kind            = "StorageV2"
  is_hns_enabled          = true

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Data Lake Gen2 Filesystem
resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = "chronosdatalake"
  storage_account_id = azurerm_storage_account.synapse_lake.id
}

# Synapse Workspace
resource "azurerm_synapse_workspace" "synapse_ws" {
  name                = "synw-chronos-${var.environment}"
  location            = azurerm_resource_group.synapse.location
  resource_group_name = azurerm_resource_group.synapse.name
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.datalake.id
  
  sql_admin_login                 = "sqladmin"
  sql_admin_password               = "@@PASSWORD@@"
  
  managed_virtual_network_enabled = true
  managed_identity = true

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Synapse SQL Pool (DW)
resource "azurerm_synapse_sql_pool" "dw_pool" {
  name                 = "DW100c"
  synapse_workspace_id  = azurerm_synapse_workspace.synapse_ws.id
  sku_name             = "DW100c"
  collation           = "SQL_Latin1_General_CP1_CI_AS"

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Synapse Spark Pool
resource "azurerm_synapse_spark_pool" "spark_pool" {
  name                  = "spark-pool"
  synapse_workspace_id   = azurerm_synapse_workspace.synapse_ws.id
  node_size             = "Small"
  node_count            = 3
  auto_scale {
    min_node_count = 2
    max_node_count = 10
  }
  auto_pause {
    enabled = true
    delay_in_minutes = 15
  }

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Integration Runtime
resource "azurerm_synapse_integration_runtime_azure" "auto_ir" {
  name                  = "auto-resolver"
  synapse_workspace_id   = azurerm_synapse_workspace.synapse_ws.id
  location             = azurerm_resource_group.synapse.location
  
  compute_type          = "General"
  core_count           = 8

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Linked Service - SQL DB
resource "azurerm_synapse_linked_service" "sql_db" {
  name                 = "AzureSQLDB"
  synapse_workspace_id  = azurerm_synapse_workspace.synapse_ws.id
  type                 = "AzureSqlDatabase"
  type_properties_json = jsonencode({
    connectionString = "Server=tcp:sql-chronos.database.windows.net,1433;Database=chronos;User ID=sqladmin;Password=@@PASSWORD@@;Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;"
  })
}

# Linked Service - Blob Storage
resource "azurerm_synapse_linked_service" "blob_storage" {
  name                 = "BlobStorage"
  synapse_workspace_id  = azurerm_synapse_workspace.synapse_ws.id
  type                 = "AzureBlobStorage"
  type_properties_json = jsonencode({
    connectionString = "DefaultEndpointsProtocol=https;AccountName=${azurerm_storage_account.synapse_lake.name};AccountKey=@@KEY@@;EndpointSuffix=core.windows.net"
  })
}

# Dataset - Transactions
resource "azurerm_synapse_dataset" "transactions" {
  name                 = "transactions_ds"
  synapse_workspace_id = azurerm_synapse_workspace.synapse_ws.id
  type                 = "DelimitedText"
  type_properties_json = jsonencode({
    location = {
      type        = "AzureBlobStorageLocation"
      container   = "bronze"
      fileName    = "transactions/*.csv"
    }
    columnDelimiter = ","
    firstRowAsHeader = true
  })
  
  linked_service_name = azurerm_synapse_linked_service.blob_storage.name
}

# Pipeline - Bronze to Silver
resource "azurerm_synapse_pipeline" "bronze_to_silver" {
  name                = "bronze_to_silver_pipeline"
  synapse_workspace_id = azurerm_synapse_workspace.synapse_ws.id

  activities_json = jsonencode([
    {
      name = "CopyTransactions"
      type = "Copy"
      typeProperties = {
        source = {
          type = "DelimitedTextSource"
        }
        sink = {
          type = "SqlDWSink"
        }
      }
    }
  ])
}

# Outputs
output "synapse_workspace_url" {
  value = azurerm_synapse_workspace.synapse_ws.endpoint
}

output "spark_pool_name" {
  value = azurerm_synapse_spark_pool.spark_pool.name
}

output "sql_pool_name" {
  value = azurerm_synapse_sql_pool.dw_pool.name
}
