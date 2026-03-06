# Azure Event Hubs Configuration
# Chronos Data Platform - Streaming Infrastructure

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
resource "azurerm_resource_group" "eventhubs" {
  name     = "rg-chronos-events-${var.environment}"
  location = "eastus"

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Event Hubs Namespace
resource "azurerm_eventhub_namespace" "chronos_ns" {
  name                = "eh-chronos-${var.environment}"
  location            = azurerm_resource_group.eventhubs.location
  resource_group_name = azurerm_resource_group.eventhubs.name
  sku                 = "Standard"
  capacity            = 2
  zone_redundant      = true

  auto_inflate_enabled     = true
  max_capacity            = 4
  partition_count         = 4
  message_retention       = 7

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Event Hub - Transactions
resource "azurerm_eventhub" "transactions" {
  name                = "transactions"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  partition_count    = 4
  message_retention  = 7

  capture_description {
    enabled = true
    encoding = "Avro"
    
    destination {
      type                = "EventHub"
      eventhub_name       = "transactions-archive"
      storage_account_id  = azurerm_storage_account.eventhub_storage.id
      blob_container_name = azurerm_storage_container.transactions_archive.name
    }
  }
}

# Event Hub - User Events
resource "azurerm_eventhub" "user_events" {
  name                = "user-events"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  partition_count    = 4
  message_retention  = 3
}

# Event Hub - IoT Telemetry
resource "azurerm_eventhub" "iot_telemetry" {
  name                = "iot-telemetry"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  partition_count    = 8
  message_retention  = 1

  retention_description {
    cleanup_policy = "Delete"
    retention_time = "PT168H"
  }
}

# Event Hub - Application Logs
resource "azurerm_eventhub" "app_logs" {
  name                = "application-logs"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  partition_count    = 2
  message_retention  = 2
}

# Event Hub - Archive (for capture)
resource "azurerm_eventhub" "transactions_archive" {
  name                = "transactions-archive"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  partition_count    = 4
  message_retention  = 7
}

# Consumer Groups
resource "azurerm_eventhub_consumer_group" "analytics_group" {
  name                = "analytics-consumer"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.eventhubs.name
}

resource "azurerm_eventhub_consumer_group" "ml_group" {
  name                = "ml-consumer"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.eventhubs.name
}

resource "azurerm_eventhub_consumer_group" "dwh_group" {
  name                = "dwh-consumer"
  namespace_name      = azurerm_eventhub_namespace.chronos_ns.name
  eventhub_name       = azurerm_eventhub.user_events.name
  resource_group_name = azurerm_resource_group.eventhubs.name
}

# Storage Account for Event Hub Capture
resource "azurerm_storage_account" "eventhub_storage" {
  name                     = "stchronosevents${var.environment}"
  location                 = azurerm_resource_group.eventhubs.location
  resource_group_name      = azurerm_resource_group.eventhubs.name
  account_tier            = "Standard"
  account_replication_type = "LRS"
  account_kind            = "StorageV2"

  tags = {
    Environment = var.environment
    Project    = "chronos-data-platform"
  }
}

# Blob Container for Archive
resource "azurerm_storage_container" "transactions_archive" {
  name                  = "transactions-archive"
  storage_account_name  = azurerm_storage_account.eventhub_storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "user_events_archive" {
  name                  = "user-events-archive"
  storage_account_name  = azurerm_storage_account.eventhub_storage.name
  container_access_type = "private"
}

# Authorization Rules
resource "azurerm_eventhub_namespace_authorization_rule" "root" {
  namespace_name = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  name            = "root-manage"
  rights         = ["Listen", "Manage", "Send"]
}

resource "azurerm_eventhub_namespace_authorization_rule" "analytics" {
  namespace_name = azurerm_eventhub_namespace.chronos_ns.name
  resource_group_name = azurerm_resource_group.eventhubs.name
  name            = "analytics-listen-send"
  rights         = ["Listen", "Send"]
}

# Outputs
output "eventhub_namespace" {
  value = azurerm_eventhub_namespace.chronos_ns.name
}

output "connection_string" {
  value = azurerm_eventhub_namespace_authorization_rule.root.primary_connection_string
  sensitive = true
}
