terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
}

provider "azurerm" {
  features {}
}

# Skeleton only (client tenant required):
# - resource group
# - storage account (ADLS Gen2)
# - container registry (ACR)
# - event hubs (optional)
