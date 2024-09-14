# config.tf

# Configure Terraform
# This block should normally never be changed except for "key" and "required_version"
terraform {
  required_version = ">= 0.12.2"

  backend "s3" {
    #### KEY MUST BE UNIQUE ACROSS ALL MODULES ####
    key = "demand-model-d"

    ####
    bucket         = "freshly-terraform-remote-state-us-east-1"
    region         = "us-east-1"
    dynamodb_table = "terraform-remote-state-lock"
  }
}

# Pull data from other Terraform modules
data "terraform_remote_state" "iam" {
  backend   = "s3"
  workspace = terraform.workspace
  config = {
    key    = "iam"
    bucket = "freshly-terraform-remote-state-us-east-1"
    region = "us-east-1"
  }
}

data "terraform_remote_state" "monitoring" {
  backend   = "s3"
  workspace = terraform.workspace

  config = {
    key    = "monitoring"
    bucket = "freshly-terraform-remote-state-us-east-1"
    region = "us-east-1"
  }
}

locals {
  env = terraform.workspace

  common_tags = {
    Terraform   = "1"
    Environment = terraform.workspace
  }
}
