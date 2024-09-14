# inputs.tf
# Specify values using
#   terraform apply -var 'app_version=1.2.3'

variable "app_version" {
  type = string
}
