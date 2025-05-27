terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.56"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.1"
    }
    ansible = {
      source = "ansible/ansible"
      version = "1.3.0"
    }
  }
}

variable "public_key_path" {
  description = <<DESCRIPTION
Path to the SSH public key to be used for authentication.
Ensure this keypair is added to your local SSH agent so provisioners can
connect.

Example: ~/.ssh/sls_aws.pub
DESCRIPTION
}

resource "random_id" "hash" {
  byte_length = 8
}

variable "key_name" {
  default     = "melhindi_aws_us-east"
  description = "AWS key pair"
}

variable "region" {}
variable "az" {}
variable "ami" {}
variable "user" {}
variable "spot" {}
variable "instance_types" {}
variable "num_instances" {}

provider "aws" {
  region = var.region
  default_tags {
   tags = {
      Project = "sls-benchmark"
   }
  }
}

# Create a VPC to launch our instances into
resource "aws_vpc" "benchmark_vpc" {
  cidr_block = "10.0.0.0/16"
  enable_dns_hostnames = true

  tags = {
    Name = "SLS-VPC-${random_id.hash.hex}"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "sls" {
  vpc_id = aws_vpc.benchmark_vpc.id
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = aws_vpc.benchmark_vpc.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.sls.id
}

# Create a subnet to launch our instances into
resource "aws_subnet" "benchmark_subnet" {
  vpc_id                  = aws_vpc.benchmark_vpc.id
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true
  availability_zone       = var.az
}

resource "aws_security_group" "benchmark_security_group" {
  name   = "terraform-sls-${random_id.hash.hex}"
  vpc_id = aws_vpc.benchmark_vpc.id

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All ports open within the VPC
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # SLS port
#  ingress {
#    from_port   = 5000
#    to_port     = 5000
#    protocol    = "tcp"
#    cidr_blocks = ["0.0.0.0/0"]
#  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "SLS-Security-Group-${random_id.hash.hex}"
  }
}

resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}"
  public_key = file(var.public_key_path)
}

resource "aws_instance" "sls" {
  ami           = var.ami
  instance_type = var.instance_types["sls"]
  #key_name      = data.aws_key_pair.auth.key_name
  key_name      = aws_key_pair.auth.id
  subnet_id     = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [
  aws_security_group.benchmark_security_group.id]
  count = var.num_instances["sls"]
  dynamic "instance_market_options" {
     for_each = var.spot ? [1] : []
     content {
         market_type = "spot"
         spot_options {
           max_price = 3.0
         }
     }
  }

  tags = {
    Name = "sls-${count.index}"
  }
}

resource "aws_instance" "client" {
  ami           = var.ami
  instance_type = var.instance_types["client"]
  #key_name      = data.aws_key_pair.auth.key_name
  key_name      = aws_key_pair.auth.id
  subnet_id     = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [
  aws_security_group.benchmark_security_group.id]
  count = var.num_instances["client"]
  dynamic "instance_market_options" {
     for_each = var.spot ? [1] : []
     content {
         market_type = "spot"
         spot_options {
           max_price = 3.0
         }
     }
  }

  root_block_device {
    volume_size = 16  # Set the root volume size to 10GB
    volume_type = "gp2"
  }

  tags = {
    Name = "sls-client-${count.index}"
  }
}

# Inventory host resource.
resource "ansible_host" "sls" {
  name = "sls-${count.index}"
  groups = ["sls"] # Groups this host is part of.
  count = var.num_instances["sls"]

  variables = {
    # Connection vars.
    ansible_user = var.user # Default user depends on the OS.
    ansible_host = aws_instance.sls[count.index].public_ip

    # Custom vars that we might use in roles/tasks.
  }
}
resource "ansible_host" "client" {
  name = "client-${count.index}"
  groups = ["client"] # Groups this host is part of.
  count = var.num_instances["client"]

  variables = {
    # Connection vars.
    ansible_user = var.user # Default user depends on the OS.
    ansible_host = aws_instance.client[count.index].public_ip

    # Custom vars that we might use in roles/tasks.
  }
}

output "sls-ips" {
  value = {
    for instance in aws_instance.sls :
    instance.public_ip => instance.private_ip
  }
}

output "sls-dns" {
  value = {
    for instance in aws_instance.sls :
    instance.public_dns => instance.private_dns
  }
}

output "client" {
  value = {
    for instance in aws_instance.client :
    instance.public_ip => instance.private_ip
  }
}

output "client_ssh_host" {
  value = aws_instance.client.0.public_ip
}

