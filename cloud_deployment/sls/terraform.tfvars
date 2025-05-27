key_name        = "leanstore_us-east"
public_key_path = "~/.ssh/id_ed25519.pub"
region          = "us-east-2"
az              = "us-east-2a"
ami             = "ami-012e6364f6bd17628"
user            = "ubuntu"
spot            = false

instance_types = {
  "sls"        = "i4i.4xlarge"
  "client"     = "i4i.4xlarge"
}

num_instances = {
  "client"     = 1
  "sls"        = 1
}
