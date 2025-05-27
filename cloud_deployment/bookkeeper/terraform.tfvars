key_name        = "leanstore_us-east"
public_key_path = "~/.ssh/id_ed25519.pub"
region          = "us-east-2"
az              = "us-east-2a"
ami             = "ami-012e6364f6bd17628"
user            = "ubuntu"
spot            = false

instance_types = {
  "pulsar"     = "i4i.8xlarge"
  "zookeeper"  = "i4i.8xlarge"
  "client"     = "i4i.8xlarge"
  "prometheus" = "t2.medium"
}

num_instances = {
  "client"     = 1
  "pulsar"     = 3
  "zookeeper"  = 3
  "prometheus" = 1
}
