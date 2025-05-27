# Cloud deployment instructions

## Dependencies

- Install Terraform: https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
- Install Ansible: https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html#installing-and-upgrading-ansible-with-pip
- Install Ansible Galaxy Packages:
```
ansible-galaxy collection install cloud.terraform
ansible-galaxy role install geerlingguy.docker
```
- Install AWS CLI 2: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- Configure AWS CLI credentials: `aws configure`

## Preperation
1) Adjust variables in `terraform.tfvars`. E.g., ssh-key, region, ami, user etc
2) Adjust variables in `ansible.cfg`. E.g., private-key (should correspond to ssh-key in `terraform.tfvars`
3) Run `terraform init` & `terraform plan` 

## Deployment
1) `terraform apply`
2) `ansible-playbook --private-key ~/.ssh/aws_tziegler_us-east.pem --inventory terraform.yaml deploy.yaml`

## Installation
1) `rsync -azP --exclude=cloud_deployment --exclude=.git -e "ssh -i ~/.ssh/<private-key-file>" .. ubuntu@<publicip>:/home/ubuntu/leanstore`
2) ssh to machine and run `mkdir build && cd build`
3) `cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j all`

## Run
Adjust `db_path` to block-device if applicable:
`./benchmark/LeanStore_YCSB -worker_count=16 -ycsb_record_count=10000000 -ycsb_exec_seconds=5 -ycsb_read_ratio=50 -bm_virtual_gb=32 -bm_physical_gb=8 -db_path=/dev/nvme1n1 -wal_fsync=false -txn_debug=true -txn_commit_variant=2 -wal_log_backend=2 -wal_batch_write_kb=512`

## Deprovision
`terraform destroy`
