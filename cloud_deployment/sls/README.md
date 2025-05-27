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
2) Adjust variables in `ansible.cfg`. E.g., private-key (should correspond to the ssh-key in `terraform.tfvars`
3) Run `terraform init` & `terraform plan` 

## Deployment
1) `terraform apply`
2) `ansible-playbook --inventory terraform.yaml deploy.yaml`


## Run
1) Use ssh command printed by ansible to login
2) Adjust "Start server with" command to start server on sls-nodes

## Deprovision
`terraform destroy`
