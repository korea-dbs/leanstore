# Cloud deployment

## Steps

```
cd sls
terraform init
terraform plan
terraform apply
ansible-playbook --user ubuntu --inventory terraform.yaml deploy.yaml
```