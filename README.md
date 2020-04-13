# Airflow KubernetesPodOperatorFromDeployment

[![PyPI version](https://badge.fury.io/py/airkupofrod.svg)](https://badge.fury.io/py/airkupofrod)
[![PyPI license](https://img.shields.io/pypi/l/airkupofrod.svg)](https://pypi.python.org/pypi/airkupofrod/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/airkupofrod.svg)](https://pypi.python.org/pypi/airkupofrod/)

Or `airkupofrod` for short, is a tiny package which does one thing - takes a deployment in your kubernetes cluster and 
turns allows you to use its pod template as a `KubernetesPodOperator` object. It does this by providing the 
`KubernetesPodOperatorFromDeployment` operator.

`airkupofrod` supports 1.10.9<=airflow<2


## Installation and usage

Ensure your airflow image has the python package `airkupofrod` installed

```bash
pip install airkubofrod
```

Then in your dags:
```python
from airkupofrod.operator import KubernetesPodOperatorFromDeployment

my_kupofrod_task = KubernetesPodOperatorFromDeployment(
    deployment_labels={"app": "my-app"}, # deployment labels to lookup by
    deployment_fields={"metadata.name": "my-app-deploy-template"}, # deployment fields to lookup by
    deployment_namespace="some-ns", # where the deployment lives
    namespace="default", # where the pod will be deployed
    task_id="my-kupofrod-task", 
    dag=dag,
    in_cluster=True, 
) 
```

You will also need to make sure that a service account attached to your airflow pods
has the a role capable of listing deployments bound to it. See 
[role-binding](https://github.com/rekon-oss/airkupofrod/tree/master/role-binding) for an example of this.

This is in addition to the role bindings necessary for the `KubernetesPodOperator` to work which can be seen in the 
[airflow helm chart](https://github.com/helm/charts/blob/master/stable/airflow/templates/role.yaml) 

## Developing

[Skaffold](https://skaffold.dev/) is used to test and develop inside kubernetes.

After ensuring you have:
 * Skaffold
 * Helm
 * Some type of k8s cluster available

Run:
```bash
skaffold dev --force=false --cleanup=false --status-check=false --port-forward
```

Then navigate to http://localhost:8080 and enable and trigger a run of the test deployments dag.

