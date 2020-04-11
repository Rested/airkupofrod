# Airflow Kubernetes Pod Operator from Deployment

Or `airkupofrod` for short, is a tiny package which does one thing - takes a deployment in your kubernetes cluster and 
turns its pod template into a `KubernetesPodOperator` object.


## Installation and usage

Ensure your airflow image has the python package `airkupofrod` installed

```bash
pip install airkubofrod
```

You will also need to make sure that a service account attached to your airflow pods
has the a role capable of listing deployments bound to it. See [role-binding](./role-binding) for an example of this.

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
skaffold dev --force=false --cleanup=false --status-check=false-port-forward
```

Then navigate to http://localhost:8080 and enable and trigger a run of the test deployments dag.

