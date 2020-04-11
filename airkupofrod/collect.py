from typing import Optional, Dict, Tuple, List

from kubernetes import config
from kubernetes.client import (
    V1DeploymentSpec,
    V1PodTemplateSpec,
    V1PodSpec,
    V1Container,
    V1Deployment,
    AppsV1Api,
)

from airkupofrod.exception import AirKuPOFroDError


def _validate_pod_template_spec(pod_template: V1PodTemplateSpec):
    pod_spec: V1PodSpec = pod_template.spec
    containers: List[V1Container] = pod_spec.containers

    if len(containers) != 1:
        raise AirKuPOFroDError(
            f"Pod template may only have 1 container, it has {len(containers)}"
        )


def _get_pod_template_from_deployment(deployment: V1Deployment,) -> V1PodTemplateSpec:
    spec: V1DeploymentSpec = deployment.spec
    pod_template: V1PodTemplateSpec = spec.template
    _validate_pod_template_spec(pod_template)
    return pod_template


def get_pod_template_from_deployment_labels_and_namespace(
    namespace: str,
    config_file: Optional[str] = None,
    cluster_context: Optional[str] = None,
    in_cluster: Optional[bool] = None,
    labels: Optional[Dict[str, str]] = None,
    fields: Optional[Dict[str, str]] = None,
) -> Tuple[V1PodTemplateSpec, V1Deployment]:
    if not (fields or labels):
        raise AirKuPOFroDError(
            "One of fields (dict) or labels (dict) must be specified to find the desired deployment"
        )
    if in_cluster:
        config.load_incluster_config()
    else:
        config.load_kube_config(config_file, cluster_context)
    apps_v1 = AppsV1Api()

    matching_deployments = apps_v1.list_namespaced_deployment(
        namespace=namespace, labels=labels, fields=fields
    ).items()

    if len(matching_deployments) > 1:
        raise AirKuPOFroDError("Multiple matching deployments found")

    try:
        deployment = next(matching_deployments)
    except StopIteration:
        raise AirKuPOFroDError("No matching deployment found")

    return _get_pod_template_from_deployment(deployment), deployment
