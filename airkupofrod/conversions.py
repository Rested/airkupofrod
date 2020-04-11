from typing import List, Tuple, Dict

from airflow.contrib.kubernetes.pod import Port, Resources
from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from kubernetes.client import (
    V1PodSpec,
    V1Container,
    V1EnvVar,
    V1EnvVarSource,
    V1SecretKeySelector,
    V1ContainerPort,
    V1VolumeMount,
    V1Volume,
    V1Affinity,
    V1ResourceRequirements,
    V1Toleration,
    V1ConfigMapKeySelector,
    V1ObjectFieldSelector,
    V1PodSecurityContext,
)


def handle_container_environment_variables(
    env_vars: List[V1EnvVar],
) -> Tuple[Dict[str, str], List[Secret], List[str], List[PodRuntimeInfoEnv]]:
    secrets = []
    plain_env_vars = {}
    config_maps = []
    runtime_env_vars = []
    for env_var in env_vars:
        value_from: V1EnvVarSource = env_var.value_from
        if value_from:
            if value_from.resource_field_ref:
                # not handled for now
                continue
            if value_from.field_ref:
                field_ref: V1ObjectFieldSelector = value_from.field_ref
                runtime_env_vars.append(
                    PodRuntimeInfoEnv(
                        field_path=field_ref.field_path, name=env_var.name
                    )
                )
            if value_from.config_map_key_ref:
                key_ref: V1ConfigMapKeySelector = value_from.config_map_key_ref
                config_maps.append(key_ref.name)

            if value_from.secret_key_ref:
                key_ref: V1SecretKeySelector = value_from.secret_key_ref
                secrets.append(
                    Secret(
                        deploy_type="env",
                        deploy_target=env_var.name,
                        secret=key_ref.name,
                        key=key_ref.key,
                    )
                )

        plain_env_vars[env_var.name] = env_var.value

    return plain_env_vars, secrets, config_maps, runtime_env_vars


def convert_security_context(pod_spec: V1PodSpec):
    security_context: V1PodSecurityContext = pod_spec.security_context
    return security_context.to_dict()


def convert_ports(container: V1Container) -> List[Port]:
    ports: List[V1ContainerPort] = container.ports
    return [Port(name=port.name, container_port=port.container_port) for port in ports]


def convert_volume_mounts(container: V1Container) -> List[VolumeMount]:
    volume_mounts: List[V1VolumeMount] = container.volume_mounts
    return [
        VolumeMount(
            name=vm.name,
            mount_path=vm.mount_path,
            sub_path=vm.sub_path,
            read_only=vm.read_only,
        )
        for vm in volume_mounts
    ]


def convert_volumes(pod_spec: V1PodSpec) -> List[Volume]:
    volumes: List[V1Volume] = pod_spec.volumes
    return [Volume(name=volume.name, configs=volume.to_dict()) for volume in volumes]


def convert_affinity(pod_spec: V1PodSpec) -> Dict:
    affinity: V1Affinity = pod_spec.affinity
    return affinity.to_dict()


def convert_resources(container: V1Container) -> Resources:
    resources: V1ResourceRequirements = container.resources
    return Resources(
        request_memory=resources.requests.get("memory"),
        request_cpu=resources.requests.get("cpu"),
        limit_memory=resources.limits.get("memory"),
        limit_cpu=resources.limits.get("cpu"),
        limit_gpu=resources.limits.get("gpu"),
    )


def convert_tolerations(pod_spec: V1PodSpec) -> List[Dict]:
    tolerations: List[V1Toleration] = pod_spec.tolerations
    return [toleration.to_dict() for toleration in tolerations]


