import warnings

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults
from kubernetes.client import V1PodSpec, V1Container, V1ObjectMeta

from airkupofrod.collect import get_pod_template_from_deployment_labels_and_namespace
from airkupofrod.conversions import (
    handle_container_environment_variables,
    convert_ports,
    convert_volume_mounts,
    convert_volumes,
    convert_affinity,
    convert_resources,
    convert_tolerations,
    convert_security_context,
)


class KubernetesPodOperatorFromDeployment(KubernetesPodOperator):
    """
       Execute a task in a Kubernetes Pod
       :param deployment_labels: A dict of labels to lookup the deployment to use as a pod template.
       :type deployment_labels: dict
       :param deployment_fields: A dict of fields to lookup the deployment to use as a pod template.
       :type deployment_fields: dict
       :param deployment_namespace: the namespace in which to search for the deployment,
            defaults to the value of the namespace parameter.
       :type deployment_namespace: str
       :param image: Docker image you wish to launch. Defaults to hub.docker.com,
           but fully qualified URLS will point to custom repositories.
       :type image: str
       :param namespace: the namespace to run within kubernetes.
       :type namespace: str
       :param cmds: entrypoint of the container. (templated)
           The docker images's entrypoint is used if this is not provided.
       :type cmds: list[str]
       :param arguments: arguments of the entrypoint. (templated)
           The docker image's CMD is used if this is not provided.
       :type arguments: list[str]
       :param image_pull_policy: Specify a policy to cache or always pull an image.
       :type image_pull_policy: str
       :param image_pull_secrets: Any image pull secrets to be given to the pod.
                                  If more than one secret is required, provide a
                                  comma separated list: secret_a,secret_b
       :type image_pull_secrets: str
       :param ports: ports for launched pod.
       :type ports: list[airflow.contrib.kubernetes.pod.Port]
       :param volume_mounts: volumeMounts for launched pod.
       :type volume_mounts: list[airflow.contrib.kubernetes.volume_mount.VolumeMount]
       :param volumes: volumes for launched pod. Includes ConfigMaps and PersistentVolumes.
       :type volumes: list[airflow.contrib.kubernetes.volume.Volume]
       :param labels: labels to apply to the Pod.
       :type labels: dict
       :param startup_timeout_seconds: timeout in seconds to startup the pod.
       :type startup_timeout_seconds: int
       :param name: name of the pod in which the task will run, will be used to
           generate a pod id (DNS-1123 subdomain, containing only [a-z0-9.-]).
       :type name: str
       :param env_vars: Environment variables initialized in the container. (templated)
       :type env_vars: dict
       :param secrets: Kubernetes secrets to inject in the container.
           They can be exposed as environment vars or files in a volume
       :type secrets: list[airflow.contrib.kubernetes.secret.Secret]
       :param in_cluster: run kubernetes client with in_cluster configuration.
       :type in_cluster: bool
       :param cluster_context: context that points to kubernetes cluster.
           Ignored when in_cluster is True. If None, current-context is used.
       :type cluster_context: str
       :param get_logs: get the stdout of the container as logs of the tasks.
       :type get_logs: bool
       :param annotations: non-identifying metadata you can attach to the Pod.
                           Can be a large range of data, and can include characters
                           that are not permitted by labels.
       :type annotations: dict
       :param resources: A dict containing resources requests and limits.
           Possible keys are request_memory, request_cpu, limit_memory, limit_cpu,
           and limit_gpu, which will be used to generate airflow.kubernetes.pod.Resources.
           See also kubernetes.io/docs/concepts/configuration/manage-compute-resources-container
       :type resources: dict
       :param affinity: A dict containing a group of affinity scheduling rules.
       :type affinity: dict
       :param node_selectors: A dict containing a group of scheduling rules.
       :type node_selectors: dict
       :param config_file: The path to the Kubernetes config file. (templated)
       :type config_file: str
       :param do_xcom_push: If do_xcom_push is True, the content of the file
           /airflow/xcom/return.json in the container will also be pushed to an
           XCom when the container completes.
       :type do_xcom_push: bool
       :param is_delete_operator_pod: What to do when the pod reaches its final
           state, or the execution is interrupted.
           If False (default): do nothing, If True: delete the pod
       :type is_delete_operator_pod: bool
       :param hostnetwork: If True enable host networking on the pod.
       :type hostnetwork: bool
       :param tolerations: A list of kubernetes tolerations.
       :type tolerations: list tolerations
       :param configmaps: A list of configmap names objects that we
           want mount as env variables.
       :type configmaps: list[str]
       :param pod_runtime_info_envs: environment variables about
                                     pod runtime information (ip, namespace, nodeName, podName).
       :type pod_runtime_info_envs: list[PodRuntimeEnv]
       :param security_context: security options the pod should run with (PodSecurityContext).
       :type security_context: dict
       :param dnspolicy: dnspolicy for the pod.
       :type dnspolicy: str
   """

    @apply_defaults
    def __init__(
        self,  # pylint: disable=too-many-arguments,too-many-locals
        namespace,
        deployment_labels=None,
        deployment_fields=None,
        deployment_namespace=None,
        image=None,
        name=None,
        cmds=None,
        arguments=None,
        ports=None,
        volume_mounts=None,
        volumes=None,
        env_vars=None,
        secrets=None,
        in_cluster=None,
        cluster_context=None,
        labels=None,
        startup_timeout_seconds=120,
        get_logs=True,
        image_pull_policy=None,
        annotations=None,
        resources=None,
        affinity=None,
        config_file=None,
        node_selectors=None,
        image_pull_secrets=None,
        service_account_name="default",
        is_delete_operator_pod=False,
        hostnetwork=None,
        tolerations=None,
        configmaps=None,
        security_context=None,
        pod_runtime_info_envs=None,
        dnspolicy=None,
        do_xcom_push=False,
        *args,
        **kwargs
    ):
        # https://github.com/apache/airflow/blob/2d0eff4ee4fafcf8c7978ac287a8fb968e56605f/UPDATING.md#unification-of-do_xcom_push-flag
        if kwargs.get("xcom_push") is not None:
            kwargs["do_xcom_push"] = kwargs.pop("xcom_push")
            warnings.warn(
                "`xcom_push` will be deprecated. Use `do_xcom_push` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super(KubernetesPodOperator, self).__init__(*args, resources=None, **kwargs)
        self.do_xcom_push = do_xcom_push

        (
            pod_template,
            deployment,
        ) = get_pod_template_from_deployment_labels_and_namespace(
            namespace=deployment_namespace or namespace,
            config_file=config_file,
            cluster_context=cluster_context,
            in_cluster=in_cluster,
            fields=deployment_fields,
            labels=deployment_labels,
        )
        pod_spec: V1PodSpec = pod_template.spec
        container: V1Container = pod_spec.containers[0]
        metadata: V1ObjectMeta = pod_template.metadata

        (
            plain_env_vars,
            container_secrets,
            container_config_maps,
            runtime_info_envs,
        ) = handle_container_environment_variables(container.env)

        self.image = image or container.image
        self.namespace = namespace
        self.cmds = cmds or container.command
        self.arguments = arguments or container.args
        self.labels = labels or metadata.labels
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = self._set_name(name or deployment.metadata.name)

        self.env_vars = env_vars or plain_env_vars
        self.ports = ports or convert_ports(container)
        self.volume_mounts = volume_mounts or convert_volume_mounts(container)
        self.volumes = volumes or convert_volumes(pod_spec)
        self.secrets = secrets or secrets
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.get_logs = get_logs
        self.image_pull_policy = (
            image_pull_policy or container.image_pull_policy or "IfNotPresent"
        )
        self.node_selectors = node_selectors or pod_spec.node_selector
        self.annotations = annotations or metadata.annotations
        self.affinity = affinity or convert_affinity(pod_spec)
        self.resources = self._set_resources(resources or convert_resources(container))
        self.config_file = config_file
        self.image_pull_secrets = image_pull_secrets or pod_spec.image_pull_secrets
        self.service_account_name = (
            service_account_name or pod_spec.service_account_name
        )
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = (
            pod_spec.host_network or False if hostnetwork is None else hostnetwork
        )
        self.tolerations = tolerations or convert_tolerations(pod_spec)
        self.configmaps = configmaps or container_config_maps
        self.security_context = security_context or convert_security_context(pod_spec)
        self.pod_runtime_info_envs = pod_runtime_info_envs or runtime_info_envs
        self.dnspolicy = dnspolicy or pod_spec.dns_policy
