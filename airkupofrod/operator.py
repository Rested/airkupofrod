import warnings

from airflow import AirflowException
from airflow.contrib.kubernetes import pod_launcher, pod_generator, kube_client
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
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
    convert_image_pull_secrets,
)
from airflow.version import version as airflow_version

from airkupofrod.request_factory import extract_env_and_secrets


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
            service_account_name=None,
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

        self.image = image
        self.namespace = namespace
        self.cmds = cmds
        self.arguments = arguments
        self.labels = labels
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = self._set_name(name or kwargs["task_id"])

        self.env_vars = env_vars
        self.ports = ports
        self.volume_mounts = volume_mounts
        self.volumes = volumes
        self.secrets = secrets
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors
        self.annotations = annotations
        self.affinity = affinity
        self.resources = self._set_resources(resources)
        self.config_file = config_file
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations
        self.configmaps = configmaps
        self.security_context = security_context
        self.pod_runtime_info_envs = pod_runtime_info_envs
        self.dnspolicy = dnspolicy

        self.deployment_namespace = deployment_namespace
        self.deployment_fields = deployment_fields
        self.deployment_labels = deployment_labels

    def execute(self, context):

        (
            pod_template,
            deployment,
        ) = get_pod_template_from_deployment_labels_and_namespace(
            namespace=self.deployment_namespace or self.namespace,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
            in_cluster=self.in_cluster,
            fields=self.deployment_fields,
            labels=self.deployment_labels,
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

        self.image = self.image or container.image
        self.cmds = self.cmds or container.command
        self.arguments = self.arguments or container.args or []
        self.labels = self.labels or metadata.labels or {}
        self.name = self._set_name(self.name or deployment.metadata.name)
        self.env_vars = self.env_vars or plain_env_vars
        self.ports = self.ports or convert_ports(container)
        self.volume_mounts = self.volume_mounts or convert_volume_mounts(container)
        self.volumes = self.volumes or convert_volumes(pod_spec)
        self.secrets = self.secrets or container_secrets
        self.image_pull_policy = (
                self.image_pull_policy or container.image_pull_policy or "IfNotPresent"
        )
        self.node_selectors = self.node_selectors or pod_spec.node_selector or {}
        self.annotations = self.annotations or metadata.annotations or {}
        self.affinity = self.affinity or convert_affinity(pod_spec)
        self.resources = (
            self.resources
            if (self.resources.has_limits() or self.resources.has_requests())
            else convert_resources(container)
        )
        self.image_pull_secrets = self.image_pull_secrets or convert_image_pull_secrets(
            pod_spec
        )
        self.service_account_name = (
                self.service_account_name
                or pod_spec.service_account_name
                or pod_spec.service_account
                or "default"
        )
        self.hostnetwork = (
            pod_spec.host_network or False
            if self.hostnetwork is None
            else self.hostnetwork
        )

        self.tolerations = self.tolerations or convert_tolerations(pod_spec)
        self.configmaps = self.configmaps or container_config_maps
        self.security_context = self.security_context or convert_security_context(
            pod_spec
        )
        self.pod_runtime_info_envs = self.pod_runtime_info_envs or runtime_info_envs
        self.dnspolicy = self.dnspolicy or pod_spec.dns_policy

        self.log.info("volumes %s", self.volumes)

        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(in_cluster=self.in_cluster,
                                                     cluster_context=self.cluster_context,
                                                     config_file=self.config_file)
            else:
                client = kube_client.get_kube_client(cluster_context=self.cluster_context,
                                                     config_file=self.config_file)

            # Add Airflow Version to the label
            # And a label to identify that pod is launched by KubernetesPodOperator
            self.labels.update(
                {
                    'airflow_version': airflow_version.replace('+', '-'),
                    'kubernetes_pod_operator': 'True',
                }
            )

            gen = pod_generator.PodGenerator()

            for port in self.ports:
                gen.add_port(port)
            for mount in self.volume_mounts:
                gen.add_mount(mount)
            for volume in self.volumes:
                gen.add_volume(volume)

            pod = gen.make_pod(
                namespace=self.namespace,
                image=self.image,
                pod_id=self.name,
                cmds=self.cmds,
                arguments=self.arguments,
                labels=self.labels,
            )

            pod.service_account_name = self.service_account_name
            pod.secrets = self.secrets
            pod.envs = self.env_vars
            pod.image_pull_policy = self.image_pull_policy
            pod.image_pull_secrets = self.image_pull_secrets
            pod.annotations = self.annotations
            pod.resources = self.resources
            pod.affinity = self.affinity
            pod.node_selectors = self.node_selectors
            pod.hostnetwork = self.hostnetwork
            pod.tolerations = self.tolerations
            pod.configmaps = self.configmaps
            pod.security_context = self.security_context
            pod.pod_runtime_info_envs = self.pod_runtime_info_envs
            pod.dnspolicy = self.dnspolicy

            launcher = pod_launcher.PodLauncher(kube_client=client,
                                                extract_xcom=self.do_xcom_push)
            # monkey patch to avoid https://github.com/apache/airflow/issues/8275
            launcher.kube_req_factory.extract_env_and_secrets = extract_env_and_secrets
            try:
                (final_state, result) = launcher.run_pod(
                    pod,
                    startup_timeout=self.startup_timeout_seconds,
                    get_logs=self.get_logs)
            finally:
                if self.is_delete_operator_pod:
                    launcher.delete_pod(pod)

            if final_state != State.SUCCESS:
                raise AirflowException(
                    'Pod returned a failure: {state}'.format(state=final_state)
                )
            if self.do_xcom_push:
                return result
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))
