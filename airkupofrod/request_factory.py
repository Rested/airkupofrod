from airflow.contrib.kubernetes.kubernetes_request_factory.kubernetes_request_factory import KubernetesRequestFactory


def extract_env_and_secrets(pod, req):
    """Overrides KubernetesRequestFactory to support variable references of secrets
    see https://github.com/apache/airflow/issues/8275"""
    envs_from_key_secrets = [
        env for env in pod.secrets if env.deploy_type == 'env' and env.key is not None
    ]

    if len(pod.envs) > 0 or len(envs_from_key_secrets) > 0 or len(pod.pod_runtime_info_envs) > 0:
        env = []
        for runtime_info in pod.pod_runtime_info_envs:
            KubernetesRequestFactory.add_runtime_info_env(env, runtime_info)
        for secret in envs_from_key_secrets:
            KubernetesRequestFactory.add_secret_to_env(env, secret)
        for k in pod.envs.keys():
            env.append({'name': k, 'value': pod.envs[k]})


        req['spec']['containers'][0]['env'] = env

    KubernetesRequestFactory._apply_env_from(pod, req)