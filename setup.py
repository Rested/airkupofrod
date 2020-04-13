import setuptools
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airkupofrod",  # Replace with your own username
    version=versioneer.get_version(),
    author="Reuben Thomas-Davis",
    author_email="reuben@rekon.uk",
    description="Takes a deployment in your kubernetes cluster and turns its pod template into a KubernetesPodOperator "
                "object.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rekon-oss/airkupofrod",
    packages=setuptools.find_packages(exclude=["tests", "helm-charts", "airflow-image"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        'apache-airflow[kubernetes]>=1.10',
    ],
    cmdclass=versioneer.get_cmdclass()
)
