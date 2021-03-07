FROM ubuntu
ADD . .
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update -yqq
RUN apt install -yqq python3 python3-pip
RUN python3 setup.py develop
RUN pip3 install -q boto3 python-keystoneclient python-swiftclient azure-storage-blob oci google-cloud-storage ibm-cos-sdk

CMD os-benchmark
