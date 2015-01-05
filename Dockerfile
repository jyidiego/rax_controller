FROM python:2

RUN pip install ipython python-etcd docker-py pyrax
RUN apt-get update
RUN apt-get install -y docker.io
