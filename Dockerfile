#
# Should be launched with the following:
#     docker run -d -v /home/core:/usr/src jyidiego/rax_controller
# The credential files:
#     .go-rax_creds:
#         RS_AUTH_URL=https://identity.api.rackspacecloud.com/v2.0/
#         RS_USERNAME=clouddemo
#         RS_API_KEY=**************
#     .rax_creds:
#         [rackspace_cloud]
#         username = clouddemo
#         api_key = ***************
# 
# Where the directory /home/core holds these two credential files.
FROM python:2

RUN pip install ipython python-etcd docker-py pyrax
RUN apt-get update
RUN apt-get install -y docker.io
RUN git clone https://github.com/jyidiego/rax_controller.git
WORKDIR /rax_controller
CMD ["python", "/rax_controller/controller.py"]
