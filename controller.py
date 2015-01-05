#!/usr/bin/env python

import json
import multiprocessing
import Queue
import pyrax
import shlex
import threading
import time
import uuid

import urllib3
urllib3.disable_warnings()

from subprocess import Popen, PIPE
from docker import Client

# Docker formatted command line
# This is a hardcoded value for running cf_processor
# we'll be making this more generic
video_docker_cmd = '''docker run --rm --env-file .go-rax-creds
                    jyidiego/gorax_trans
                    /go/src/github.com/jyidiego/gorax_transcoder/cf_processor
                    -raw_video %s
                    -output_container %s
                    -input_container %s'''

# Not really used yet, leaving it around for the future
class DockWorker(object):
    def __init__(   self,
                    docker_image_name,
                    command,
                    environment,
                    base_url="unix://var/run/docker.sock",
                    version="1.15" ):
        self.dockerh = Client( base_url=base_url, version=version ) 
        self.command = None
        self.docker_img = None

    def run_image(  docker_image_name,
                    command,
                    environment ):
        self.command = command
        self.docker_img = docker_image_name


class RaxAuth(object):
    def __init__(   self,
                    credential_file=".rax_creds",
                    region="IAD",
                    auth_url="https://identity.api.rackspacecloud.com/v2.0/" ):
        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(credential_file)
        self.pyrax = pyrax
        self.region = region
        self.rax_user = pyrax.identity.username
        self.rax_apikey = pyrax.identity.api_key
        self.rax_auth_url=auth_url

    def gorax_env_dict(self):
        return {    "RS_USERNAME" : self.rax_user,
                    "RS_API_KEY" : self.rax_apikey,
                    "RS_AUTH_URL" : self.rax_auth_url  }

class RaxQueue(threading.Thread):

    def __init__(   self,
                    rax_auth,
                    rax_msg_ttl=300,
                    rax_msg_grace=60,
                    rax_queue='transcode_demo',
                    time_to_wait=2,
                    debug=False ):

        super(RaxQueue, self).__init__()
        self.cq = rax_auth.pyrax.connect_to_queues(region=rax_auth.region)  # Demo to run out of IAD
        self.cq.client_id = str(uuid.uuid4())  # Randomly create a uuid client id

        if not self.cq.queue_exists(rax_queue):
            self.rax_queue = self.cq.create(rax_queue)
        else:
            for queue in self.cq.list():
                if queue.name == rax_queue:
                    self.rax_queue = queue
                    break
            # self.rax_queue = self.cq.list()
            # self.rax_queue = [ (i.name, i) for i in self.cq.list() if i.name == rax_queue ]
        self.local_queue = Queue.Queue(maxsize=multiprocessing.cpu_count())
        self.rax_msg_ttl = rax_msg_ttl
        self.rax_msg_grace = rax_msg_grace
        self.tt_wait = time_to_wait

    def run(self):
        ''' No Longer used. Decided this isn't a good idea
            Instead better to have the workers just grab tasks directly off the Rax
            Cloud Queue and have the controller manage initializing the workers
            rather than keeping a thread that puts messages on a local queue.
            Keeping the code around for reference. '''
        while True:
            try:
                # 1 message per iteration
                if not self.local_queue.full():
                    m = self.cq.claim_messages(self.rax_queue, self.rax_msg_ttl, self.rax_msg_ttl, 1)
                    if m:
                        self.local_queue.put(m)
                        print "Adding message id %s to the local queue" % m.id
                    else:
                        print "Rax Cloud Queue is empty...."
                        time.sleep(self.tt_wait)
                else:
                    # print "Local queue is full waiting for messages to process..."
                    time.sleep(self.tt_wait)
            except pyrax.exceptions.ClientException, e:
                print "Couldn't claim or delete message: %s" % e

    def task_done(self, rax_message):
        for i in rax_message.messages:
            i.delete(claim_id=i.claim_id)
        # No longer used just kept for future reference
        # self.local_queue.task_done()

    def get_task(self):
	while [ True ]:
            m = self.cq.claim_messages( self.rax_queue,
                                        self.rax_msg_ttl,
                                        self.rax_msg_grace,
                                        1 )
            if m:
                return m
            else:
                print "%s: Rax Cloud Queue is empty...waiting %s seconds." % (threading.current_thread().name, self.tt_wait)
                time.sleep(self.tt_wait)


def get_worker_count():
    # Convenient way to set worker count dynamically!
    return multiprocessing.cpu_count()

def process_video( item, rax_auth, logs_container ):
    # items will always have just one message
    item_dict = json.loads(item.messages[0].body)

    video_cmd = video_docker_cmd % ( item_dict['videofile'],
                                     item_dict['output-container'],
                                     item_dict['input-container'] )
    pargs = shlex.split( video_cmd )
    p = Popen( pargs, stdout=PIPE, stderr=PIPE )
    p.wait()
    stdoutdata, stderrdata = p.communicate()
    cf = rax_auth.pyrax.connect_to_cloudfiles(region=rax_auth.region)
    container = cf.get_container( logs_container )

    kwargs={ "obj_name" : item_dict['videofile'] + ".stdout.log",
             "data" : stdoutdata }
    t_stdout = threading.Thread(target=container.create, kwargs=kwargs)
    t_stdout.start()

    kwargs={ "obj_name" : item_dict['videofile'] + ".stderr.log",
             "data" : stderrdata }
    t_stderr = threading.Thread(target=container.create, kwargs=kwargs)
    t_stderr.start()

    # wait for the threads to finish uploading logs
    t_stdout.join()
    t_stderr.join()

def worker( rax_queue, rax_auth, container_logs ):
    while True:
        item = rax_queue.get_task()
        process_video(item, rax_auth, container_logs)
        rax_queue.task_done(item)
        update_job_status(item)

def update_job_status( rax_queue_message):
    print "Job %s is Done!" % rax_queue_message

def init_worker_pool( rax_queue, rax_auth, container_logs ): 
    args=( rax_queue, rax_auth, container_logs )
    for i in range( get_worker_count() ):
        t = threading.Thread(target=worker, args=args)
        t.daemon = True
        t.start()

def main():
    pass

if __name__ == '__main__':
    pass
