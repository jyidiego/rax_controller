#!/usr/bin/env python

import json
import multiprocessing
import Queue
import pyrax
import shlex
import sys
import threading
import time
import uuid

import urllib3
urllib3.disable_warnings()

from subprocess import Popen, PIPE
from docker import Client

from requests.adapters import ConnectionError
from requests.adapters import SSLError

# Docker formatted command line
# This is a hardcoded value for running cf_processor
# we'll be making this more generic
# video_docker_cmd = '''docker run --rm --env-file /usr/src/.go-rax-creds
docker_image = 'jyidiego/gorax_trans'
cmd = '/go/src/github.com/jyidiego/gorax_transcoder/cf_processor -raw_video %s -output_container %s -input_container %s'


class DockWorker(object):
    def __init__(   self,
                    base_url="unix://var/run/docker.sock",
                    version="1.15" ):
        self.dockerh = Client( base_url=base_url, version=version ) 

    def is_container_running(self, container_id):
        return self.dockerh.inspect_container(container_id)['State']['Running']

    def run_container_from_image( self,
                                  docker_image,
                                  command_line,
                                  environment=None,
                                  volumes=[] ):
        cont_dict = self.dockerh.create_container( image=docker_image,
                                                   command=command_line,
                                                   environment=environment,
                                                   volumes=volumes )
        self.dockerh.start(cont_dict['Id'])
        return cont_dict

    def wait(self, container_id ):
        return self.dockerh.wait( container_id )

    def logs(self, container_id ):
        return self.dockerh.logs( container_id )

    def remove_container(self, container_id):
        return self.dockerh.remove_container( container_id )


class RaxAuth(object):
    def __init__(   self,
                    credential_file="/usr/src/.rax_creds",
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
                    rax_msg_ttl=1200,
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

    def release_task(self, rax_message):
        self.rax_queue.release_claim( rax_message )

    def refresh_claim_ttl(self, rax_message, ttl):
        rax_message.reload()
        if ( rax_message.ttl - rax_message.age ) < ( self.tt_wait + 5 ):
            self.rax_queue.update_claim( rax_message, ttl=ttl, grace=ttl )
            return True
        else:
            return False

    def get_task(self):
	while True:
            try:
                m = self.rax_queue.claim_messages( ttl=self.rax_msg_ttl,
                                                   grace=self.rax_msg_grace,
                                                   count=1 )
            except SSLError,e:
                print "SSLError: %s" % e
                continue
            except ConnectionError, e:
                print "ConnectionError: %s" % e
                continue
            except:
                e = sys.exc_info()[0]
                print "Unknown Error: %s" % e
                continue
            if m:
                return m
            else:
                # print "%s: Rax Cloud Queue is empty...waiting %s seconds." % (threading.current_thread().name, self.tt_wait)
                time.sleep(self.tt_wait)

def cpu_map( x ):
    return { 1 : 1, 
             2 : 1,
             3 : 1 }.get(x, x/2)

def get_worker_count():
    # Convenient way to set worker count dynamically!
    return cpu_map( multiprocessing.cpu_count() )

def message_refresh( item, message_list ):
    item.reload()
    for msg in message_list:
        msg.reload()
    item.messages = message_list
    return ( item, message_list )

def monitor_container( event, rax_queue, item, container_id ):
    d = DockWorker()
    item, msg_list = message_refresh( item, item.messages )
    while not event.is_set():
        try:
            if rax_queue.refresh_claim_ttl( item, rax_queue.rax_msg_ttl ):
                item, msg_list = message_refresh( item, msg_list )
                print "REFRESHING Claim: %s" % item 
            else:
                item, msg_list = message_refresh( item, msg_list )
                print "NOOP Claim: %s" % item 

            for msg in msg_list:
                msg.reload()
                if msg.age > msg.ttl:
                    # If we make it here our docker container is probably hung
                    # and we should give up our claim
                    print "Message age exceeded ttl, giving up our claim: %s" % msg
                    print "Killing container: %s" % d.inspect_container(container_id) 
                    d.dockerh.kill( container_id )
                    return 1
        except:
            e = sys.exc_info()[0]
            print "MONITOR encountered an error: ", e
            print "Thread: ", threading.current_thread()
            print "Trying again...."
            continue
        time.sleep( rax_queue.tt_wait )
    return 0

def process_video( item, rax_auth, rax_queue, logs_container ):
    dock_worker = DockWorker()
    # items will always have just one message
    try:
        item_dict = json.loads(item.messages[0].body)
    except ValueError,e:
        print "json module was unable to convert to a python dict."
        print "ValueError: %s" % e
        return False
        

    video_cmd = cmd % ( item_dict['videofile'],
                        item_dict['output-container'],
                        item_dict['input-container'] )

    container_dict = dock_worker.run_container_from_image( docker_image,
                                                           video_cmd,
                                                           rax_auth.gorax_env_dict() )

    # Not going to use subprocess.Popen
    # pargs = shlex.split( video_cmd )
    # p = Popen( pargs, stdout=PIPE, stderr=PIPE )

    # Though i needed this but maybe not
    # c_id = p.stdout.readline().strip()
    e = threading.Event()
    m_thread = threading.Thread(target=monitor_container, args=(e, rax_queue, item, container_dict['Id'],))
    m_thread.start()

    # Not using subprocess.Popen
    # p.wait()
    ret_code = dock_worker.wait( container_dict['Id'] )


    # time for the monitoring thread to be terminated
    e.set()

    # brief wait while the worker thread catches up.
    # m_thread.join() probably not necessary
    # stdoutdata, stderrdata = p.communicate()
    stdout_stderr = dock_worker.logs( container_dict['Id'] )
    cf = rax_auth.pyrax.connect_to_cloudfiles(region=rax_auth.region)
    container = cf.get_container( logs_container )

    # Simple and straight forward wrap with a try/exception
    try:
        stdout_stderr_blob = container.store_object( obj_name=item_dict['videofile'] + ".stdout.log",
                                              data=stdout_stderr )
        print "Stored stdout:", stdout_stderr_blob
        #stderr_blob = container.store_object( obj_name=item_dict['videofile'] + ".stderr.log",
        #                                      data=stderrdata )
        #print "Stored stderr:", stderr_blob
    except:
        e = sys.exc_info()[0]
        print "Upload of stdout/stderr to cloud files failed!"
        print "Unknown Error: ", e
        print "Continuing on.."

    #####################################################################
    # De-complicate the upload
    #kwargs={ "obj_name" : item_dict['videofile'] + ".stdout.log",
    #         "data" : stdoutdata }
    #t_stdout = threading.Thread(target=container.create, kwargs=kwargs)
    #t_stdout.start()

    #kwargs={ "obj_name" : item_dict['videofile'] + ".stderr.log",
    #         "data" : stderrdata }
    #t_stderr = threading.Thread(target=container.create, kwargs=kwargs)
    #t_stderr.start()

    # wait for the threads to finish uploading logs
    #t_stdout.join()
    #t_stderr.join()
    #####################################################################


    # if p.returncode < 0:
    if ret_code != 0:
        print "Container had issues, leaving it alone for inspection: ", container_dict['Id']
        return False
    else:
        # Cleanup Container
        dock_worker.remove_container( container_dict['Id'] )
        return True

def worker( rax_queue, rax_auth, container_logs ):
    item = None
    messages = None
    while True:
        try:
            if not item: 
                item = rax_queue.get_task()
            # because reloading will cause the message attribute
            # list on item to disapper we need to save it
            # in another variable.
            # This first if is for the first time we enter the loop or
            # if an exception occured trying to get an item
            if item.messages:
                item, messages = message_refresh( item, item.messages )
            else:
                item, messages = message_refresh( item, messages )

            if process_video( item, rax_auth, rax_queue, container_logs):
                retry = True
                while retry:
                    try:
                        item, messages = message_refresh( item, messages )
                        rax_queue.task_done(item)
                        print "Job %s is Done!" % item
                        retry = False
                    except:
                        e = sys.exc_info()[0]
                        print "WORKER encountered an error marking task done: ", e
                        print "Thread: ", threading.current_thread()
                        print "Trying again...."
                        time.sleep(rax_queue.tt_wait)
                        continue
            else:
                retry = True
                while retry:
                    try:
                        item, messages = message_refresh( item, messages )
                        rax_queue.release_task(item) 
                        retry = False
                        print "Couldn't Process task:\n%sreleasing claim\n" % item
                    except:
                        e = sys.exc_info()[0]
                        print "WORKER encountered an error releasing task: ", e
                        print "Thread: ", threading.current_thread()
                        print "Trying again...."
                        continue
            item = None
            messages = None
        except:
            e = sys.exc_info()[0]
            print "WORKER encountered an error: ", e
            print "Thread: ", threading.current_thread()
            print "Trying again...."
            continue

def update_job_status( rax_queue_message):
    print "Job %s is Done!" % rax_queue_message

def init_worker_pool( rax_queue, rax_auth, container_logs, num_workers=get_worker_count() ): 
    worker_pool = [ ]
    args=( rax_queue, rax_auth, container_logs )
    for i in range( num_workers ):
        t = threading.Thread(target=worker, args=args)
        t.daemon = True
        t.start()
        worker_pool.append(t)
    return worker_pool

def main(poll=60, time_to_wait=2, jobs_container='video_jobs'):
    auth = RaxAuth()
    rax_queue = RaxQueue( rax_auth=auth, rax_msg_ttl=60, time_to_wait=time_to_wait)
    worker_pool = init_worker_pool( rax_queue, auth, jobs_container) 
    while True:
        time.sleep(poll)
        for w in worker_pool:
            if w.is_alive() and w.daemon:
                print "ALIVE + name: %s id: %s" % (w.name, w.ident) 
            elif w.daemon:
                print "DEAD - name: %s id: %s" % (w.name, w.ident) 

if __name__ == '__main__':
    main()
