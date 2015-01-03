#!/usr/bin/env python

import docker
import json
import multiprocessing
import Queue
import pyrax
import threading
import time
import uuid


class RaxQueue(threading.Thread):

    def __init__(   self,
                    credential_file=".rax_creds",
                    rax_msg_ttl=300,
                    rax_queue='transcode_demo',
                    region="IAD",
                    time_to_wait=5,
                    debug=False ):

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(credential_file)
        super(RaxQueue, self).__init__()
        self.cq = pyrax.connect_to_queues(region=region)  # Demo to run out of IAD
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
        self.region = region
        self.local_queue = Queue.Queue(maxsize=multiprocessing.cpu_count())
        self.rax_msg_ttl = rax_msg_ttl
        self.tt_wait = time_to_wait

    def run(self):
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
        self.local_queue.task_done()

    def get_task(self):
        return self.local_queue.get()


def get_worker_count():
    # Convenient way to set worker count dynamically!
    return multiprocessing.cpu_count()

def process_video( item, docker_image_name ):
    # items will always have just one message
    item_dict = json.loads(item.messages[0].body)
    input_container = item_dict['input-container']
    output_container = item_dict['output-container']
    media_file = item_dict['videofile']

def worker( queue_object ):
    while True:
        item = queue_object.get_task()
        process_video(item)
        queue_object.task_done(item)
        update_job_status(item)

def update_job_status():
    print "Job %s is Done!"

def init_worker_pool():
    for i in range( get_worker_count() ):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

def get_tasks():
    # This fuction pulls tasks of Rackspace Cloud Queues
    pass

def main():
    pass

if __name__ == '__main__':
    pass
