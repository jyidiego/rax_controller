#!/usr/bin/env python

import docker
import etcd
import multiprocessing
import Queue
import pyrax
import threading


class RaxQueue(threading.Thread):

    def __init__(   self,
                    credential_file=".rax_creds",
                    rax_msg_ttl=300,
                    rax_queue='transcode_demo',
                    region="IAD",
                    debug=False ):

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(credential_file)
        self.cq = pyrax.connect_to_queues(region=region)  # Demo to run out of IAD
        self.cq.client_id = str(uuid.uuid4())  # Randomly create a uuid client id

        if not self.cq.queue_exists(queue_name):
            self.cq.create(queue_name)
            self.queue_name = queue_name
            self.region = region
        self.local_queue = Queue(maxsize=multiprocessing.cpu_count())
        self.rax_msg_ttl = rax_msg_ttl

    def run(self):
        while True:
            try:
                # take default 300 ttl and grace, 1 message per iter
                m = self.cq.claim_messages(self.queue_name, self.rax_msg_ttl, self.rax_msg_ttl, 1)
                if m:
                    print "Adding message id %s to the local queue" % m.id
                    self.local_queue.put(m)
                else:
                    print "No messages to process..."
                    time.sleep(5)
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

def process_video( item ):
    input_container = item.input_container
    output_container = item.output_container
    media_file = item.media_file

def worker( queue_object ):
    while True:
        item = queue_object.get_task()
        process_video(item)
        queue_object.task_done()

def init_worker_pool():
    for i in range( get_worker_count() )
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
