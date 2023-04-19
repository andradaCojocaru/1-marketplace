"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import time
from threading import Thread


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        # get the current producer id
        self.producer_id = marketplace.register_producer()
        # make the thread daemon
        Thread.__init__(self, daemon=kwargs["daemon"], kwargs=kwargs)

    def run(self):
        while True:
            # for every product
            for product in self.products:
                quantity = product[1]
                # until it is no quantity to make
                while quantity != 0:
                    # if it is free space to make the product
                    if self.marketplace.publish(self.producer_id, product[0]):
                        time.sleep(product[2])
                        quantity -= 1
                    # no free space, wait
                    else:
                        time.sleep(self.republish_wait_time)
