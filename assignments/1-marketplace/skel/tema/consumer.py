"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        Thread.__init__(self, **kwargs)

    def run(self):
        # for every cart
        for cart in self.carts:
            # get the current cart id
            cart_id = self.marketplace.new_cart()

            # get operation to do in cart
            for operation in cart:
                match operation["type"]:
                    case "add":
                        # until we have quantity left
                        while operation["quantity"] > 0:
                            # if we can add to cart
                            if self.marketplace.add_to_cart(cart_id, operation["product"]):
                                operation["quantity"] -= 1
                            # wait until we can retry and maybe the product will be provided
                            else:
                                sleep(self.retry_wait_time)
                    case "remove":
                        # until we have quantity left
                        while operation["quantity"] > 0:
                            self.marketplace.remove_from_cart(cart_id, operation["product"])
                            operation["quantity"] -= 1

            # get the list of achieved products
            products = self.marketplace.place_order(cart_id)
            # display the required message
            for product in products:
                print(f"{self.name} bought {product[1]}")
