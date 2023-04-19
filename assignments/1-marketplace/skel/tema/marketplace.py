"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import logging
import time
import unittest
from logging.handlers import RotatingFileHandler
from threading import Lock
from skel.tema.product import Coffee, Tea


class TestMarketplace(unittest.TestCase):
    """
    Class that represents the unitTest for Marketplace.
    """

    def setUp(self):
        """Initialization for main attributes of class"""
        self.queue_size_per_producer = 15
        self.marketplace = Marketplace(self.queue_size_per_producer)

    def test_register_producer(self):
        """
        Function that represents the unitTest for register_producer.
        """
        self.assertEquals(self.marketplace.register_producer(), 1, "Expected to return id 1")

    def test_register_multiple_producer(self):
        """
        Function that represents the unitTest for register_producer.
        """
        self.assertEquals(self.marketplace.register_producer(), 1, "Expected to return id 1")
        self.assertEquals(self.marketplace.register_producer(), 2, "Expected to return id 2")

    def test_publish(self):
        """
        Function that represents the unitTest for register_p.
        """
        producer_id = self.marketplace.register_producer()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.assertTrue(self.marketplace.publish(producer_id, product), "Expected to return True")

    def test_multiple_publish(self):
        """
        Function that represents the unitTest for register_p.
        """
        producer_id = self.marketplace.register_producer()
        product1 = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        product2 = Tea(name="Linden", type="Herbal", price=9)
        self.assertTrue(self.marketplace.publish(producer_id, product1), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product2), "Expected to return True")

    def test_new_cart(self):
        self.assertEquals(self.marketplace.new_cart(), 1, "Expected to return id 1")

    def test_multiple_new_cart(self):
        self.assertEquals(self.marketplace.new_cart(), 1, "Expected to return id 1")
        self.assertEquals(self.marketplace.new_cart(), 2, "Expected to return id 2")

    def test_add_to_cart(self):
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product), "Expected to return True")

    def test_remove_from_cart(self):
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.marketplace.add_to_cart(cart_id, product)
        self.assertTrue(self.marketplace.remove_from_cart(cart_id, product), "Expected to return True")

    def test_place_order(self):
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.marketplace.add_to_cart(cart_id, product)
        self.assertEquals(self.marketplace.place_order(cart_id)[0], ('1', Coffee(name='Indonezia', price=1, acidity='5.05', roast_level='MEDIUM')), "Expected to return Tea")


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.products_left_per_producer = {}
        self.products_every_producer = {}
        self.carts = {}
        self.lock_remove_from_cart = Lock()
        self.lock_add_to_cart = Lock()
        self.lock_register_producer = Lock()
        self.lock_register_cart = Lock()
        self.lock_publish = Lock()
        self.number_producers = 0
        self.number_consumers = 0

        log_formatter = logging.Formatter("%(asctime)s:%(levelname)s: " "%(message)s")
        logging.Formatter.converter = time.gmtime
        logger = logging.getLogger()
        handler = RotatingFileHandler('my_logger', maxBytes=4096, backupCount=1)
        handler.setFormatter(log_formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        self.logger = logger

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        self.logger.info("Entered register_producer()")
        with self.lock_register_producer:
            self.number_producers += 1
            producer_id = self.number_producers
            self.products_left_per_producer[producer_id] = self.queue_size_per_producer
            self.products_every_producer[str(producer_id)] = []
        self.logger.info("Finished register_producer(): returned producer_id: %s", producer_id)
        return producer_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        self.logger.info("Entered publish()")
        with self.lock_publish:
            if self.products_left_per_producer[producer_id] == 0:
                return False
            self.products_every_producer[str(producer_id)].append(product)
            self.products_left_per_producer[producer_id] -= 1
            self.logger.info("Finished publish(): producer_id %s and product %s", producer_id, product)
            return True
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.logger.info("Entered new_cart")
        with self.lock_register_cart:
            self.number_consumers += 1
            self.carts[str(self.number_consumers)] = []
            self.logger.info("Finished new_cart(): returned cart_id: %s", self.number_consumers)
        return self.number_consumers

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        self.logger.info("Entered add_to_cart()")
        with self.lock_add_to_cart:
            if str(cart_id) not in self.carts:
                self.logger.info("carts: %s", self.carts)
                self.logger.info("Not a valid cart: cart_id: %s", cart_id)
                return False
            self.logger.info("number producers: %s", self.number_producers)
            for producer_id in range(1, self.number_producers + 1):
                product_list = self.products_every_producer.get(str(producer_id), [])
                count = product_list.count(product)
                if count > 0:
                    self.carts[str(cart_id)].append((str(producer_id), product))
                    self.products_every_producer[str(producer_id)].remove(product)
                    self.products_left_per_producer[producer_id] += 1
                    self.logger.info("Finished add_cart(): cart_id: %s, product: %s!", cart_id, product)
                    return True
            return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        self.logger.info("Entered remove_from_cart()")
        with self.lock_remove_from_cart:
            for cart_product in self.carts[str(cart_id)]:
                if cart_product[1] == product:
                    self.carts[str(cart_id)].remove((cart_product[0], cart_product[1]))
                    self.products_left_per_producer[int(cart_product[0])] -= 1
                    self.products_every_producer[cart_product[0]].append(cart_product[1])
                    self.logger.info("Finished remove_cart(): cart_id: %s, product: %s", cart_id, product)
                    return True
            self.logger.info("Not a valid cart: cart_id: %s", cart_id)
            return False

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        self.logger.info("Finished place_order(): list: %s", self.carts[str(cart_id)])
        return self.carts[str(cart_id)]
