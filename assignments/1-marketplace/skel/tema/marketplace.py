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
try:
    from tema.product import Coffee, Tea
except ImportError:
    from product import Coffee, Tea


class TestMarketplace(unittest.TestCase):
    """
    Class that represents the unitTest for Marketplace.
    """

    def setUp(self):
        """Initialization"""
        self.queue_size_per_producer = 5
        self.marketplace = Marketplace(self.queue_size_per_producer)

    def test_register_producer(self):
        """
        Function that represents the unitTest for register_producer.
        """
        self.assertEqual(self.marketplace.register_producer(), 1, "Expected to return id 1")

    def test_register_multiple_producer(self):
        """
        Function that represents the unitTest for multiple register_producer.
        """
        self.assertEqual(self.marketplace.register_producer(), 1, "Expected to return id 1")
        self.assertEqual(self.marketplace.register_producer(), 2, "Expected to return id 2")

    def test_publish(self):
        """
        Function that represents the unitTest for publish.
        """
        producer_id = self.marketplace.register_producer()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.assertTrue(self.marketplace.publish(producer_id, product), "Expected to return True")

    def test_multiple_publish(self):
        """
        Function that represents the unitTest for multiple publish.
        """
        producer_id = self.marketplace.register_producer()
        product1 = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        product2 = Tea(name="Linden", type="Herbal", price=9)
        self.assertTrue(self.marketplace.publish(producer_id, product1), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product2), "Expected to return True")

    def test_publish_more_then_max(self):
        """
        Function that represents the unitTest for publish more elements than space.
        """
        producer_id = self.marketplace.register_producer()
        product1 = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        product2 = Tea(name="Linden", type="Herbal", price=9)
        product3 = Coffee(name="Brasil", acidity="5.09", roast_level="MEDIUM", price=7)
        product4 = Tea(name="Wild Cherry", type="Black", price=5)
        product5 = Coffee(name="Ethiopia", acidity="5.09", roast_level="MEDIUM", price=10)
        product6 = Tea(name="Cactus Fig", type="Green", price=3)
        self.assertTrue(self.marketplace.publish(producer_id, product1), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product2), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product3), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product4), "Expected to return True")
        self.assertTrue(self.marketplace.publish(producer_id, product5), "Expected to return True")
        self.assertFalse(self.marketplace.publish(producer_id, product6), "Expected to "
                                                                          "return False")

    def test_new_cart(self):
        """
        Function that represents the unitTest for new_cart.
        """
        self.assertEqual(self.marketplace.new_cart(), 1, "Expected to return id 1")

    def test_multiple_new_cart(self):
        """
        Function that represents the unitTest for multiple new_cart.
        """
        self.assertEqual(self.marketplace.new_cart(), 1, "Expected to return id 1")
        self.assertEqual(self.marketplace.new_cart(), 2, "Expected to return id 2")

    def test_add_to_cart(self):
        """
        Function that represents the unitTest for add_to_cart.
        """
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.assertTrue(self.marketplace.add_to_cart(cart_id, product), "Expected to return True")

    def test_remove_from_cart(self):
        """
        Function that represents the unitTest for remove_from_cart.
        """
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.marketplace.add_to_cart(cart_id, product)
        self.assertTrue(self.marketplace.remove_from_cart(cart_id, product),
                        "Expected to return True")

    def test_place_order(self):
        """
        Function that represents the unitTest for place_order.
        """
        producer_id = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        self.marketplace.publish(producer_id, product)
        self.marketplace.add_to_cart(cart_id, product)
        self.assertEqual(self.marketplace.place_order(cart_id), [('1', Coffee(name='Indonezia',
            price=1, acidity='5.05', roast_level='MEDIUM'))], "Expected to return Coffee Product")

    def test_place_order_complex(self):
        """
        Function that represents the unitTest for complex place_order.
        """
        producer_id1 = self.marketplace.register_producer()
        cart_id = self.marketplace.new_cart()
        product1 = Coffee(name="Indonezia", acidity="5.05", roast_level="MEDIUM", price=1)
        product2 = Tea(name="Linden", type="Herbal", price=9)
        product3 = Coffee(name="Brasil", acidity="5.09", roast_level="MEDIUM", price=7)
        product4 = Tea(name="Wild Cherry", type="Black", price=5)
        product5 = Coffee(name="Ethiopia", acidity="5.09", roast_level="MEDIUM", price=10)
        product6 = Tea(name="Cactus Fig", type="Green", price=3)
        self.marketplace.publish(producer_id1, product1)
        self.marketplace.publish(producer_id1, product2)
        self.marketplace.publish(producer_id1, product3)
        self.marketplace.publish(producer_id1, product4)
        self.marketplace.publish(producer_id1, product5)
        self.marketplace.publish(producer_id1, product6)
        self.marketplace.add_to_cart(cart_id, product1)
        self.marketplace.add_to_cart(cart_id, product2)
        self.marketplace.add_to_cart(cart_id, product3)
        self.marketplace.remove_from_cart(cart_id, product1)
        self.marketplace.remove_from_cart(cart_id, product2)
        self.marketplace.add_to_cart(cart_id, product4)
        self.assertEqual(self.marketplace.place_order(cart_id), [('1',
            Coffee(name="Brasil", acidity="5.09",roast_level="MEDIUM", price=7)), ('1',
            Tea(name="Wild Cherry", type="Black", price=5))], "Expected to return Tea")


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
        # nr of elements that every producer can add
        self.products_left_per_producer = {}
        # products for every producer
        self.products_every_producer = {}
        # producer_id (in case it is deleted and put back to products_every_producer) + product
        self.carts = {}
        # locks for possible race condition situations
        self.lock_add_to_cart = Lock()
        self.lock_register_producer = Lock()
        self.lock_register_cart = Lock()
        self.number_producers = 0
        self.number_consumers = 0
        # logger
        log_formatter = logging.Formatter("%(asctime)s:%(levelname)s: " + "%(message)s")
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
        # lock for possible race condition on giving the producer_id
        with self.lock_register_producer:
            self.number_producers += 1
            producer_id = self.number_producers
            # first every producer has queue_size_per_producer spaces left to create products
            self.products_left_per_producer[producer_id] = self.queue_size_per_producer
            # make an empty array of products
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

        # if no space left
        if self.products_left_per_producer[producer_id] == 0:
            return False
        # add product to producer_id list
        self.products_every_producer[str(producer_id)].append(product)
        # delete one space for products
        self.products_left_per_producer[producer_id] -= 1
        self.logger.info("Finished publish(): producer_id %s and product %s",
                         producer_id, product)
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        # lock for possible race condition for giving the cart_id
        self.logger.info("Entered new_cart")
        with self.lock_register_cart:
            self.number_consumers += 1
            cart_id = self.number_consumers
            # create empty array corresponding to the cart_id
            self.carts[str(cart_id)] = []
            self.logger.info("Finished new_cart(): returned cart_id: %s", cart_id)
        return cart_id

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
        # lock for possible race condition when get elements for cart
        with self.lock_add_to_cart:
            self.logger.info("number producers: %s", self.number_producers)
            # get all products ids
            for producer_id in range(1, self.number_producers + 1):
                product_list = self.products_every_producer.get(str(producer_id), [])
                count = product_list.count(product)
                # if the producer has the product
                if count > 0:
                    # add in the cart the producer id and the product
                    self.carts[str(cart_id)].append((str(producer_id), product))
                    # remove the product from producer id list
                    self.products_every_producer[str(producer_id)].remove(product)
                    # make space for other product to make
                    self.products_left_per_producer[producer_id] += 1
                    self.logger.info("Finished add_cart(): cart_id: %s, product: %s!",
                                     cart_id, product)
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
        for cart_product in self.carts[str(cart_id)]:
            # if the product was found
            if cart_product[1] == product:
                # remove the product from cart
                self.carts[str(cart_id)].remove((cart_product[0], cart_product[1]))
                # we have the product_id = cart_product[0]
                # remove space from product id list
                self.products_left_per_producer[int(cart_product[0])] -= 1
                # put item in product id list
                self.products_every_producer[cart_product[0]].append(cart_product[1])
                self.logger.info("Finished remove_cart(): cart_id: %s, product: %s",
                                 cart_id, product)
                return True
        self.logger.info("Not a valid cart: cart_id: %s", cart_id)
        return False

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        # get the list of items from cart
        self.logger.info("Finished place_order(): list: %s", self.carts[str(cart_id)])
        return self.carts[str(cart_id)]
