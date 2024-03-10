import unittest
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from Tests.AbstractTest import AbstractTest
from datetime import date

from Business.Apartment import Apartment
from Business.Owner import Owner
from Business.Customer import Customer

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):
    def test_customer(self) -> None:
        c1 = Customer(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
        c2 = Customer(2, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c2), 'invalid name')

    def test_owner(self) -> None:
        o1 = Owner(1, 'o1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner')
        o2 = Owner(2, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_owner(o2), 'invalid name')

    def test_owns_cascade(self) -> None:
        o1 = Owner(1, 'o1')
        c1 = Customer(1, 'c1')
        a1 = Apartment(1, 'dizingof', 'tel aviv', 'ukrain', 100)
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1))
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1))
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(a1))
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1))
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(
            1, 1,
            date.fromisoformat('2021-01-01'),
            date.fromisoformat('2021-01-10'),
            100,
        ))
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(1))
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_cancelled_reservation(
            1, 1,
            date.fromisoformat('2021-01-01'),
        ))


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
