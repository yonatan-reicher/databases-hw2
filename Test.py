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

    def test_get_apartment_recommendation(self):
        c1 = Customer(1, 'c1')
        c2 = Customer(2, 'c2')
        a1 = Apartment(10, 'dizingof 10', 'tel aviv', 'ukrain', 100)
        a2 = Apartment(20, 'dizingof 20', 'tel aviv', 'ukrain', 100)
        d1 = date.fromisoformat('2021-01-01')
        d2 = date.fromisoformat('2021-01-10')
        d3 = date.fromisoformat('2021-01-20')
        d4 = date.fromisoformat('2021-01-30')
        self.assertEqual(Solution.get_apartment_recommendation(1), [])
        self.assertEqual(Solution.get_apartment_recommendation(-1), [])
        self.assertEqual(Solution.add_customer(c1), ReturnValue.OK)
        self.assertEqual(Solution.add_customer(c2), ReturnValue.OK)
        self.assertEqual(Solution.add_apartment(a1), ReturnValue.OK)
        self.assertEqual(Solution.add_apartment(a2), ReturnValue.OK)
        self.assertEqual(Solution.get_apartment_recommendation(1), [])
        self.assertEqual(Solution.customer_made_reservation(2, 10, d1, d2, 100), ReturnValue.OK)
        self.assertEqual(Solution.customer_reviewed_apartment(2, 10, d3, 5, 'good'), ReturnValue.OK)
        self.assertEqual(Solution.customer_made_reservation(2, 20, d3, d4, 100), ReturnValue.OK)
        self.assertEqual(Solution.customer_reviewed_apartment(2, 20, d4, 7, 'great'), ReturnValue.OK)
        self.assertEqual(Solution.get_apartment_recommendation(1), [])
        self.assertEqual(Solution.customer_made_reservation(1, 10, d3, d4, 100), ReturnValue.OK)
        self.assertEqual(Solution.customer_reviewed_apartment(1, 10, d4, 3, 'good'), ReturnValue.OK)
        self.assertEqual(Solution.get_apartment_recommendation(1)[0][0].get_id(), a2.get_id())
        self.assertEqual(Solution.get_apartment_recommendation(1)[0][0].get_city(), a2.get_city())
        self.assertEqual(Solution.get_apartment_recommendation(1)[0][0].get_size(), a2.get_size())
        self.assertEqual(Solution.get_apartment_recommendation(1)[0][0].get_address(), a2.get_address())
        self.assertEqual(Solution.get_apartment_recommendation(1), [(a2, 3 / 5 * 7)])

    def test_two_reviews_by_same_customer(self):
        c1 = Customer(1, 'c1')
        a1 = Apartment(10, 'dizingof 10', 'tel aviv', 'ukrain', 100)
        d1 = date.fromisoformat('2021-01-01')
        d2 = date.fromisoformat('2021-01-10')
        d3 = date.fromisoformat('2021-01-20')
        d4 = date.fromisoformat('2021-01-30')
        self.assertEqual(Solution.add_customer(c1), ReturnValue.OK)
        self.assertEqual(Solution.add_apartment(a1), ReturnValue.OK)
        self.assertEqual(Solution.customer_made_reservation(1, 10, d1, d2, 100), ReturnValue.OK)
        self.assertEqual(Solution.customer_reviewed_apartment(1, 10, d3, 5, 'good'), ReturnValue.OK)
        self.assertEqual(Solution.customer_reviewed_apartment(1, 10, d4, 7, 'great'), ReturnValue.ALREADY_EXISTS)


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
