import unittest
from Solution import *
from Utility.ReturnValue import ReturnValue


class TestCRUD(unittest.TestCase):

    def setUp(self):
        create_tables()

    def tearDown(self):
        # This method will be called after each test
        # Clean up your test environment
        clear_tables()

    def test_Owner(self):
        self.assertEqual(add_owner(Owner(1, "Dan")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "Yuval")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_owner(Owner(0, "")), ReturnValue.BAD_PARAMS)
        # self.assertEqual(add_owner(None), ReturnValue.ERROR)

        self.assertEqual(get_owner(1).get_owner_id(), 1)

        self.assertEqual(delete_owner(3), ReturnValue.OK)
        self.assertEqual(delete_owner(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_owner(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_owner(nullptr), ReturnValue.ERROR)

        self.assertEqual(get_owner(3), Owner.bad_owner())
        self.assertEqual(add_owner(Owner(3, "Nadav")), ReturnValue.OK)

    def test_apartment(self):
        self.assertEqual(add_apartment(Apartment(1, "123 Main St", "Haifa", "Israel", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(2, "456 Elm St", "NY", "NY", 800)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(3, "123 Main St", "Haifa", "lebanon", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(0, 0, 0, 0, 0)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(1, "123 Main St", "Haifa", "Israel", 80)), ReturnValue.ALREADY_EXISTS)
        # self.assertEqual(add_apartment(None), ReturnValue.ERROR)

        self.assertEqual(get_apartment(1).get_id(), 1)

        self.assertEqual(delete_apartment(3), ReturnValue.OK)
        self.assertEqual(delete_apartment(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_apartment(0), ReturnValue.BAD_PARAMS)
        # self.assertEqual(delete_apartment(None), ReturnValue.ERROR)

        self.assertEqual(get_apartment(1), Apartment.bad_apartment())
        self.assertEqual(add_apartment(Apartment(3, "123 Main St", "Haifa", "lebanon", 80)), ReturnValue.OK)

    def test_customer(self):
        self.assertEqual(add_customer(Customer(1, "Dani")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(2, "Yuvali")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(3, "Nadavi")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(3, "Nadavi")), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_customer(Customer(0, "")), ReturnValue.BAD_PARAMS)
        # self.assertEqual(add_customer(None), ReturnValue.BAD_PARAMS)

        self.assertEqual(get_customer(1).get_customer_id(), 1)

        self.assertEqual(delete_customer(3), ReturnValue.OK)
        self.assertEqual(delete_customer(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_customer(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_customer(nullptr), ReturnValue.ERROR)

        self.assertEqual(get_customer(3), Customer.bad_customer())

    def owner_owns_apartment(self):
        self.assertEqual(owner_owns_apartment(1, 1), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 2), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 3), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 1), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(owner_owns_apartment(1, 10), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_owns_apartment(0, 0), ReturnValue.BAD_PARAMS)

        self.assertEqual(get_apartment_owner(1).get_id(), 1)
        self.assertEqual(get_owner_apartments(1), [1, 3])

        self.assertEqual(owner_doesnt_own_apartment(1, 3), ReturnValue.OK)
        self.assertEqual(owner_doesnt_own_apartment(1, 3), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_doesnt_own_apartment(0, 3), ReturnValue.BAD_PARAMS)

        self.assertEqual(get_apartment_owner(1), Owner.bad_owner())
        self.assertEqual(get_owner_apartments(3), [])

        self.assertEqual(owner_owns_apartment(1, 3), ReturnValue.OK)


if __name__ == "__main__":
    unittest.main()
