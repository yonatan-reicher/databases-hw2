from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Owner("
                     "id INTEGER PRIMARY KEY,"
                     "name TEXT NOT NULL,"
                     "CONSTRAINT positive_owner_id CHECK (id >= 0))"
                     )

        conn.execute("CREATE TABLE Customer("
                     "id INTEGER PRIMARY KEY,"
                     "name TEXT NOT NULL,"
                     "CONSTRAINT positive_customer_id CHECK (id >= 0))"
                     )

        conn.execute("""
            CREATE TABLE Apartment(
                id          INTEGER PRIMARY KEY, 
                address     TEXT NOT NULL,
                city        TEXT NOT NULL,
                country     TEXT NOT NULL,
                size        INTEGER NOT NULL,
                CONSTRAINT positive_apartment_id CHECK (id >= 0)
                CONSTRAINT positive_size CHECK (size > 0)
                CONSTRAINT unique_address UNIQUE (address, city, country)
            )
        """)
        conn.execute("""
            CREATE TABLE Owns(
                owner_id        INTEGER REFERENCES Owner(id) ON DELETE CASCADE,
                apartment_id    INTEGER REFERENCES Apartment(id) ON DELETE CASCADE,
                PRIMARY KEY(apartment_id)
            )
        """)
        conn.execute("""
                    CREATE TABLE Reservation(
                        customer_id     INTEGER REFERENCES Customer(id) ON DELETE CASCADE,
                        apartment_id    INTEGER REFERENCES Apartment(id) ON DELETE CASCADE,
                        start_date      DATE NOT NULL,      
                        end_date        DATE NOT NULL, 
                        price           INTEGER NOT NULL,
                        PRIMARY KEY(apartment_id, costumer_id)
                        CONSTRAINT positive_price CHECK (price > 0)
                        CONSTRAINT legal_dates CHECK (start_date <= end_date) NOT VALID
                    )
                """)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        if conn: conn.close()


def clear_tables():
    # TODO: implement
    pass


def drop_tables():
    # TODO: implement
    pass


def add_owner(owner: Owner) -> ReturnValue:
    if not isinstance(owner, Owner):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:

        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owner(id, name) VALUES({ownerid}, {ownername})").format(
            ownerid=sql.Literal(owner.get_owner_id()),
            ownername=sql.Literal(owner.get_owner_name()))
        rows_effected, _ = conn.execute(query)

    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT name FROM owner WHERE id = {ownerid}").format(ownerid=sql.Literal(owner_id))
        rows_effected, owner_name = conn.execute(query)
        if owner_name is not None:
            return Owner(owner_id, owner_name)
        else:
            return Owner.bad_owner()
        # will happen any way after try termination or exception handling
    finally:
        if conn:
            conn.close()
    pass

def delete_owner(owner_id: int) -> ReturnValue:
    # for delete funcs, params value can and should be tested via python.
    if not isinstance(owner_id, int):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM owner WHERE id = {ownerid}").format(ownerid=sql.Literal(owner_id))
        rows_effected = conn.execute(query)
        if rows_effected is not 0:
            # Owner exists and deleted one or more times
            return ReturnValue.OK
        else:
            return ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return_value = ReturnValue.OK
    pass


def add_apartment(apartment: Apartment) -> ReturnValue:
    if not isinstance(apartment, Apartment):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Apartment(id, address, city, country, size) VALUES({apartmentid}, "
                        "{apartmentaddress}), {apartmentcity}, {apartmentcountry}, {apartmentsize})").format(
            apartmentid=sql.Literal(apartment.get_id()),
            apartmentaddress=sql.Literal(apartment.get_address()),
            apartmentcity=sql.Literal(apartment.get_city()),
            apartmentcountry=sql.Literal(apartment.get_country()),
            apartmentsize=sql.Literal(apartment.get_size()))
        rows_effected, _ = conn.execute(query)

    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def get_apartment(apartment_id: int) -> Apartment:
    if not isinstance(apartment_id, int):
        return Apartment.bad_apartment()
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT id, address, city, country, size FROM Apartment WHERE id = {apartmentid}").format(
            apartmentid=sql.Literal(apartment_id))
        rows_affected, result = conn.execute(query)
        if rows_affected is not 0:
            return Apartment(result[0], result[1], result[2], result[3], result[4])
        else:
            return Apartment.bad_apartment()
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    if not isinstance(apartment_id, int):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE id = {apartmentid}").format(ownerid=sql.Literal(apartment_id))
        rows_effected = conn.execute(query)
        if rows_effected is not 0:
            # Apartment exists and deleted one or more times
            return ReturnValue.OK
        else:
            # Apartment does not exist
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def add_customer(customer: Customer) -> ReturnValue:
    if not isinstance(customer, Customer):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Customer(id, name) VALUES({customerid}, {customername})").format(
            customerid=sql.Literal(customer.get_customer_id()),
            customername=sql.Literal(customer.get_customer_name()))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 1:
            return ReturnValue.OK
        if rows_effected == 0:
            return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def get_customer(customer_id: int) -> Customer:
    if not isinstance(customer_id, int):
        return Customer.bad_customer()
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT name FROM customer WHERE id = {customerid}").format(customerid=sql.Literal(customer_id))
        rows_effected, customer_name = conn.execute(query)
        if customer_name is not None:
            return Customer(customer_id, customer_name)
        else:
            return Customer.bad_customer()
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    if not isinstance(customer_id, int):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE id = {customerid}").format(customerid=sql.Literal(customer_id))
        rows_effected = conn.execute(query)
        if rows_effected is not 0:
            # Customer exists and deleted one or more times
            return ReturnValue.OK
        else:
            # Customer does not exist
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    if (not isinstance(customer_id, int) or not isinstance(apartment_id, int) or not isinstance(start_date, date)
            or not isinstance(end_date, date)):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Reservasion(costumer_id, apartment_id, start_date, end_date, price) "
                        "SELECT({customerid}, {apartmentid}, {startdate}, {enddate}, {apartmentprice})"
                        "WHERE NOT EXISTS("
                        "SELECT 1 FROM Reservasion AS selection "
                        "WHERE selection.apartment_id = {apartmentid}"
                        "AND (selection.start_date, selection.end_date) OVERLAPS (startdate, enddate)").format(
            customerid=sql.Literal(customer_id),
            apartmentid=sql.Literal(apartment_id),
            startdate=sql.Literal(start_date),
            enddate=sql.Literal(end_date),
            apartmentprice=sql.Literal(total_price)
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 1:
            return ReturnValue.OK
        if rows_effected == 0:
            return ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if (not isinstance(customer_id, int) or not isinstance(apartment_id, int) or not isinstance(start_date, date)):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Reservasion WHERE customer_id = {customerid} "
                        "AND apartment_id = {apartmentid} "
                        "AND start_date = {startdate}  ").format(
            customerid=sql.Literal(customer_id),
            apartmentid=sql.Literal(apartment_id),
            startdate=sql.Literal(start_date),
        )
        rows_effected, _ = conn.execute(query)
        if rows_effected == 1:
            return ReturnValue.OK
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return_value = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
        return return_value
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
