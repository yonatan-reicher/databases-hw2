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
                     "CONSTRAINT positive_owner_id CHECK (id > 0))"
                     )

        conn.execute("CREATE TABLE Customer("
                     "id INTEGER PRIMARY KEY,"
                     "name TEXT NOT NULL,"
                     "CONSTRAINT positive_customer_id CHECK (id > 0))"
                     )

        conn.execute("""
            CREATE TABLE Apartment(
                id          INTEGER PRIMARY KEY, 
                address     TEXT NOT NULL,
                city        TEXT NOT NULL,
                country     TEXT NOT NULL,
                size        INTEGER NOT NULL,
                CONSTRAINT positive_apartment_id CHECK (id > 0),
                CONSTRAINT positive_size CHECK (size > 0),
                CONSTRAINT unique_address UNIQUE (address, city, country)
            )
        """)
        conn.execute("""
            CREATE TABLE Owns(
                owner_id        INTEGER REFERENCES Owner(id) ON DELETE CASCADE,
                apartment_id    INTEGER REFERENCES Apartment(id) ON DELETE CASCADE,
                CONSTRAINT positive_owner_id CHECK (owner_id > 0),
                CONSTRAINT positive_apartment_id CHECK (apartment_id > 0),
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
                        CONSTRAINT positive_price CHECK (price > 0),
                        CONSTRAINT positive_customer_id CHECK (customer_id > 0),
                        CONSTRAINT positive_apartment_id CHECK (apartment_id > 0),
                        CONSTRAINT legal_dates CHECK (start_date <= end_date)
                    )
                """)
        conn.execute("""
                    CREATE TABLE Review(
                        customer_id     INTEGER REFERENCES Customer(id) ON DELETE CASCADE,
                        apartment_id    INTEGER REFERENCES Apartment(id) ON DELETE CASCADE,
                        date            DATE NOT NULL,      
                        rating          INTEGER NOT NULL,
                        review_text     TEXT NOT NULL,
                        CONSTRAINT positive_customer_id CHECK (customer_id > 0),
                        CONSTRAINT positive_apartment_id CHECK (apartment_id > 0),
                        PRIMARY KEY (customer_id, apartment_id),
                        CONSTRAINT legal_rating CHECK (rating >= 1 AND rating <= 10) 
                    )
                """)
        conn.execute("""
                    CREATE VIEW OAP AS
                        SELECT A.owner_id, B.name, A.apartment_id, C.address, C.city, C.country, C.size
                        FROM Owns A, Owner B, Apartment C
                        WHERE A.owner_id = B.id AND A.apartment_id = C.id
                       """)
        # Create an average rating view for use with get_apartment_rating, get_owner_rating
        # Note: How do apartments with no reviews get an average rating of 0?
        #       We use a join
        conn.execute("""
            CREATE VIEW AverageApartmentRating AS
            SELECT apartment_id, average_rating
            FROM (
                SELECT apartment_id, AVG(rating) AS average_rating FROM Review
                GROUP BY apartment_id
                UNION ALL
                SELECT id AS apartment_id, 0 FROM Apartment
                WHERE id NOT IN (SELECT apartment_id FROM Review)
            )
        """)
        conn.execute("""
                        CREATE VIEW CustomerRatio AS
                        SELECT R1.customer_id AS id1,
                               R2.customer_id AS id2,
                               AVG(R1.rating / R2.rating::DECIMAL) AS ratio
                        FROM Review AS R1
                        JOIN Review AS R2 ON R1.apartment_id = R2.apartment_id
                        WHERE R1.customer_id != R2.customer_id
                        GROUP BY R1.customer_id, R2.customer_id
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


tables = [
    'Owner',
    'Customer',
    'Apartment',
    'Owns',
    'Reservation',
    'Review',
]


def clear_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        for table in tables:
            conn.execute(f"DELETE FROM {table}")
    except Exception as e:
        print(e)
    finally:
        if conn: conn.close()


def drop_tables():
    conn = None
    try:
        conn = Connector.DBConnector()
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    except Exception as e:
        print(e)
    finally:
        if conn: conn.close()


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


def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT name FROM owner WHERE id = {ownerid}").format(ownerid=sql.Literal(owner_id))
        rows_effected, result = conn.execute(query)
        if not result.isEmpty():
            owner_name = result[0]['name']
            return Owner(owner_id, owner_name)
        else:
            return Owner.bad_owner()
    except DatabaseException.ConnectionInvalid as e:
        return Owner.bad_owner()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return Owner.bad_owner()
    except DatabaseException.CHECK_VIOLATION as e:
        return Owner.bad_owner()
    except DatabaseException.UNIQUE_VIOLATION as e:
        return Owner.bad_owner()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Owner.bad_owner()
        # will happen any way after try termination or exception handling
    finally:
        if conn:
            conn.close()

def delete_owner(owner_id: int) -> ReturnValue:
    # for delete funcs, params value can and should be tested via python.
    if owner_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM owner WHERE id = {ownerid}").format(ownerid=sql.Literal(owner_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected != 0:
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
    return return_value


def add_apartment(apartment: Apartment) -> ReturnValue:
    if not isinstance(apartment, Apartment):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.ERROR
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Apartment(id, address, city, country, size) VALUES({apartmentid}, "
                        "{apartmentaddress}, {apartmentcity}, {apartmentcountry}, {apartmentsize})").format(
            apartmentid=sql.Literal(apartment.get_id()),
            apartmentaddress=sql.Literal(apartment.get_address()),
            apartmentcity=sql.Literal(apartment.get_city()),
            apartmentcountry=sql.Literal(apartment.get_country()),
            apartmentsize=sql.Literal(apartment.get_size()))
        rows_effected, _ = conn.execute(query)
        return_value = ReturnValue.OK

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


def get_apartment(apartment_id: int) -> Apartment:
    if not isinstance(apartment_id, int):
        return Apartment.bad_apartment()
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT id, address, city, country, size FROM Apartment WHERE id = {apartmentid}").format(
            apartmentid=sql.Literal(apartment_id))
        rows_affected, result = conn.execute(query)
        if rows_affected != 0:
            return Apartment(result['id'][0], result['address'][0], result['city'][0], result['country'][0], result['size'][0])
        else:
            return Apartment.bad_apartment()
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    if apartment_id <=0:
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartment WHERE id = {apartmentid}").format(apartmentid=sql.Literal(apartment_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected != 0:
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


def get_customer(customer_id: int) -> Customer:
    if not isinstance(customer_id, int):
        return Customer.bad_customer()
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT name FROM customer WHERE id = {customerid}").format(customerid=sql.Literal(customer_id))
        rows_effected, result = conn.execute(query)
        if not result.isEmpty():
            customer_name = result[0]['name']
            return Customer(customer_id, customer_name)
        else:
            return Customer.bad_customer()
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    if customer_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customer WHERE id = {customerid}").format(customerid=sql.Literal(customer_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected != 0:
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
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return return_value


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    #if (not isinstance(customer_id, int) or not isinstance(apartment_id, int) or not isinstance(start_date, date)
     #       or not isinstance(end_date, date) or customer_id <=0 or apartment_id <=0):
    #    return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                INSERT INTO Reservation(customer_id, apartment_id, start_date, end_date, price)
                SELECT {customerid}, {apartmentid}, {startdate}, {enddate}, {apartmentprice}
                WHERE NOT EXISTS(
                    SELECT 1 FROM Reservation AS selection
                    WHERE selection.apartment_id = {apartmentid}
                    AND (selection.start_date, selection.end_date) OVERLAPS ({startdate}, {enddate})
                )
            """).format(
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


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if (customer_id <= 0 or apartment_id <= 0):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Reservation WHERE customer_id = {customerid} "
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


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    if (customer_id <=0 or apartment_id <= 0 or rating <=0 or rating >10):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                INSERT INTO Review(customer_id, apartment_id, date, rating, review_text) 
                SELECT {customerid}, {apartmentid}, {reviewdate}, {rating}, {reviewtext}
                WHERE EXISTS(
                    SELECT 1 FROM Reservation AS selection 
                    WHERE selection.apartment_id = {apartmentid}
                    AND selection.customer_id = {customerid}
                    AND selection.end_date <= {reviewdate}
                )
            """).format(
            customerid=sql.Literal(customer_id),
            apartmentid=sql.Literal(apartment_id),
            reviewdate=sql.Literal(review_date),
            rating=sql.Literal(rating),
            reviewtext=sql.Literal(review_text)
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
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return return_value


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:

    if (customer_id <= 0 or apartment_id <= 0):
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    #TODO: USE VIEW HERE
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            UPDATE Review 
            SET date = {reviewdate}, rating = {reviewrating}, review_text = {reviewtext}
            WHERE(
                customer_id = {customerid}
                AND apartment_id = {apartmentid}
                AND EXISTS(
                    SELECT 1 FROM Review AS selection 
                    WHERE selection.apartment_id = {apartmentid}
                    AND selection.customer_id = {customerid}
                    AND selection.date <= {reviewdate}
                )
            )
        """).format(
            customerid=sql.Literal(customer_id),
            apartmentid=sql.Literal(apartment_id),
            reviewdate=sql.Literal(update_date),
            reviewrating=sql.Literal(new_rating),
            reviewtext=sql.Literal(new_text)
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
        return_value = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return return_value


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if not isinstance(owner_id, int) or not isinstance(apartment_id, int) or owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owns(owner_id, apartment_id) VALUES({ownerid}, {apartmentid})").format(
            ownerid=sql.Literal(owner_id),
            apartmentid=sql.Literal(apartment_id))
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
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return return_value


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if not isinstance(owner_id, int) or not isinstance(apartment_id, int) or owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owns "
                        "WHERE owner_id = {ownerid} "
                        "AND apartment_id = {apartmentid}").format(
                        ownerid=sql.Literal(owner_id),
                        apartmentid=sql.Literal(apartment_id))
        rows_effected, _ = conn.execute(query)
        if rows_effected != 0:
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
        return_value = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return_value = ReturnValue.NOT_EXISTS
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return return_value


#TODO: check that the view used here is indeed working
def get_apartment_owner(apartment_id: int) -> Owner:
    if not isinstance(apartment_id, int):
        return Owner.bad_owner()
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT owner_id, name FROM OAP WHERE apartment_id = {apartmentid}").format(
                                                                apartmentid=sql.Literal(apartment_id))
        rows_effected, result = conn.execute(query)
        if rows_effected == 0:
            return Owner.bad_owner()
        if result is not None:
            return Owner(result['owner_id'][0], result['name'][0])
        else:
            return Owner.bad_owner()
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()


#TODO: check that the view used here is indeed working
def get_owner_apartments(owner_id: int) -> List[Apartment]:
    query_result = []
    if not isinstance(owner_id, int):
        return query_result
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT owner_id, apartment_id, address, city, country, size FROM OAP WHERE owner_id = {ownerid}").format(
            ownerid=sql.Literal(owner_id))
        rows_effected, result = conn.execute(query)
        if rows_effected == 0:
            return query_result
        if not result.isEmpty():
            for index in result:
                query_result.append(Apartment(
                    index['apartment_id'],
                    index['address'],
                    index['city'],
                    index['country'],
                    index['size']
                ))
    finally:
        # will happen any way after try termination or exception handling
        if conn:
            conn.close()
    return query_result


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    if not isinstance(apartment_id, int):
        return 0

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT average_rating
            FROM AverageApartmentRating
            WHERE apartment_id = {apartment_id}
        """).format(apartment_id=sql.Literal(apartment_id))
        _, result = conn.execute(query)
        return result[0]['average_rating'] if not result.isEmpty() else 0
    finally:
        if conn: conn.close()


def get_owner_rating(owner_id: int) -> float:
    if not isinstance(owner_id, int):
        return 0

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT COALESCE(AVG(average_rating), 0) AS average_rating
            FROM AverageApartmentRating, Owns
            WHERE owner_id = {owner_id}
            AND AverageApartmentRating.apartment_id = Owns.apartment_id
        """).format(owner_id=sql.Literal(owner_id))
        _, result = conn.execute(query)
        return result[0]['average_rating'] if not result.isEmpty() else 0
    finally:
        if conn: conn.close()


def get_top_customer() -> Customer:
    conn = None
    try:
        # How do I construct a customer? From an id and name.
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT customer_id, name
            FROM Customer, (
                SELECT customer_id, COUNT(*) AS reservations
                FROM Reservation
                GROUP BY customer_id
                UNION ALL
                SELECT id AS customer_id, 0
                FROM Customer
                WHERE id NOT IN (SELECT customer_id FROM Reservation)
            ) as T
            WHERE Customer.id = T.customer_id
            ORDER BY reservations DESC, customer_id ASC
            LIMIT 1
        """).format()
        _, result = conn.execute(query)
        return Customer(result[0]['customer_id'], result[0]['name']) if not result.isEmpty() else Customer.bad_customer()
    finally:
        if conn: conn.close()


def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT name, COUNT(*) AS reservations
            FROM Owner, Owns, Reservation
            WHERE Owner.id = Owns.owner_id
            AND Owns.apartment_id = Reservation.apartment_id
            GROUP BY name

            UNION ALL

            SELECT name, 0
            FROM Owner
            WHERE NOT EXISTS (
                SELECT *
                FROM Owns, Reservation
                WHERE Owner.id = Owns.owner_id
                AND Owns.apartment_id = Reservation.apartment_id
            )
        """).format()
        _, result = conn.execute(query)
        return list((row['name'], row['reservations']) for row in result)
    finally:
        if conn: conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # Owners that have apartments in all cities
    # => Owners such that there is no city that owner doesn't have an apartment
    #    in.
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT DISTINCT id, name
            FROM Owner
            WHERE NOT EXISTS (
                SELECT * FROM Apartment as A
                WHERE NOT EXISTS (
                    SELECT * FROM OAP
                    WHERE OAP.city = A.city
                    AND OAP.country = A.country
                    AND OAP.owner_id = Owner.id
                )
            )
        """).format()
        _, result = conn.execute(query)
        return list(Owner(row['id'], row['name']) for row in result)
    finally:
        if conn: conn.close()


def best_value_for_money() -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT Apartment.id AS id, address, city, country, size
            FROM Apartment, AverageApartmentRating, (
                SELECT 
                    apartment_id,
                    COALESCE(AVG(price / (end_date - start_date)), 0) AS avg_night_price
                FROM Reservation
                GROUP BY apartment_id
            ) AS T
            WHERE Apartment.id = AverageApartmentRating.apartment_id
            AND Apartment.id = T.apartment_id
            ORDER BY average_rating / avg_night_price DESC
            LIMIT 1
        """).format()
        _, result = conn.execute(query)
        return Apartment(
            result[0]['id'],
            result[0]['address'],
            result[0]['city'],
            result[0]['country'],
            result[0]['size']
        ) if not result.isEmpty() else Apartment.bad_apartment()
    finally:
        if conn: conn.close()


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            (
                SELECT EXTRACT(MONTH FROM end_date) AS month, 0.15 * SUM(price) AS profit
                FROM Reservation
                WHERE EXTRACT(YEAR FROM end_date) = {year}
                GROUP BY month
                UNION ALL
                SELECT month, 0
                FROM generate_series(1, 12) AS month
                WHERE month NOT IN (
                    SELECT EXTRACT(MONTH FROM end_date) FROM Reservation
                    WHERE EXTRACT(YEAR FROM end_date) = {year}
                )
            ) ORDER BY month
        """).format(year=sql.Literal(year))
        _, result = conn.execute(query)
        return list((row['month'], row['profit']) for row in result)
    finally:
        if conn: conn.close()


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # For every apartment:
    #   For every other customer != customer:
    #     our_apartments = customer apartments /\ other customer apartments
    #     his_average_rating = average rating by other of our apartments
    #     my_average_rating = average rating by customer of our apartments
    #     approx = my_average_rating / his_average_rating * his_rating of apartment
    #   approx = average of approxes
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
            SELECT A.id AS apartment_id, address, city, country, size,
                   AVG(LEAST(GREATEST(ratio * R.rating, 1), 10)) AS approx
            FROM Apartment as A, CustomerRatio, Review as R
            WHERE A.id = R.apartment_id
            AND CustomerRatio.id1 = {customer_id}
            AND CustomerRatio.id2 = R.customer_id
            AND A.id NOT IN (
                SELECT apartment_id
                FROM Reservation
                WHERE customer_id = {customer_id}
            )
            GROUP BY A.id, address, city, country, size
            ORDER BY A.id ASC
        """).format(customer_id=sql.Literal(customer_id))
        _, result = conn.execute(query)
        return list(
            (Apartment(row['apartment_id'], row['address'], row['city'], row['country'], row['size']), float(row['approx']))
            for row in result
        )
    finally:
        if conn: conn.close()
