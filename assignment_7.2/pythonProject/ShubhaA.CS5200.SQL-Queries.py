import sqlite3

"""
title: ASSIGNMENT 07.2: Query a Database with SQL in Python 
author: Shubha, Adithya
date: 2024-10-23

"""


def beautifyPrint(rs: list, columnNames: list):
    """
    Prints the result set in a formatted manner.

    Parameters:
        rs (list): The result set to be printed.
        columnNames (list): The list of column names for the result set.
    """
    # referredFrom: https://stackoverflow.com/questions/12453580/how-to-concatenate-join-items-in-a-list-to-a-single-string
    print("; ".join(map(str, columnNames)))
    for row in rs:
        print("; ".join(map(str, row)))


def countriesWithMoreThanTenSuppliers(dbCon: sqlite3.Connection):
    """
    Gets number of countries with more than ten suppliers and
    Fetches and prints countries with and their supplier count.

    Parameters:
        dbCon (sqlite3.Connection): The database connection object.
    """

    try:
        query = ('''SELECT S.Country, COUNT(S.SupplierID) AS TotalSuppliers
                    FROM Suppliers S
                    GROUP BY S.Country
                    HAVING TotalSuppliers > 10''')

        cursor = dbCon.cursor()
        cursor.execute(query)
        rs = cursor.fetchall()
        print(
            "Which countries have more than ten suppliers? List the country and the number of suppliers in each.")
        print("Number of countries with more than 10 suppliers: ", len(rs))

        if (len(rs) > 10):
            print("Countries with more than 10 suppliers")
            columnNames = [description[0] for description in cursor.description]
            beautifyPrint(rs, columnNames)
            lineSeparator()
            print("Fetching countries with their supplier count")
        else:
            print("No countries with more than 10 suppliers.Printing all countries with their supplier count")

        query = ('''SELECT S.Country, COUNT(S.SupplierID) AS TotalSuppliers
                                       FROM Suppliers S
                                       GROUP BY S.Country
                                       ORDER BY TotalSuppliers DESC''')
        cursor = dbCon.cursor()
        cursor.execute(query)
        rs = cursor.fetchall()
        columnNames = [description[0] for description in cursor.description]

        print("Countries with their supplier count")
        beautifyPrint(rs, columnNames)
        cursor.close()

    except sqlite3.Error as exception:
        print("Error in fetching countries with more than 10 suppliers", exception)


def totalProductsSlodByEachSupplier(dbCon: sqlite3.Connection):
    """
    Fetches and prints the total number of products sold by each supplier.

    Parameters:
        dbCon (sqlite3.Connection): The database connection object.
    """

    try:
        query = ('''SELECT S.SupplierName, COUNT(DISTINCT P.ProductID) AS TotalProductsSold
                    FROM Suppliers S
                    JOIN Products P ON S.SupplierID = P.SupplierID
                    GROUP BY S.SupplierName
                    ORDER BY S.SupplierName''')
        cursor = dbCon.cursor()
        cursor.execute(query)
        print(
            "What is the total number of product sold by each supplier. Display the result.")
        rs = cursor.fetchall()
        columnNames = [description[0] for description in cursor.description]
        beautifyPrint(rs, columnNames)
        cursor.close()
    except sqlite3.Error as exception:
        print("Error in fetching total products sold by each supplier", exception)


def fetchDetailsOfSuppliers(dbCon: sqlite3.Connection):
    """
    Fetches and prints the name, contact name, and country of all suppliers, sorted by supplier name.

    Parameters:
        dbCon (sqlite3.Connection): The database connection object.
    """

    try:
        query = ('''SELECT S.SupplierName, S.ContactName, S.Country
                    FROM Suppliers S
                    ORDER BY S.SupplierName''')
        cursor = dbCon.cursor()
        cursor.execute(query)
        print(
            "What are the name, contact name, and country of all suppliers, sorted by supplier name? Print the result set.")
        rs = cursor.fetchall()
        columnNames = [description[0] for description in cursor.description]
        beautifyPrint(rs, columnNames)
        cursor.close()
    except sqlite3.Error as exception:
        print("Error in fetching details of suppliers", exception)


def connectToDatabase(databaseName: str) -> sqlite3.Connection:
    """
    Connects to the specified SQLite database.
    Parameters:
       databaseName (str): The name of the SQLite database file.
    Returns:
       sqlite3.Connection: The database connection object if successful, None otherwise.
    """

    try:
        dbCon = sqlite3.connect(databaseName)
        return dbCon
    except sqlite3.Error as exception:
        print("Cannot connect to database", exception)
    return None


def lineSeparator():
    """
    Prints a line separator.
    """
    print("________________________________________________________________________________________")


def main():
    """
    Main function to execute the script. Connects to the database and fetches various details.
    """
    dbCon = connectToDatabase("OrdersDB.sqlitedb.db")
    if dbCon is None:
        return
    dbCon.execute("PRAGMA foreign_keys = ON")
    fetchDetailsOfSuppliers(dbCon)
    lineSeparator()
    totalProductsSlodByEachSupplier(dbCon)
    lineSeparator()
    countriesWithMoreThanTenSuppliers(dbCon)
    lineSeparator()

    dbCon.close()


# Code is referred from: http://artificium.us/lessons/07.python/l-7-300-primer-sqlite-py/l-7-300.html
main()
