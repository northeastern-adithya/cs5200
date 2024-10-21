# ________________
# title: ASSIGNMENT 07.1, Build Triggers in SQLite
# author: Shubha, Adithya
# date: 2024-10-22
# ______________

# Install program required packages =============================
# Installs RSQLite to connect to SqlLite.
# Referred from: http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Install_Packages_on_Demand
installRequiredPackages <- function() {
  packages <- c("RSQLite")

  # Install packages that are not installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }


  # Load all packages
  invisible(lapply(packages, library, character.only = TRUE))
}

# Create a DB Connection ============================
# Creates a DB connection with the given dbName
# @param: dbName - Name of the db to connect to
# referredFrom: http://artificium.us/lessons/06.r/l-6-301-sqlite-from-r/l-6-301.html#Connect_to_Database
createDBConnection <- function(dbName) {
  return(dbConnect(RSQLite::SQLite(), dbName))
}


# Alter Employee Table ============================
# Alters the Employee table to add TotalSold column
# @param: dbCon - DB connection object
alterEmployeeTable <- function(dbCon) {
  # tableInfo referredFrom: https://tableplus.com/blog/2018/04/sqlite-show-table-description.html
  result <- dbGetQuery(dbCon, "PRAGMA table_info(Employees);")
  # If total sold is an existing column, drop it
  if ("TotalSold" %in% result$name) {
    dbExecute(dbCon, "ALTER TABLE Employees
    DROP COLUMN TotalSold;")
  }
  # Add TotalSold column
  dbExecute(dbCon,
            "
    ALTER TABLE Employees
    ADD COLUMN TotalSold NUMERIC(10, 2);
")
}

# Trigger on insert into order details ============================
# Creates a trigger to update the TotalSold column in Employees table
# when a new row is inserted into OrderDetails table
# @param: dbCon - DB connection object
# coalesce referredFrom: https://www.w3schools.com/sql/func_sqlserver_coalesce.asp
afterInsertTiggerOnOrderDetails <- function(dbCon) {
  # Dropping trigger if it exists
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS UpdateEmployeeTotalSoldOnInsert;")
  # Creating trigger
  dbExecute(
    dbCon,
    "
            CREATE TRIGGER UpdateEmployeeTotalSoldOnInsert
            AFTER INSERT ON OrderDetails
            FOR EACH ROW
            BEGIN
                UPDATE Employees
            SET TotalSold = (
                SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
                FROM Orders o
                JOIN OrderDetails od ON o.OrderID = od.OrderID
                JOIN Products p ON od.ProductID = p.ProductID
                WHERE o.EmployeeID = Employees.EmployeeID
            )
            WHERE EmployeeID = (
                SELECT o.EmployeeID
                FROM Orders o
                WHERE o.OrderID = NEW.OrderID
            );
END;"
  )
}

# Trigger on update into order details ============================
# Creates a trigger to update the TotalSold column in Employees table
# when a row is updated in OrderDetails table
# @param: dbCon - DB connection object
afterUpdateTiggerOnOrderDetails <- function(dbCon) {
  # Dropping trigger if it exists
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS UpdateEmployeeTotalSoldOnUpdate;")
  dbExecute(
    dbCon,
    "
            CREATE TRIGGER UpdateEmployeeTotalSoldOnUpdate
            AFTER UPDATE ON OrderDetails
            FOR EACH ROW
            BEGIN
                UPDATE Employees
            SET TotalSold = (
                SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
                FROM Orders o
                JOIN OrderDetails od ON o.OrderID = od.OrderID
                JOIN Products p ON od.ProductID = p.ProductID
                WHERE o.EmployeeID = Employees.EmployeeID
            )
            WHERE EmployeeID = (
                SELECT o.EmployeeID
                FROM Orders o
                WHERE o.OrderID = NEW.OrderID
            );
END;"
  )
}

# Trigger on delete from order details ============================
# Creates a trigger to update the TotalSold column in Employees table
# when a row is deleted from OrderDetails table
# @param: dbCon - DB connection object
afterDeleteTiggerOnOrderDetails <- function(dbCon) {
  # Dropping trigger if it exists
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS UpdateEmployeeTotalSoldOnDelete;")
  dbExecute(
    dbCon,
    "
            CREATE TRIGGER UpdateEmployeeTotalSoldOnDelete
            AFTER DELETE ON OrderDetails
            FOR EACH ROW
            BEGIN
                UPDATE Employees
            SET TotalSold = (
                SELECT COALESCE(SUM(od.Quantity * p.Price), 0)
                FROM Orders o
                JOIN OrderDetails od ON o.OrderID = od.OrderID
                JOIN Products p ON od.ProductID = p.ProductID
                WHERE o.EmployeeID = Employees.EmployeeID
            )
            WHERE EmployeeID = (
                SELECT o.EmployeeID
                FROM Orders o
                WHERE o.OrderID = OLD.OrderID
            );
END;"
  )
}

# Update Employee Table to get Total Sold ============================
# Updates the TotalSold column in Employees table
# to contain the total amount (in terms of revenue generated)
# that the employee sold to all customers.
# @param: dbCon - DB connection object
updateEmployeeTableToGetTotalSold <- function(dbCon) {
  dbExecute(
    dbCon,
    "
    UPDATE Employees
    SET TotalSold = (
        SELECT COALESCE(SUM(OD.Quantity * P.Price), 0)
        FROM Orders O
        JOIN OrderDetails OD ON O.OrderID = OD.OrderID
        JOIN Products P ON OD.ProductID = P.ProductID
        WHERE O.EmployeeID = Employees.EmployeeID
    );
"
  )
}


# Validations on insert trigger ============================
# Validates the insert trigger by inserting a row into OrderDetails
# and checking if the TotalSold column in Employees table is updated
# @param: dbCon - DB connection object
validateInsertTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating insert trigger\n")
    # Sample ids which the tests work on
    employeeIdToTest <- 1
    productIdToTest <- 1
    orderIdToTest <- 10258
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    # Inserting a row with 1 quantity in OrderDetails
    dbExecute(dbCon, sprintf("INSERT INTO OrderDetails (OrderID, ProductID, Quantity) VALUES (%s, %s, 1);", orderIdToTest, productIdToTest))

    employeeAfterInsert <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after inserting 1 quantity for id %s: %s\n", employeeIdToTest, employeeAfterInsert$TotalSold))
    # Checking with productPrice*1 since 1 quantity is inserted
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterInsert$TotalSold - employee$TotalSold))
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of insert trigger: ", e$message, "\n")
  })
}

# Validations on update trigger ============================
# Validates the update trigger by updating a row in OrderDetails
# and checking if the TotalSold column in Employees table is updated
# @param: dbCon - DB connection object
validateUpdateTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating update trigger\n")
    employeeIdToTest <- 1
    productIdToTest <- 1
    orderIdToTest <- 10258
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    # Seting quanitty to 2 which was previously 1 for the row with orderId and productId
    dbExecute(dbCon, sprintf("UPDATE OrderDetails SET Quantity = 2 WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))

    employeeAfterUpdate <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after updating quantity to 2 for id %s: %s\n", employeeIdToTest, employeeAfterUpdate$TotalSold))
    # Checking with productPrice*1 since 1 additional quantity was updated
    # previously 1, currently 2
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterUpdate$TotalSold - employee$TotalSold))
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of update trigger: ", e$message, "\n")
  })
}

# Validations on delete trigger ============================
# Validates the delete trigger by deleting a row in OrderDetails
# and checking if the TotalSold column in Employees table is updated
# @param: dbCon - DB connection object
validateDeleteTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating delete trigger\n")
    employeeIdToTest <- 1
    productIdToTest <- 1
    orderIdToTest <- 10258
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee for id %s: %s\n", employeeIdToTest, employee$TotalSold))
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of product for id %s: %s\n", productIdToTest, productPrice$Price))
    dbExecute(dbCon, sprintf("DELETE FROM OrderDetails WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))

    employeeAfterDelete <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold of employee after deleting quantity for id %s: %s\n", employeeIdToTest, employeeAfterDelete$TotalSold))
    # Checking with productPrice*2 since 2 quantity was previously sold which is deleted.
    cat(sprintf("Expected decrease in price: %s, actual:%s\n", productPrice$Price * 2, employee$TotalSold - employeeAfterDelete$TotalSold))
    cat("________________________________________________\n")
  }, error = function(e) {
    cat("Error during validation of delete trigger: ", e$message, "\n")
  })
}

# Perform Validations ============================
# Performs validations on the triggers
# @param: dbCon - DB connection object
performValidations <- function(dbCon) {
  validateInsertTrigger(dbCon)
  validateUpdateTrigger(dbCon)
  validateDeleteTrigger(dbCon)
}

# Main function ============================
main <- function() {
  installRequiredPackages()
  dbCon <- createDBConnection('OrdersDB.sqlitedb.db')
  # Enabling foreign key constraint,
  # ref: https://northeastern.instructure.com/courses/192346/assignments/2351515?module_item_id=10707674
  dbExecute(dbCon, "PRAGMA foreign_keys = ON")

  alterEmployeeTable(dbCon)
  updateEmployeeTableToGetTotalSold(dbCon)
  afterInsertTiggerOnOrderDetails(dbCon)
  afterUpdateTiggerOnOrderDetails(dbCon)
  afterDeleteTiggerOnOrderDetails(dbCon)
  performValidations(dbCon)
  dbDisconnect(dbCon)
}

main()