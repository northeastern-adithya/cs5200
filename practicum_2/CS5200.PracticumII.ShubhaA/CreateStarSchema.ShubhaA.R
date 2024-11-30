# -------------------------------------------
# title: "Create Star/Snowflake Schema"
# subtitle: CS5200 / Practicum II Part 2
# author: "Shubha, Adithya"
# email: "shubha.a@northeastern.edu"
# date: "Fall 2024"
# --------------------------------------------

# Install program required packages =============================
# Installs RSQLite and DBI to connect to SqlLite.
# Referred from: http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Install_Packages_on_Demand
installRequiredPackages <- function() {
  packages <- c("RSQLite", "DBI")
  
  # Install packages that are not installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
  
  
  # Load all packages
  lapply(packages, library, character.only = TRUE)
}

# Function to connect to the mysql database hosted online
# @return: database connection
# referredFrom: http://artificium.us/lessons/06.r/l-6-301-sqlite-from-r/l-6-301.html#Connect_to_Database
connectToMySqlDatabase <- function() {
  # DB credentials
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_LfgR_-4_x2xF-vv7_Bg"
  dbHost <- "practicum-1-cs-5200.b.aivencloud.com"
  dbPort <- 22694
  
  
  return (
    dbConnect(
      RMySQL::MySQL(),
      user = dbUser,
      password = dbPassword,
      dbname = dbName,
      host = dbHost,
      port = dbPort
    )
  )
}

# Creates a DB connection to local sqlite with the given dbName
# @param: dbName - Name of the db to connect to
# referredFrom: http://artificium.us/lessons/06.r/l-6-301-sqlite-from-r/l-6-301.html#Connect_to_Database
createSQLLiteDBConnection <- function(dbName) {
  return(dbConnect(RSQLite::SQLite(), dbName))
}

# drop the given table if it exists.
# @param: table - table to drop
# @param: dbCon - database connection to connect to
dropTableIfExist <- function(dbCon,table) {
  dbExecute(dbCon, sprintf("DROP TABLE IF EXISTS %s", table))
}


# Returns the list of all existing tables in the database
# @param: dbCon - database connection to connect to
getAllExistingTables <- function(dbCon) {
  return(dbListTables(dbCon))
}

# Drops all the existing tables of program from a given db connection.
# @param: dbCon - database connection to connect to
dropAllExistingTable <- function(dbCon) {
  tables <- getAllExistingTables(dbCon)
  for (table in tables) {
      dropTableIfExist(dbCon,table)
  }
}


# All the global variables defined here
productsFactsTable <- "product_facts"

# Create the product facts table
# Products fact table is created with the one big table approach since it contains 
# only product name, unitcost as additional dimensions in the source product table. 
# In order to prevent joins and make the queries faster,
# we are creating a single table with all the required columns.
# @param: dbCon - database connection to connect to
createProductFactsTable <- function(dbCon){
  query <- sprintf("CREATE TABLE IF NOT EXISTS %s (
    productId INT,
    productName VARCHAR(255),
    country VARCHAR(255),
    year INT,
    month INT,
    quarter INT,
    totalRevenue DECIMAL(18,2),
    totalUnitsSold INT,
    PRIMARY KEY (productId, country, year, month)
);", productsFactsTable)
  dbExecute(dbCon, query)
}

# Function to get names of all tables that start with 'sales_'
# @param: dbCon - database connection to connect to
getSalesTables <- function(dbCon) {
  allTables <- dbListTables(dbCon)
  salesTables <- grep("^sales_", allTables, value = TRUE)
  return(salesTables)
}

# Insert into database in batches
# @param: dbCon - database connection to connect to
# @param: batchSize - batch size to insert into the database
# @param: initialQuery - insert query with table and column names
# @param: values - values to insert into the database
insertInBatches <- function(dbCon, batchSize, initialQuery, values) {
  numBatches <- ceiling(length(values) / batchSize)
  for (i in 1:numBatches) {
    startIdx <- (i - 1) * batchSize + 1
    endIdx <- min(i * batchSize, length(values))
    batchValues <- values[startIdx:endIdx]
    completeQuery <- paste(initialQuery, paste(batchValues, collapse = ","))
    dbExecute(dbCon, completeQuery)
  }
}

# Insert the required data into the product facts table
# @param: dbCon - database connection to connect to
# @param: data - data to insert into the database
insertIntoProductFactsTable <- function(dbCon, data) {
  if (nrow(data) > 0) {
    query <- sprintf(
      "INSERT INTO %s (productId,productName, country, year, month, quarter, totalRevenue,totalUnitsSold) VALUES",
      productsFactsTable
    )

    # referredFrom: https://ademos.people.uic.edu/Chapter4.html
    values <- apply(data, 1, function(row) {
      sprintf(
        "(%s, '%s', '%s', %s, %s, %s, %s,%s)",
        row["productID"],
        row["productName"],
        row["country"],
        row["year"],
        row["month"],
        row["quarter"],
        row["totalRevenue"],
        row["totalUnitsSold"]
      )
    })
    insertInBatches(dbCon, 100, query, values)
  }
}

# Reads data from the local database to populate the product facts table
# @param: dbCon - database connection to connect to
# @param: tableName - table name to read from
readFromLocalDBToPopulateProductFactsTable <- function(dbCon,tableName) {
  query <- sprintf(
    "SELECT
    s.productId,
    p.productName,
    c.country,
    strftime('%%Y', s.date) AS year,
    strftime('%%m', s.date) AS month,
    CEIL(strftime('%%m', s.date) / 3.0) AS quarter,
    SUM(s.qty * p.unitcost) AS totalRevenue,
    SUM(s.qty) AS totalUnitsSold
FROM
    %s s
JOIN
    products p ON s.productId = p.productId
JOIN
    customers c ON s.customerID = c.customerId
GROUP BY
    s.productId,
    c.country,
    year,
    month,
    quarter",
    tableName
  )
  return(dbGetQuery(dbCon, query))
}


# Reads and inserts data into the product facts table
# Iterates over all the sales tables and reads data from the local database 
# to populate the product facts table
# @param: mySqlCon - database connection to connect to mysql
# @param: sqliteCon - database connection to connect to sqlite
readAndInsertForProductFactsTable <- function(mySqlCon, sqliteCon) {
  createProductFactsTable(mySqlCon)
  salesTables <- getSalesTables(sqliteCon)
  for (table in salesTables) {
    data <- readFromLocalDBToPopulateProductFactsTable(sqliteCon, table)
    insertIntoProductFactsTable(mySqlCon, data)
  }
}


# Cleans the environment before beginning the execution
# referredFrom:https://northeastern.instructure.com/courses/192346/assignments/2351524
cleanEnv <- function() {
  rm(list = ls())
}

# Main function to run the program
main <- function() {
  installRequiredPackages()
  cleanEnv()
  mySqlCon <- connectToMySqlDatabase()
  sqliteCon <- createSQLLiteDBConnection("pharmacy.db")
  dropAllExistingTable(mySqlCon)
  readAndInsertForProductFactsTable(mySqlCon, sqliteCon)
  dbDisconnect(mySqlCon)
  dbDisconnect(sqliteCon)
}

main()





