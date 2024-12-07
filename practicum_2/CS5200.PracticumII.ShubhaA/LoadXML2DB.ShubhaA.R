# -------------------------------------------
# title: "Create Analytics Database"
# subtitle: CS5200 / Practicum II Part 1
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


# Creates a DB connection with the given dbName
# @param: dbName - Name of the db to connect to
# referredFrom: http://artificium.us/lessons/06.r/l-6-301-sqlite-from-r/l-6-301.html#Connect_to_Database
createDBConnection <- function(dbName) {
  return(dbConnect(RSQLite::SQLite(), dbName))
}

# All the global variables defined here
productsTableName <- "products"
repsTableName <- "reps"
customersTableName <- "customers"
salesTableName <- "sales"

# drop the given table if it exists.
# @param: table - table to drop
# @param: dbCon - database connection to connect to
dropTableIfExist <- function(dbCon, table) {
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
    # referredFrom: https://www.rdocumentation.org/packages/base/versions/3.6.2/topics/grep
    if (!grepl("^sql", table)) {
      dropTableIfExist(dbCon, table)
    }
  }
}


# Creates products table in database
# Products table has productID as generated key, productName and unitCost
# @param: dbCon - database connection to connect to
createProductsTable <- function(dbCon) {
  query <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s (
    productID INTEGER PRIMARY KEY AUTOINCREMENT,
    productName VARCHAR(255) NOT NULL,
    unitCost DECIMAL(10,2) NOT NULL
);",
    productsTableName
  )
  dbExecute(dbCon, query)
}

# Creates reps table in database
# Reps table has repID as primary key, repFN, repLN, repTR, repPH, repCm, repHireDate.
# Assumption that repID is always defined and is unique.
# @param: dbCon - database connection to connect to
createRepsTable <- function(dbCon) {
  query <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s (
    repID INTEGER PRIMARY KEY,
    repFN VARCHAR(255) NOT NULL,
    repLN VARCHAR(255) NOT NULL,
    repTR VARCHAR(255) NOT NULL,
    repPH VARCHAR(255) NOT NULL,
    repCm DECIMAL(10,2) NOT NULL,
    repHireDate DATE
);",
    repsTableName
  )
  dbExecute(dbCon, query)
}

# Creates customers table in database.
# Customer table has customerId as generated key, customerName and country
# @param: dbCon - database connection to connect to
createCustomersTable <- function(dbCon) {
  query <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s (
    customerID INTEGER PRIMARY KEY AUTOINCREMENT,
    customerName VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL
);",
    customersTableName
  )
  dbExecute(dbCon, query)
}

# Creates sales table in database
# Sales table has salesID as generated key, date, customerID, productID, qty, repID, data_source.
# data_source is a comibination of txnID and fileName.
# @param: dbCon - database connection to connect to
createSalesTable <- function(dbCon, tableName) {
  query <- sprintf(
    "CREATE TABLE IF NOT EXISTS %s (
    salesID INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE,
    customerID INTEGER NOT NULL,
    productID INTEGER NOT NULL,
    qty INTEGER NOT NULL,
    repID INTEGER NOT NULL,
    data_source VARCHAR(255),
    FOREIGN KEY (customerID) REFERENCES %s(customerID),
    FOREIGN KEY (productID) REFERENCES %s(productID),
    FOREIGN KEY (repID) REFERENCES %s(repID)
);",
    tableName,
    customersTableName,
    productsTableName,
    repsTableName
  )
  dbExecute(dbCon, query)
}


# Creates all the tables required for the program
# @param: dbCon - database connection to connect to
createAllTables <- function(dbCon) {
  createProductsTable(dbCon)
  createRepsTable(dbCon)
  createCustomersTable(dbCon)
  createSalesTable(dbCon, salesTableName)
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


# Insert into reps table
# @param: dbCon - database connection to connect to
# @param: dataFrame - data frame to insert into the database
# @param: batchSize - batch size to insert into the database
insertDataIntoRepsTable <- function(dbCon, dataFrame, batchSize) {
  repData <- dataFrame
  if (nrow(repData) > 0) {
    query <- sprintf(
      "INSERT INTO %s (repID,repFN, repLN, repTR, repPH, repCm, repHireDate) VALUES",
      repsTableName
    )
    # referredFrom: https://ademos.people.uic.edu/Chapter4.html
    values <- apply(repData, 1, function(row) {
      repHireDate <- if (row["repHireDate"] == "Unknown")
        "NULL"
      else
        sprintf("'%s'", row["repHireDate"])
      sprintf(
        "(%s, '%s', '%s', '%s', '%s', %s, %s)",
        row["repID"],
        # Replacing single quote with double quote to avoid sql errors.
        gsub("'", "''", row["repFN"]),
        gsub("'", "''", row["repLN"]),
        gsub("'", "''", row["repTR"]),
        gsub("'", "''", row["repPh"]),
        row["repCm"],
        repHireDate
      )
    })
    insertInBatches(dbCon, batchSize, query, values)
  }
}


# Insert into products table
# @param: dbCon - database connection to connect to
# @param: dataFrame - data frame to insert into the database
# @param: batchSize - batch size to insert into the database
insertDataIntoProductsTable <- function(dbCon, dataFrame, batchSize) {
  # Getting combination of unique product and unit cost
  productData <- unique(dataFrame[c("prod", "unitcost")])
  if (nrow(productData) > 0) {
    query <- sprintf("INSERT INTO %s (productName,unitcost) VALUES",
                     productsTableName)
    values <- apply(productData, 1, function(row) {
      sprintf("('%s',%s)",
              # Replacing single quote with double quote to avoid sql errors.
              gsub("'", "''", row["prod"]), 
              row["unitcost"])
    })
    insertInBatches(dbCon, batchSize, query, values)
  }
}

# Insert into customers table
# @param: dbCon - database connection to connect to
# @param: dataFrame - data frame to insert into the database
# @param: batchSize - batch size to insert into the database
insertDataIntoCustomersTable <- function(dbCon, dataFrame, batchSize) {
  # Getting combination of unique customer and country
  customerData <- unique(dataFrame[c("cust", "country")])
  if (nrow(customerData) > 0) {
    query <- sprintf("INSERT INTO %s (customerName,country) VALUES",
                     customersTableName)
    values <- apply(customerData, 1, function(row) {
      sprintf("('%s','%s')",
              # Replacing single quote with double quote to avoid sql errors.
              gsub("'", "''", row["cust"]),
              gsub("'", "''", row["country"]))
    })
    insertInBatches(dbCon, batchSize, query, values)
  }
}

# Gets the customer id from given customer name and country
# @param: customerMapping - customer mapping data frame
# @param: customerName - customer name
# @param: country - customer state
getCustomerId <- function(customerMapping, customerName, country) {
  # Getting matching row by equating customerName and country
  matchingRow <- customerMapping[customerMapping$customerName == customerName &
                                   customerMapping$country == country, ]
  return(matchingRow$customerID[1])
}

# Gets the product id from given product name and unit cost
# @param: productMapping - product mapping data frame
# @param: productName - product name
# @param: unitCost - product unit cost
getProductId <- function(productMapping, productName, unitCost) {
  unitCost <- as.numeric(unitCost)
  matchingRow <- productMapping[productMapping$productName == productName
                                &
                                  productMapping$unitCost == unitCost, ]
  return(matchingRow$productID[1])
}


# Insert into sales table
# @param: dbCon - database connection to connect to
# @param: dataFrame - data frame to insert into the database
# @param: batchSize - batch size to insert into the database
insertDataIntoSalesTable <- function(dbCon, dataFrame, batchSize) {
  customerMapping <-
    dbGetQuery(
      dbCon,
      sprintf(
        "SELECT customerID, customerName, country FROM %s",
        customersTableName
      )
    )
  
  productMapping <-
    dbGetQuery(
      dbCon,
      sprintf(
        "SELECT productID, productName, unitCost FROM %s",
        productsTableName
      )
    )
  
  salesData <- dataFrame
  if (nrow(salesData) > 0) {
    query <- sprintf(
      "INSERT INTO %s (date,customerID, productID, qty, repID,data_source) VALUES",
      salesTableName
    )
    values <- apply(salesData, 1, function(row) {
      date <- if (row["date"] == "Unknown")
        "NULL"
      else
        sprintf("'%s'", row["date"])
      
      dataSource <- sprintf("'%s_%s'", row["txnID"], row["fileName"])
      sprintf(
        "(%s,%s,%s,%s,%s,%s)",
        date,
        getCustomerId(
          customerMapping,
          gsub("'", "''", row["cust"]),
          gsub("'", "''", row["country"])
        ),
        getProductId(productMapping, gsub("'", "''", row["prod"]), row["unitcost"]),
        row["qty"],
        row["repID"],
        dataSource
      )
    })
    insertInBatches(dbCon, batchSize, query, values)
  }
}


# Function to replace empty values with sentinel values
# @param: columnValue - current column value
# @param: sentinelValue - sentinel value to replace.
replaceEmptyValues <- function(columnValue, sentinelValue) {
  # if column value is NA or empty or N/A, replace with sentinel value
  ifelse(
    is.na(columnValue) |
      columnValue == "" | columnValue == "N/A",
    sentinelValue,
    columnValue
  )
}

# Function to clean up the reps data
# @param: dataFrame - data frame to clean up
cleanUpRepsData <- function(dataFrame) {
  # Removing rows with NA in repID
  dataFrame <- dataFrame[!is.na(dataFrame$repID), ]
  # Defining sentinel values for each column.
  sentinelValues <-  list(
    repFN = "Unknown First Name",
    repLN = "Unknown Last Name",
    repTR = "Unknown Terrority",
    repPh = "Unknown Phone Number",
    repCm = 0,
    repHireDate = "Unknown"
  )
  # Replacing empty values with sentinel values
  dataFrame[names(sentinelValues)] <- Map(replaceEmptyValues, dataFrame[names(sentinelValues)], sentinelValues)
  
  # Converting from string to date data type if not Unknown
  # date from source is of format "Mon DD YYYY"
  dataFrame$repHireDate <- ifelse(
    dataFrame$repHireDate == "Unknown",
    dataFrame$repHireDate,
    as.character(as.Date(dataFrame$repHireDate, format = "%b %d %Y"))
  )
  
  return(dataFrame)
}

# Function to clean up the columns in sales data
# @param: dataFrame - data frame to clean up
cleanUpSalesData <- function(dataFrame) {
  # Removing rows with NA in txnID
  dataFrame <- dataFrame[!is.na(dataFrame$txnID), ]
  
  # Defining sentinel values for each column.
  sentinelValues <-  list(
    cust = "Unknown Customer",
    prod = "Unknown Product",
    qty = 0,
    unitcost = 0,
    country = "Unknown Country",
    date = "Unknown"
  )
  
  # Replacing empty values with sentinel values
  dataFrame[names(sentinelValues)] <- Map(replaceEmptyValues, dataFrame[names(sentinelValues)], sentinelValues)
  
  # Converting from string to date data type
  dataFrame$date <- ifelse(dataFrame$date == "Unknown",
                           dataFrame$date,
                           as.character(as.Date(dataFrame$date, format = "%m/%d/%Y")))
  return(dataFrame)
}


# Function to extract reps data and insert into reps table
# @param: dbCon - database connection to connect to
# @param: directory - directory to read reps data from
extractRepsData <- function(dbCon, directory) {
  batchSize <- 100
  repsFile <- list.files(path = directory, pattern = "^pharmaReps.*csv$")
  repsData <- data.frame()
  for (file in repsFile) {
    newRepsData <- read.csv(file.path(directory, file))
    repsData <- rbind(repsData, newRepsData)
  }
  repsData <- cleanUpRepsData(repsData)
  insertDataIntoRepsTable(dbCon, repsData, batchSize)
}

# Function to extract sales data and insert into tables
# @param: dbCon - database connection to connect to
# @param: directory - directory to read sales data from
extractSalesData <- function(dbCon, directory) {
  salesFile <- list.files(path = directory, pattern = "^pharmaSalesTxn.*csv$")
  batchSize <- 100
  salesData <- data.frame()
  for (file in salesFile) {
    newSalesData <- read.csv(file.path(directory, file))
    newSalesData$fileName <- file
    salesData <- rbind(salesData, newSalesData)
  }
  salesData <- cleanUpSalesData(salesData)
  insertDataIntoCustomersTable(dbCon, salesData, batchSize)
  insertDataIntoProductsTable(dbCon, salesData, batchSize)
  insertDataIntoSalesTable(dbCon, salesData, batchSize)
}

insertDataIntoSalesPartition <- function(dbCon, salesData, tableName, batchSize) {
  if (nrow(salesData) > 0) {
    query <- sprintf(
      "INSERT INTO %s (date,customerID, productID, qty, repID,data_source) VALUES",
      tableName
    )
    values <- apply(salesData, 1, function(row) {
      sprintf("('%s',%s,%s,%s,%s,'%s')",
              row["date"],
              row["customerID"],
              row["productID"],
              row["qty"],
              row["repID"],
              row["data_source"])
    })
    insertInBatches(dbCon, batchSize, query, values)
  }
  
}

# Partition sales data by year
# @param: dbCon - database connection to connect to
partitionSalesData <- function(dbCon) {
  minMaxYear <- getMinMaxYear(dbCon)
  minYear <- minMaxYear$minYear
  maxYear <- minMaxYear$maxYear
  for (year in minYear:maxYear) {
    query <- sprintf("SELECT * FROM %s WHERE strftime('%%Y', date) = '%s'",
                     salesTableName,
                     year)
    salesData <- dbGetQuery(dbCon, query)
    tableName <- sprintf("%s_%s", salesTableName, year)
    createSalesTable(dbCon, tableName)
    insertDataIntoSalesPartition(dbCon, salesData, tableName, 100)
  }
  dropTableIfExist(dbCon, salesTableName)
}

# Get the min and max year from the sales table
# @param: dbCon - database connection to connect to
getMinMaxYear <- function(dbCon) {
  query <- sprintf(
    "SELECT MIN(strftime('%%Y', date)) as minYear, MAX(strftime('%%Y', date)) as maxYear FROM %s",
    salesTableName
  )
  return(dbGetQuery(dbCon, query))
}

# Cleans the environment before beginning the execution
# referredFrom:https://northeastern.instructure.com/courses/192346/assignments/2351524
cleanEnv <- function() {
  rm(list = ls())
}

# Main function to run the program
main <- function() {
  cleanEnv()
  installRequiredPackages()
  dbCon <- createDBConnection("pharmacy.db")
  dropAllExistingTable(dbCon)
  dbExecute(dbCon,"PRAGMA foreign_keys = ON")
  createAllTables(dbCon)
  dataLoc <- "csv-data"
  extractRepsData(dbCon, dataLoc)
  extractSalesData(dbCon, dataLoc)
  partitionSalesData(dbCon)
  dbDisconnect(dbCon)
}

# Run the main function
main()
