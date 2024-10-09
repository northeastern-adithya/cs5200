# ________________
# title: ASSIGNMENT 05.1: Implement a Relational Database
# author: Shubha, Adithya
# date: 2024-10-08
# erd: https://lucid.app/lucidchart/16e62b28-eecc-437e-8653-4f07ad10e8e1/edit?viewport_loc=-177%2C-840%2C1685%2C853%2C0_0&invitationId=inv_a98f0543-1e9c-461b-b26c-a1ab530cc017
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
  return (dbConnect(RSQLite::SQLite(), dbName))
}

# SQL statement for creating difficulty table if not present ====================
# Creates the following columns for difficulty table:
# id, integer as PK
# level, varchar
sqlToCreateDifficulty <- function() {
  return ('CREATE TABLE IF NOT EXISTS difficulty(
    id INTEGER PRIMARY KEY,
    level VARCHAR NOT NULL
);')
}

# SQL statement to insert data for difficulty =======================
# SQL statement to insert 4 rows to difficulty.
sqlToInsertIntoDifficulty <- function() {
  return (
    "INSERT INTO difficulty (id, level) VALUES
          (1, 'beginner'),
          (2, 'intermediate'),
          (3, 'advanced'),
          (4, 'graduate');"
  )
  
}

# SQL statement for creating module table if not present ====================
# Creates the following columns for module table:
# id, varchar as PK
# title, varchar
# length_in_minutes, integer
# difficulty_id, integer FK to id of difficult table
sqlToCreateModule <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS module (
    id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    length_in_minutes INTEGER NOT NULL,
    difficulty_id INTEGER NOT NULL,
    FOREIGN KEY (difficulty_id) REFERENCES difficulty(id)
);'
  )
}


# SQL statement to insert data for module =======================
# SQL statement to insert 3 rows to module
sqlToInsertIntoModule <- function() {
  return (
    "INSERT INTO module (id, title, length_in_minutes, difficulty_id) VALUES
        ('M1', 'DATABASE SYSTEMS', 60, 1),
        ('M2', 'DATA PROCESSING & R', 45, 3),
        ('M3', 'DATA MODELING', 30, 2);"
  )
}

# SQL statement for creating lesson table if not present ====================
# Creates the following columns for lesson table:
# id, integer as PK
# category, integer
# number, integer
# title, varchar
sqlToCreateLesson <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson (
    id INTEGER PRIMARY KEY,
    category INTEGER NOT NULL,
    number INTEGER NOT NULL,
    title VARCHAR NOT NULL
);'
  )
}


# SQL statement to insert data for lesson =======================
# SQL statement to insert 5 rows to lesson
sqlToInsertIntoLesson <- function() {
  return (
    "INSERT INTO lesson (id, category, number, title) VALUES
      (1, 1, 1, 'The Need for databases'),
      (2, 1, 2, 'R Ecosystem'),
      (3, 2, 1, 'Working with R'),
      (4, 2, 2, 'R for programmers'),
      (5, 3, 1, 'Relationship Types');"
  )
}

# SQL statement for creating lesson module relation table if not present ====================
# Creates the following columns for lesson module relation table
# id, integer as PK
# lesson_id, integer as FK to id of lesson
# module_id, integer as FK to id of module
sqlToCreateLessonModuleRelation <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson_module_relation (
    id INTEGER PRIMARY KEY,
    lesson_id INTEGER NOT NULL,
    module_id TEXT NOT NULL,
    FOREIGN KEY (lesson_id) REFERENCES lesson(id),
    FOREIGN KEY (module_id) REFERENCES module(id)
);'
  )
}

# SQL statement to insert data for lesson module relation =======================
# SQL statement to insert 6 rows to lesson module relation
sqlToInsertIntoLessonModuleRelation <- function() {
  return (
    "INSERT INTO lesson_module_relation (id, lesson_id, module_id) VALUES
          (1,1,'M1'),
          (2,2,'M2'),
          (3,3,'M2'),
          (4,4,'M2'),
          (5,1,'M2'),
          (6,5,'M3');"
  )
}


# SQL statement for creating lesson prerequisite table if not present ====================
# Creates the following columns for lesson prerequisite table
# id, integer as PK
# lesson_id, integer as FK to id of lesson
# lesson_prerequisite_id, integer as FK to id of lesson
sqlToCreateLessonPrerequisite <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson_prerequisite (
    id INTEGER PRIMARY KEY,
    lesson_id INTEGER NOT NULL,
    lesson_prerequisite_id INTEGER NOT NULL,
    FOREIGN KEY (lesson_id) REFERENCES lesson(id),
    FOREIGN KEY (lesson_prerequisite_id) REFERENCES lesson(id)
);
'
  )
}

# SQL statement to insert data for lesson prerequisite =======================
# SQL statement to insert 4 rows to lesson prerequisite
sqlToInsertIntoLessonPrerequisite <- function() {
  return (
    "INSERT INTO lesson_prerequisite (id, lesson_id, lesson_prerequisite_id) VALUES
            (1, 2, 1),
            (2, 3, 1),
            (3, 3, 2),
            (4, 4, 1);"
  )
}

# Drop table sql query ==================
# SQL query to drop the given table if it exists.
# @param: table - table to drop
dropTableIfExist <- function(table) {
  return (sprintf('drop table if exists %s', table))
}

# Drop all existing table ================
# Drops all the existing tables from a given db connection.
# @param: dbCon - db connection to connect to
dropAllExistingTable <- function(dbCon) {
  tables <- dbListTables(dbCon)
  for (table in tables) {
    dbExecute(dbCon, dropTableIfExist(table))
  }
}

# Create defined tables ================
# Following function creates difficulty, module,lesson,
# lesson_module_relation, lesson_prerequisite tables.
createAllTables <- function(dbCon) {
  dbExecute(dbCon, sqlToCreateDifficulty())
  dbExecute(dbCon, sqlToCreateModule())
  dbExecute(dbCon, sqlToCreateLesson())
  dbExecute(dbCon, sqlToCreateLessonModuleRelation())
  dbExecute(dbCon, sqlToCreateLessonPrerequisite())
}

# Insert data into tables ================
# Inserts data into difficulty, module,lesson,
# lesson_module_relation, lesson_prerequisite tables.
insertIntoAllTables <- function(dbCon)
{
  dbExecute(dbCon, sqlToInsertIntoDifficulty())
  dbExecute(dbCon, sqlToInsertIntoModule())
  dbExecute(dbCon, sqlToInsertIntoLesson())
  dbExecute(dbCon, sqlToInsertIntoLessonModuleRelation())
  dbExecute(dbCon, sqlToInsertIntoLessonPrerequisite())
}


# Validate data after insertion =====================
# Validates if the data inserted is equal to the expectation by
# printing the actual values.
validatedDataInsertedToTables <- function(dbCon) {
  cat("Validating data after insertion______________________\n")
  
  difficulty = dbReadTable(dbCon, 'difficulty')
  cat(sprintf(
    "Expected 'difficult' data set size to be 4, actual: %d\n",
    nrow(difficulty)
  ))
  
  module = dbReadTable(dbCon, 'module')
  cat(sprintf(
    "Expected 'module' data set size to be 3, actual: %d\n",
    nrow(module)
  ))
  
  lesson = dbReadTable(dbCon, 'lesson')
  cat(sprintf(
    "Expected 'lesson' data set size to be 5, actual: %d\n",
    nrow(lesson)
  ))
  
  lessonModuleRelation = dbReadTable(dbCon, 'lesson_module_relation')
  cat(sprintf(
    "Expected 'lesson_module_relation' data set size to be 6, actual: %d\n",
    nrow(lessonModuleRelation)
  ))
  
  lessonPrerequisite = dbReadTable(dbCon, 'lesson_prerequisite')
  cat(sprintf(
    "Expected 'lesson_prerequisite' data set size to be 4, actual: %d\n",
    nrow(lessonPrerequisite)
  ))
}

# SQL query to get title of all modules present ==================
getTitleOfModulesQuery <- function() {
  return("SELECT title FROM module;")
}

# Gets the module titles present in the database ==================
# Prints the module titles in readable format.
getModuleTitles <- function(dbCon) {
  cat("The names of the modules are as follows:\n")
  module <- dbGetQuery(dbCon, getTitleOfModulesQuery())
  cat(paste(module$title, collapse = ", "), "\n", sep = "")
}

# Query to get the count of lessons having a particular prerequisite============
# @param: prerequisite id of the lesson beign a prerequisite.
queryGetCountOfLessonsHavingParticularPrerequisite <- function(prerequisite) {
  return(
    sprintf(
      "SELECT count(*)
                 FROM lesson_prerequisite
                 WHERE lesson_prerequisite_id = %s;",
      prerequisite
    )
  )
}

# Gets number of lessons having prerequisite as lesson one ==================
# @param: dbCon - connection of the db
getCountOfLessonsHavingLessonOneAsPrerequisite <- function(dbCon) {
  countOfLessonOneAsPrerequisite <- dbGetQuery(dbCon,queryGetCountOfLessonsHavingParticularPrerequisite(1))
  cat(
    sprintf(
      "The count of lessons having lesson one as prerequisite: %d\n",
      countOfLessonOneAsPrerequisite[[1]]
    )
  )
}

# SQL query to get all available difficultly levels ==================
queryOfDifficultyLevel <- function() {
  return ("SELECT level FROM difficulty;")
}

# Gets all available difficulty levels ===============
# @param: dbCon - connection of the db
getAllDifficultyLevels <- function(dbCon) {
  cat("The difficulty levels are:\n")
  difficulty <- dbGetQuery(dbCon, queryOfDifficultyLevel())
  cat(paste(difficulty$level, collapse = ", "), "\n", sep = "")
}


# SQL query to get lesson having a defined category =========
# @param:category - category to get the title of 
queryTitleOfLessonHavingParticularCategory<- function(category){
  return (sprintf("SELECT title FROM lesson WHERE category = %s",category))
}

# Gets the title of lessons with 1 as category ================
# @param: dbCon - connection of the db
getTitleOfLessonsHavingCategoryAsOne <- function(dbCon){
  cat("The title of lessons with category one are:\n")
  lesson <- dbGetQuery(dbCon, queryTitleOfLessonHavingParticularCategory(1))
  cat(paste(lesson$title, collapse = ", "), "\n", sep = "")
}


# Query to get the numbe of lessons in a given module
# @param: moduleId - moduleId to find number of lessons.
queryToGetNumberOfLessonsInModule <-function(moduleId){
  return (sprintf("SELECT count(*) FROM lesson_module_relation where module_id = '%s'",moduleId))
}

# Gets the numer of lessons present in module two.
# @param: dbCon - connection of the db
getNumberOfLessonsForModuleTwo <- function(dbCon){
  lessonsForModuleTwo <- dbGetQuery(dbCon,queryToGetNumberOfLessonsInModule("M2"))
  cat(
    sprintf(
      "Module two has %s lessons\n",
      lessonsForModuleTwo[[1]]
    )
  )
}





# Queries on the database to get insights on the data ===============
queryOnTheTables <- function(dbCon) {
  
  cat("Generating few insights on the database_____________\n")
  # Gets all the module titles present in the database
  getModuleTitles(dbCon)
  # Gets count of lessons having lessone one as prerequisite
  getCountOfLessonsHavingLessonOneAsPrerequisite(dbCon)
  
  # Gets all difficlty levels
  getAllDifficultyLevels(dbCon)
  
  # Gets title of the lessons with one as its category
  getTitleOfLessonsHavingCategoryAsOne(dbCon)
  
  #Get number of lessons in module two
  getNumberOfLessonsForModuleTwo(dbCon)
}


# Main Method =======================
# Main function to create db connection, create table, insert and validate the
# data of the table.
main <- function() {
  installRequiredPackages()
  dbCon <- createDBConnection('lessonDB-ShubhaA.sqlitedb')
  dropAllExistingTable(dbCon)
  # Enabling foreign key constraint, ref: https://northeastern.instructure.com/courses/192346/assignments/2351515?module_item_id=10707674
  dbExecute(dbCon, "PRAGMA foreign_keys = ON")
  createAllTables(dbCon)
  insertIntoAllTables(dbCon)
  # Validates the data inserted
  validatedDataInsertedToTables(dbCon)
  
  # Run analysis on the data inserted
  queryOnTheTables(dbCon)
  
  # Below lines can be uncommented to clean up after the program is run.
  #dbExecute(dbCon, "PRAGMA foreign_keys = OFF")
  #dropAllExistingTable(dbCon)
  
  # Disconnects the connection from database.
  dbDisconnect(dbCon)
}

main()
