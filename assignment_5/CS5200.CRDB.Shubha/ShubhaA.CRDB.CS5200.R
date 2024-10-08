# ________________
# title: ASSIGNMENT 05.1: Implement a Relational Database
# author: Shubha, Adithya
# date: 2024-10-08
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


createDBConnection <- function(dbName) {
  return (dbConnect(RSQLite::SQLite(), dbName))
}

sqlToCreateDifficulty <- function() {
  return ('CREATE TABLE IF NOT EXISTS difficulty(
    id INTEGER PRIMARY KEY,
    level TEXT
);')
}

sqlToInsertIntoDifficulty <- function() {
  return (
    "INSERT INTO difficulty (id, level) VALUES
          (1, 'beginner'),
          (2, 'intermediate'),
          (3, 'advanced'),
          (4, 'graduate');"
  )
  
}

sqlToCreateModule <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS module (
    id TEXT PRIMARY KEY,
    title TEXT,
    length_in_minutes NUMBER,
    difficulty_id INTEGER,
    FOREIGN KEY (difficulty_id) REFERENCES difficulty(id)
);'
  )
}


sqlToInsertIntoModule <- function() {
  return (
    "INSERT INTO module (id, title, length_in_minutes, difficulty_id) VALUES
        ('M1', 'DATABASE SYSTEMS', 60, 1),
        ('M2', 'DATA PROCESSING & R', 45, 3),
        ('M3', 'DATA MODELING', 30, 2);"
  )
}

sqlToCreateLesson <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson (
    id INTEGER PRIMARY KEY,
    category INTEGER,
    number INTEGER,
    title TEXT
);'
  )
}

sqlToInsertIntoLesson <- function() {
  return (
    "INSERT INTO Lesson (id, category, number, title) VALUES
      (1, 1, 1, 'The Need for databases'),
      (2, 1, 2, 'R Ecosystem'),
      (3, 2, 1, 'Working with R'),
      (4, 2, 2, 'R for programmers'),
      (5, 3, 1, 'Relationship Types');"
  )
}

sqlToCreateLessonModuleRelation <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson_module_relation (
    id INTEGER PRIMARY KEY,
    lesson_id INTEGER,
    module_id TEXT,
    FOREIGN KEY (lesson_id) REFERENCES lesson(id),
    FOREIGN KEY (module_id) REFERENCES module(id)
);'
  )
}

sqlToInsertIntoLessonModuleRelation <- function() {
  return (
    "INSERT INTO lesson_module_relation (id, lesson_id, module_id) VALUES
          (1, 1, 'M1'),
          (2, 2, 'M2'),
          (3, 3, 'M2'),
          (4, 4, 'M2'),
          (5, 1, 'M2'),
          (6,5,'M3');"
  )
}


sqlToCreateLessonPrerequisite <- function() {
  return (
    'CREATE TABLE IF NOT EXISTS lesson_prerequisite (
    id INTEGER PRIMARY KEY,
    lesson_id INTEGER,
    lesson_prerequisite_id INTEGER,
    FOREIGN KEY (lesson_id) REFERENCES lesson(id),
    FOREIGN KEY (lesson_prerequisite_id) REFERENCES lesson(id)
);
'
  )
}

sqlToInsertIntoLessonPrerequisite <- function() {
  return (
    "INSERT INTO lesson_prerequisite (id, lesson_id, lesson_prerequisite_id) VALUES
            (1, 2, 1),
            (2, 3, 1),
            (3, 3, 2),
            (4, 4, 1);"
  )
}

dropTableIfExist <- function(table) {
  return (sprintf('drop table if exists %s', table))
}

dropAllExistingTable <- function(dbCon) {
  tables <- dbListTables(dbCon)
  for (table in tables) {
    dbExecute(dbCon, dropTableIfExist(table))
  }
}

createAllTables <- function(dbCon) {
  dbExecute(dbCon, sqlToCreateDifficulty())
  dbExecute(dbCon, sqlToCreateModule())
  dbExecute(dbCon, sqlToCreateLesson())
  dbExecute(dbCon, sqlToCreateLessonModuleRelation())
  dbExecute(dbCon, sqlToCreateLessonPrerequisite())
}

insertIntoAllTables <- function(dbCon)
{
  dbExecute(dbCon, sqlToInsertIntoDifficulty())
  dbExecute(dbCon, sqlToInsertIntoModule())
  dbExecute(dbCon, sqlToInsertIntoLesson())
  dbExecute(dbCon, sqlToInsertIntoLessonModuleRelation())
  dbExecute(dbCon, sqlToInsertIntoLessonPrerequisite())
}



main <- function() {
  installRequiredPackages()
  dbCon <- createDBConnection('lessonDB-ShubhaA.sqlitedb')
  dropAllExistingTable(dbCon)
  dbExecute(dbCon, "PRAGMA foreign_keys = ON")
  createAllTables(dbCon)
  insertIntoAllTables(dbCon)
  #dbExecute(dbCon, "PRAGMA foreign_keys = OFF")
  #dropAllExistingTable(dbCon)
  dbDisconnect(dbCon)
}

main()

