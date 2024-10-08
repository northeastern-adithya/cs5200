# ________________
# title: ASSIGNMENT 02.1, Build File Data Store
# author: Shubha, Adithya
# ______________

# Initialising global variables rootDir and intakeDir ========================
rootDir <- "docDB"
intakeDir <- "docTemp"


# Install program required packages =============================
# Installs testthat to use for testing.
# Referred from: http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Install_Packages_on_Demand
installRequiredPackages <- function() {
  packages <- c("testthat")
  
  # Install packages that are not installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
  
  
  # Load all packages
  invisible(lapply(packages, library, character.only = TRUE))
}


# Memory Management In R=========================================================
# Referred from: http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Memory_Management
cleanUpVaraibles <- function() {
  rm(list = ls(all.names = TRUE))
  gc()
}


# Splits File Name =========================================================
# Splits the file name into vectors separated by (.).
# @param: fileName -  Name of the file.
# Referred from: https://artificium.us/lessons/06.r/l-6-112-text-proc/l-6-112.html#Splitting_a_String_into_Tokens
splitFileName <- function(fileName) {
  return (unlist(strsplit(fileName, "\\.")))
}

# Check File Validity=========================================================
# Checks if file name is proper by validating the length after splitting
# on (.) is equal to 4. Also does additional checking if the year part is
# a valid number between 1000 and 3000. 
# @param: fileName -  Name of the file.
checkFile <- function(fileName) {
  if(is.na(fileName)){
    return (FALSE)
  }
  splittedFileName = splitFileName(fileName)
  if(length(splittedFileName) == 4){
    year = splittedFileName[3]
    # suppressWarnings referred from: https://www.r-bloggers.com/2022/02/nas-introduced-by-coercion/#:~:text=In%20this%20scenario%2C%20just%20wrap,%E2%80%9CNAs%20introduced%20by%20coercion%E2%80%9D.
    yearAsInteger = suppressWarnings(as.integer(year))
    if(is.na(yearAsInteger)){
      return (FALSE)
    }else{
      return (yearAsInteger >= 1000 &&  yearAsInteger <= 3000)
    }
    
  }else{
    return (FALSE)
  }
}

# Get Year Property=========================================================
# Returns the year part of the file name.
# @param: fileName -  Name of the file.
# stops program if invalid file is encountered.
getYear <- function(fileName) {
  if (checkFile(fileName)) {
    return (splitFileName(fileName)[3])
  } else{
    return (stop(sprintf(
      "Error: Invalid file name:%s", fileName
    )))
  }
}

# Get Case Id Property=========================================================
# Returns the case id part of the file name.
# @param: fileName -  Name of the file.
# stops program if invalid file is encountered.
getCaseID <- function(fileName) {
  if (checkFile(fileName)) {
    return (splitFileName(fileName)[1])
  } else{
    return (stop(sprintf(
      "Error: Invalid file name:%s", fileName
    )))
  }
}

# Generate Document Path=========================================================
# Generates document path of where the file should be stored.
# @param: rootFolder - Folder where the document should be stored. Defaults to #rootDir.
# @param: caseID - caseId of the file.
# @param: year - year of the file.
genDocPath <- function(rootFolder = rootDir, caseID, year) {
  return (paste(rootFolder, caseID, year, sep = "/"))
}


# Create Folder=========================================================
# Creates a folder if not present in the said path. Uses recursive = TRUE
# to create recursively.
# Referred from: https://stackoverflow.com/questions/4216753/folder-management-with-r-check-existence-of-directory-and-create-it-if-it-does
# @param: folderPath - Location of the folder to be created.
createFolderIfNotPresent <- function(folderPath) {
  if (!dir.exists(folderPath)) {
    dir.create(folderPath, r = TRUE)
  }
}

# Set up DB=========================================================
# Sets up folder if not present.
# @param: rootFolder - Location of root folder. Defaults to # rootDir
# @param: intakeFolder - Location of intake folder. Defaults to # intakeDir
setupDB <- function(rootFolder = rootDir,
                    intakeFolder = intakeDir) {
  createFolderIfNotPresent(rootFolder)
  createFolderIfNotPresent(intakeFolder)
}

# Validate Folder Path=========================================================
# Validates if the vector of folders given are present.
# @param: folderPaths: Vector of folder paths to validate.
validateIfFolderIsPresent <- function(folderPaths) {
  for (folder in folderPaths) {
    if (!dir.exists(folder)) {
      return (FALSE)
    }
  }
  return (TRUE)
}

# Validate File Path=========================================================
# Validates if given file is present in the path.
# @param: filePath: File path to validate.
validateIfFileIsPresent <- function(filePath) {
  if (!file.exists(filePath)) {
    return (FALSE)
  }
  return (TRUE)
}

# Throws exception if folder not found ===================
# Throws exception if intake or root folder is not found.
# @param: intakeFolder - location of intake folder. Defaults to #intakeDir
# @param: rootFolder - location of root folder. Defaults to #rootDir
thowExceptionOnFolderNotFound<- function(intakeFolder = intakeDir, rootFolder = rootDir){
  # Throws exception if the intake or root folder is not present.
  if (!validateIfFolderIsPresent(c(intakeFolder, rootFolder))) {
    stop(
      paste0(
        "Intake directory: ",
        intakeFolder,
        " or root directory: ",
        rootFolder,
        " not found"
      )
    )
  }
}

# Store Doc Procedure=========================================================
# Copies the document from intakeFolder to rootFolder and deletes the document.
# from intake folder once copying is successful.
# referred from: http://artificium.us/lessons/06.r/l-6-402-filesystem-from-r/l-6-402.html
# @param: intakeFolder - Location of intake folder. Defaults to # intakeDir
# @param: file - File to copy.
# @param: rootFolder - Location of root folder. Defaults to # rootDir
storeDoc <- function(intakeFolder = intakeDir,
                     file,
                     rootFolder = rootDir) {
  
  thowExceptionOnFolderNotFound(intakeFolder,rootFolder)
  
  # tryCatch referred from: https://stackoverflow.com/questions/12193779/how-to-use-the-trycatch-function
  tryCatch(
    {
      # Returns copying as false if invalid file name is encountered.
      if (!checkFile(file)) {
        return (FALSE)
      }
      intakeFilePath = paste(intakeFolder, file, sep = "/")
      # Returns copying as false if file is not found in the intake folder.
      if (!validateIfFileIsPresent(intakeFilePath)) {
        return (FALSE)
      }
      intakeFileInfo <- file.info(intakeFilePath)
      rootPath <- genDocPath(rootFolder, getCaseID(file), getYear(file))
      createFolderIfNotPresent(rootPath)
      rootFilePath <- paste(rootPath, file, sep = "/")
      file.copy(intakeFilePath, rootFilePath)
      
      # Checks if the file was successfully copied.
      if (validateIfFileIsPresent(rootFilePath)) {
        rootPathInfo <- file.info(rootFilePath)
        # If successfully copied, checks the size of the both intake file and root file.
        if (intakeFileInfo$size == rootPathInfo$size) {
          # Removes the file from intake.
          if(!file.remove(intakeFilePath)){
            cat(sprintf("File removal failed for file:%s. Would have to remove manually\n",file))
          }
          return (TRUE)
        } else{
          return (FALSE)
        }
      } else{
        return (FALSE)
      } 
    },error = function() {
      return (FALSE)
    }
    
  )
  
  
}

# Store All Doc Procedure=========================================================
# Copies all the valid files present from intake to root folder.
# referred from: http://artificium.us/lessons/06.r/l-6-402-filesystem-from-r/l-6-402.html
# @param: intakeFolder - Location of intake folder. Defaults to # intakeDir
# @param: rootFolder - Location of root folder. Defaults to # rootDir
storeAllDocs <- function(intakeFolder = intakeDir,
                         rootFolder = rootDir) {
  thowExceptionOnFolderNotFound(intakeFolder,rootFolder)
  files <- unlist(list.files(path = intakeFolder))
  filesNotProcessed <- c()
  for (file in files) {
    if (!checkFile(file)) {
      cat(sprintf("File %s not processed due to invalid file name.\n", file))
      filesNotProcessed <- c(filesNotProcessed, file)
      next
    }
    if (!storeDoc(intakeFolder, file, rootFolder)) {
      cat(sprintf("Encountered failure while processing file: %s.\n", file))
      filesNotProcessed <- c(filesNotProcessed, file)
    }
  }
  cat(sprintf(
    "Successfully processed %d files\n",
    length(files) - length(filesNotProcessed)
  ))
  if (length(filesNotProcessed) != 0) {
    cat("These files were not processed:\n")
    cat(filesNotProcessed, sep = "\n")
  }
}

# Reset Database=========================================================
# Resets all the content of root folder without deleting the actual folder.
# referred from: http://artificium.us/lessons/06.r/l-6-402-filesystem-from-r/l-6-402.html
# @param: rootFolder - Location of root folder. Defaults to # rootDir
resetDB <- function(rootFolder = rootDir) {
  if(validateIfFolderIsPresent(rootFolder)){
    unlink(sprintf("%s/*", rootFolder), recursive = TRUE) 
  }
}


# Test Methods =============================================
testDir = "test"

# Set Up Test Dir===================================================
# Sets up a temporary test directory for running tests.
# @param: testFolder, location of temporary test folder. Defaults to testDir
setupTestDir <- function(testFolder = testDir){
  createFolderIfNotPresent(testFolder)
}

# Clean Up Test Dir===================================================
# Cleans up temporary test directory after running tests.
# @param: testFolder, location of temporary test folder. Defaults to testDir
cleanUpTestDir <- function(testFolder = testDir){
  if(validateIfFolderIsPresent(testFolder)){
    unlink(testFolder,r=T)
  }
}

# Test for splitting file name=============================
testSplitFileName <- function(){
  test_that("Tests Splitting File Name Works", {
    expect_equal(splitFileName("MA3324-SF-0712.Morgan.2024.pptx"), c("MA3324-SF-0712","Morgan","2024","pptx"))
  })
}

# Test to verify checkFile function=============================
testCheckFile <- function(){
  test_that("Test Check File Works", {
    expect_equal(checkFile("MA3324-SF-0712.Morgan.2024.pptx"), TRUE)
    expect_equal(checkFile("MA3324-SF-0712.Morgan.1.pptx"), FALSE)
    expect_equal(checkFile("MA3324-SF-0712.Morgan.A.pptx"), FALSE)
    expect_equal(checkFile("MA3324-SF.0712.Morgan.2024.pptx"), FALSE)
  })
}

# Test to verify getYear function=============================
testGetYear <- function(){
  test_that("Test Get Year Works", {
    expect_equal(getYear("MA3324-SF-0712.Morgan.2024.pptx"), "2024")
    expect_error(getYear("MA3324-SF.0712.Morgan.2024.pptx"), "Error: Invalid file name:MA3324-SF.0712.Morgan.2024.pptx")
  })
}

# Test to verify getCaseID function=============================
testGetCaseID <- function(){
  test_that("Test Get Case ID Works", {
    expect_equal(getCaseID("MA3324-SF-0712.Morgan.2024.pptx"), "MA3324-SF-0712")
    expect_error(getCaseID("MA3324-SF.0712.Morgan.2024.pptx"), "Error: Invalid file name:MA3324-SF.0712.Morgan.2024.pptx")
  })
}

# Tests to verify proper doc path is created=============================
testGenDocPath <- function(){
  test_that("Test Gen doc path works", {
    expect_equal(genDocPath("docDB","MA3324-SF-0712","2024"), "docDB/MA3324-SF-0712/2024")
  })
}

# Tests to verify create folder if not present============================
testCreateFolderIfNotPresent <- function(){
  # Also validates function validateIfFolderIsPresent()
  test_that("Create Folder If Not Present Works", {
    tempFolder = paste(testDir,"testCreateFolderIfNotPresent",sep = "/")
    createFolderIfNotPresent(tempFolder)
    expect_equal(validateIfFolderIsPresent(tempFolder), TRUE)
    createFolderIfNotPresent(tempFolder)
    expect_equal(validateIfFolderIsPresent(tempFolder), TRUE)
    unlink(tempFolder,r=T)
    expect_equal(validateIfFolderIsPresent(tempFolder), FALSE)
  })
}

# Tests to verify setUpDB============================
# Creates a dummy folder in testDir
testSetupDB <- function(){
  test_that("Test setting up db works", {
    testRootFolder = paste(testDir,"testDocDB",sep = "/")
    testIntakeFolder = paste(testDir,"testDocTemp",sep = "/")
    setupDB(testRootFolder,testIntakeFolder)
    expect_equal(dir.exists(testRootFolder),TRUE)
    expect_equal(dir.exists(testIntakeFolder),TRUE)
    unlink(testRootFolder, r = T)
    unlink(testIntakeFolder, r = T)
  })
}

# Creating dummy data=======================
# Helper fucntion for tests to create dummy data.
# Writing dummy data was referred from: https://stackoverflow.com/questions/2470248/write-lines-of-text-to-a-file-in-r
createDummyData<- function(fileName){
  fileConn<-file(fileName)
  writeLines(c("This is a dummy data"), fileConn)
  close(fileConn)
}


# Tests store doc method================
testStoreDoc <- function(){
  test_that("Test store docs fails if test directory is not present", {
    expect_error(storeDoc("randomFolder1","file1","randomFolder2"),
                 "Intake directory: randomFolder1 or root directory: randomFolder2 not found"
    )
  })
  
  test_that("Test store doc returns false if file is not present", {
    testRootFolder = paste(testDir,"testDocDB",sep = "/")
    testIntakeFolder = paste(testDir,"testDocTemp",sep = "/")
    setupDB(testRootFolder,testIntakeFolder)
    expect_equal(storeDoc(testIntakeFolder,"file1",testRootFolder),FALSE)
    unlink(testRootFolder, r = T)
    unlink(testIntakeFolder, r = T)
    
  })
  
}

# Tests store all docs method================
# internally tests storeDoc(), validateIfFileIsPresent() by testing storeAllDocs()
testStoreAllDocs <- function(){
  test_that("Test store all docs works", {
    testRootFolder = paste(testDir,"testDocDB",sep = "/")
    testIntakeFolder = paste(testDir,"testDocTemp",sep = "/")
    setupDB(testRootFolder,testIntakeFolder)
    validFileName = "Case-1.Client-1.2024.txt"
    invalidFileName = "Case-1.Client.1.2023.txt"
    validFileIntakePath = paste(testIntakeFolder,validFileName,sep = "/")
    invalidFileIntakePath = paste(testIntakeFolder,invalidFileName,sep = "/")
    createDummyData(validFileIntakePath)
    createDummyData(invalidFileIntakePath)
    storeAllDocs(testIntakeFolder,testRootFolder)
    expect_equal(file.exists(paste(testRootFolder,"Case-1/2024/Case-1.Client-1.2024.txt",sep = "/")),TRUE)
    expect_equal(file.exists(invalidFileIntakePath),TRUE)
    unlink(testRootFolder, r = T)
    unlink(testIntakeFolder, r = T)
  })
  
  test_that("Test store all docs fails if test directory is not present", {
    expect_error(storeAllDocs("randomFolder1","randomFolder2"),
                 "Intake directory: randomFolder1 or root directory: randomFolder2 not found"
    )
  })
  
}

# Main test method =========================
# Main test method to set up test directory,execute tests and clean up afterwards. 
test <- function(){
  setupTestDir()
  testSplitFileName()
  testCheckFile()
  testGetYear()
  testGetCaseID()
  testGenDocPath()
  testCreateFolderIfNotPresent()
  testSetupDB()
  testStoreDoc()
  testStoreAllDocs()
  cleanUpTestDir()
  cleanUpVaraibles()
}

# Main Method ================================================
# Sets of required DB, validates if the folders were set up properly and
# copies files from intake folder to root folder.
main <- function() {
  cleanUpVaraibles()
  installRequiredPackages()
  test() # Can be commented out to skip tests
  
  setupDB()
  thowExceptionOnFolderNotFound()
  
  storeAllDocs()
  # resetDB() # Can be uncommented to reset the entire db
  cleanUpVaraibles()
}

main()
