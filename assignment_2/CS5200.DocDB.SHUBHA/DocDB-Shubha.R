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
# Refrenced from: http://artificium.us/lessons/06.r/l-6-104-r4progs/l-6-104.html#Memory_Management
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
# Checks if file name is proper by validating the length after spliting
# on (.) is equal to 4.
# @param: fileName -  Name of the file.
checkFile <- function(fileName) {
  return (length(splitFileName(fileName)) == 4)
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
  if (file.exists(rootFilePath)) {
    rootPathInfo <- file.info(rootFilePath)
    # If successfully copied, checks the size of the both intake file and root file.
    if (intakeFileInfo$size == rootPathInfo$size) {
      # Removes the file from intake.
      return (file.remove(intakeFilePath))
    } else{
      return (FALSE)
    }
  } else{
    return (FALSE)
  }
  
}

# Store All Doc Procedure=========================================================
# Copies all the valid files present from intake to root folder.
# referred from: http://artificium.us/lessons/06.r/l-6-402-filesystem-from-r/l-6-402.html
# @param: intakeFolder - Location of intake folder. Defaults to # intakeDir
# @param: rootFolder - Location of root folder. Defaults to # rootDir
storeAllDocs <- function(intakeFolder = intakeDir,
                         rootFolder = rootDir) {
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
    cat("These files were not processed:")
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

setupTestDir <- function(testFolder = testDir){
  createFolderIfNotPresent(testFolder)
}

cleanUpTestDir <- function(testFolder = testDir){
  if(validateIfFolderIsPresent(testFolder)){
    unlink(testFolder,r=T)
  }
}

testSplitFileName <- function(){
  test_that("Tests Splitting File Name Works", {
    expect_equal(splitFileName("MA3324-SF-0712.Morgan.2024.pptx"), c("MA3324-SF-0712","Morgan","2024","pptx"))
  })
}

testCheckFile <- function(){
  test_that("Test Check File Works", {
    expect_equal(checkFile("MA3324-SF-0712.Morgan.2024.pptx"), TRUE)
    expect_equal(checkFile("MA3324-SF.0712.Morgan.2024.pptx"), FALSE)
  })
}

testGetYear <- function(){
  test_that("Test Get Year Works", {
    expect_equal(getYear("MA3324-SF-0712.Morgan.2024.pptx"), "2024")
    expect_error(getYear("MA3324-SF.0712.Morgan.2024.pptx"), "Error: Invalid file name:MA3324-SF.0712.Morgan.2024.pptx")
  })
}


testGetCaseID <- function(){
  test_that("Test Get Case ID Works", {
    expect_equal(getCaseID("MA3324-SF-0712.Morgan.2024.pptx"), "MA3324-SF-0712")
    expect_error(getCaseID("MA3324-SF.0712.Morgan.2024.pptx"), "Error: Invalid file name:MA3324-SF.0712.Morgan.2024.pptx")
  })
}

testGenDocPath <- function(){
  test_that("Test Gen doc path works", {
    expect_equal(genDocPath("docDB","MA3324-SF-0712","2024"), "docDB/MA3324-SF-0712/2024")
  })
}

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

test <- function(){
  setupTestDir()
  testSplitFileName()
  testCheckFile()
  testGetYear()
  testGetCaseID()
  testGenDocPath()
  testCreateFolderIfNotPresent()
  testSetupDB()
  cleanUpTestDir()
  cleanUpVaraibles()
}

# Main Method ================================================
# Sets of required DB, validates if the folders were set up properly and
# copies files from intake folder to root folder.
main <- function() {
  cleanUpVaraibles()
  installRequiredPackages()
  test()
  
  setupDB()
  if (!validateIfFolderIsPresent(c(rootDir, intakeDir))) {
    stop(sprintf(
      "Intake directory: %s or root directory: %s not found",
      rootDir,
      intakeDir
    ))
  }
  
  storeAllDocs()
  cleanUpVaraibles()
}

main()
