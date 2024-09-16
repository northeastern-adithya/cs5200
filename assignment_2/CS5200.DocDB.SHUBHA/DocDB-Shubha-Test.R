testSplitFileName <- function(){
  test_that("Splitting File Name Works", {
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
    expect_error(getYear("MA3324-SF.0712.Morgan.2024.pptx"), NA)
  })
}

testGetCaseID <- function(){
  test_that("Test Get Case ID Workds", {
    expect_equal(getCaseID("MA3324-SF-0712.Morgan.2024.pptx"), "MA3324-SF-0712")
    expect_equal(getCaseID("MA3324-SF.0712.Morgan.2024.pptx"), NA)
  })
}

testGenDocPath <- function(){
  test_that("Test Gen doc path works", {
    expect_equal(genDocPath(rootDir,"MA3324-SF-0712","2024"), "docDB/MA3324-SF-0712/2024")
  })
}

testSetupDB <- function(){
  test_that("Test setting up db works", {
    testRootFolder = "testDocDB"
    testIntakeFolder = "testDocTemp"
    if(dir.exists(testRootFolder)){
      unlink(testRootFolder, recursive = TRUE)
    }
    if(dir.exists(testIntakeFolder)){
      unlink(testIntakeFolder, recursive = TRUE)
    }
    setupDB(testRootFolder,testIntakeFolder)
    expect_equal(dir.exists(testRootFolder),TRUE)
    expect_equal(dir.exists(testIntakeFolder),TRUE)
    unlink(testRootFolder, recursive = TRUE)
    unlink(testIntakeFolder, recursive = TRUE)
  })
}

test <- function(){
  installRequiredPackages()
  cleanUpVaraibles()
  testSplitFileName()
  testCheckFile()
  testGetYear()
  testGetCaseID()
  testGenDocPath()
  testSetupDB()
  cleanUpVaraibles()
}

test()