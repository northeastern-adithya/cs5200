rootDir <- "docDB"
intakeDir <- "docTemp"

cleanUpVaraibles <- function(){
  rm(list = ls(all.names = TRUE))
  gc()
}

splitFileName <- function(fileName){
  return (unlist(strsplit(fileName,"\\.")))
}

checkFile <- function(fileName){
  return ( length(splitFileName(fileName)) == 4 )
}

getYear <- function(fileName){
  if(checkFile(fileName)){
    return (splitFileName(fileName)[3])
  }else{
    return (NA)
  }
}

getCaseID <- function(fileName){
  if(checkFile(fileName)){
    return (splitFileName(fileName)[1])
  }else{
    return (NA)
  }
}

genDocPath <- function(rootFolder=rootDir, caseID, year){
  return (paste(rootFolder,caseID,year,sep ="/"))
}


createFolderIfNotPresent<- function(folderPath){
  if (!dir.exists(folderPath)){
    dir.create(folderPath,r=TRUE)
  }
}

setupDB <- function(rootFolder= rootDir,intakeFolder = intakeDir)
{
  createFolderIfNotPresent(rootFolder)
  createFolderIfNotPresent(intakeFolder)
}

validateIfFolderIsPresent <- function(folderPaths){
  for(folder in folderPaths){
    if(!dir.exists(folder)){
      return (FALSE)
    }
  }
  return (TRUE)
}

validateIfFileIsPresent <- function(filePath){
  if(!file.exists(filePath)){
    return (FALSE)
  }
  return (TRUE)
}

storeDoc <- function(intakeFolder = intakeDir, file, rootFolder = rootDir){
  if(!validateIfFolderIsPresent(c(intakeFolder,rootFolder))){
    stop(paste0("Intake directory: ",intakeFolder, " or root directory: ",rootFolder, " not found"))
  }
  intakeFilePath = paste(intakeFolder,file,sep = "/")
  if(!validateIfFileIsPresent(intakeFilePath)){
    return (FALSE)
  }
  intakeFileInfo <- file.info(intakeFilePath)
  if(!checkFile(file)){
    return (FALSE)
  }
  rootPath <- genDocPath(rootFolder,getCaseID(file),getYear(file))
  createFolderIfNotPresent(rootPath)
  rootFilePath <- paste(rootPath,file,sep="/")
  file.copy(intakeFilePath, rootFilePath)
  if(file.exists(rootFilePath)){
    rootPathInfo <- file.info(rootFilePath)
    if(intakeFileInfo$size == rootPathInfo$size){
      return (file.remove(intakeFilePath))
    }else{
      return (FALSE)
    }
  }else{
    return (FALSE)
  }
  
}

storeAllDocs<- function(intakeFolder=intakeDir, rootFolder=rootDir){
  files <- unlist(list.files(path = intakeFolder))
  filesNotProcessed <- c()
  for (file in files){
    if(!checkFile(file)){
      print(sprintf("File %s not processed due to invalid file name.",file))
      filesNotProcessed <- c(filesNotProcessed,file)
      next
    }
    if(!storeDoc(intakeFolder,file,rootFolder)){
      print(sprintf("Encountered failure while processing file: %s.",file))
      filesNotProcessed <- c(filesNotProcessed,file)
    }
  }
  print(sprintf("Successfully processed %d files",
          length(files)- length(filesNotProcessed)))
  if(length(filesNotProcessed) != 0 ){
    print("These files were not processed:")
    cat(filesNotProcessed,sep = "\n")
    
  }
}

resetDB <- function(rootFolder = rootDir){
  unlink(sprintf("%s/*",rootFolder), recursive = TRUE)
}

main <- function(){
  cleanUpVaraibles()
  setupDB()
  if(!validateIfFolderIsPresent(c(rootDir,intakeDir))){
    stop(sprintf("Intake directory: %s or root directory: %s not found",rootDir,intakeDir))
  }
  
  #storeAllDocs()
  resetDB()
  cleanUpVaraibles()
}

main()