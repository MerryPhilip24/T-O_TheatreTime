################################################################################################
#       PROJECT:    GET THEATRE DATA FROM MORE THAN 30K UNIQUE SPELL IDS
#       CREATED BY: MERRY PHILIP
#       DATE:       26th MAY 2023
################################################################################################

"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  INSTALL PACKAGES   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
#List of Packages
list.of.packages <- c( "tidyverse", "lubridate", "GetoptLong", "devtools", "odbc", 
                       "DBI", "xlsx", "writexl", "RDCOMClient", "keyring") 

#Install packages if not already installed
new.package <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.package)){
  install.packages(new.package)}

# Install the libraries
invisible(lapply(list.of.packages, library, character.only = TRUE))
"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

######################################  SERVER CONNECTION   ######################################
#Connecting to the server- AZURE002
"@Set the Password@"
server <- "sqlldndapprd002.database.windows.net"
database <- "sdbldndapprd002"
password <- Sys.getenv("Automation_Password")
Azure<- DBI::dbConnect(odbc::odbc(),
                       UID = "Merry.Philip@colchesterhospital.nhs.uk",
                       password = password,
                       Driver ="ODBC Driver 17 for SQL Server",
                       Server = server, Database = database,
                       Authentication = "ActiveDirectoryInteractive")

###################################################################################################

# Read the CSV file containing the IDs
ids <- read.xlsx("//esneft.nhs.uk/share/Finance/Capacity & Analytics/Business Informatics/Team Folders/Merry/Git/T&OTheatreTime/df_consultants.xlsx"
                 , sheetIndex = 1)

#Rename the column to merge
ids <- ids %>%  
  rename(Spell_ID = SPELL_ID)

# Create an empty data frame to store the merged data
mergedData <- data.frame()

# Retrieve data for each batch of 1,000 IDs  - 
#"This is because the SQL query does not run with where clause having many (like MANY) values"

#Create how much IDs you want each loop
batchSize <- 1000
numBatches <- ceiling(nrow(ids) / batchSize)          

for (i in 1:numBatches) {
  # Get the IDs for the current batch
  startIdx <- (i - 1) * batchSize + 1
  endIdx <- min(startIdx + batchSize - 1, nrow(ids))
  currentIds <- ids[startIdx:endIdx, , drop = FALSE]
  
  # Create the SQL query with the current IDs
  sqlQuery <- paste("SELECT [Spell ID], [Patient ID], [Session Planned Start Date/Time],",
                    "[Planned Start Date/Time], [Planned Duration], [Actual Duration],",
                    "[Called Date/Time], [Left Ward Date/Time], [Arrived Date/Time],",
                    "[Anaesthetic Date/Time], [Into Theatre Date/Time], [Ready to Leave Date/Time],",
                    "[First Knife to Skin Date/Time], [Last Knife Down Date/Time],",
                    "[Out of Theatre Date/Time], [H4 Minutes]",
                    "FROM [powerbi].[TheatreOperations]",
                    "WHERE [Spell ID] IN ('", 
                    paste(currentIds$Spell_ID, collapse = "', '"), "')", sep = "")
  
  # Execute the SQL query and fetch the data
  queryResult <- dbGetQuery(Azure, sqlQuery)
  
  
  
  
  # Merge the data with the IDs in the same order
  if (i == 1) {
    mergedData <- queryResult
  } else {
    mergedData <- merge(mergedData, queryResult, by = c("Spell ID", "Patient ID", 
                                                        "Session Planned Start Date/Time", 
                                                        "Planned Start Date/Time", 
                                                        "Planned Duration", 
                                                        "Actual Duration", 
                                                        "Called Date/Time", 
                                                        "Left Ward Date/Time", 
                                                        "Arrived Date/Time", 
                                                        "Anaesthetic Date/Time", 
                                                        "Into Theatre Date/Time", 
                                                        "Ready to Leave Date/Time", 
                                                        "First Knife to Skin Date/Time", 
                                                        "Last Knife Down Date/Time", 
                                                        "Out of Theatre Date/Time", 
                                                        "H4 Minutes"), all = TRUE)
  }
}

#Get the data out
write.xlsx(mergedData, "//esneft.nhs.uk/share/Finance/Capacity & Analytics/Business Informatics/Team Folders/Merry/Git/T&OTheatreTime/T&OTheatre.xlsx")
