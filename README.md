# Whole-Blood-Study-Enroll-Tracy
 
 This script generates a report that shows all patients in the UCC with a CBC (WBC count result and Abs Neutrophil if present) recieved in lab that also have a Microbiology test in lab from 4:00 am the day the script is run to the current time the script is run. If it is run after 10:05 am the start time for the query will be 10 am of the current day. 

 Two sql statments query cerner. One for the CBC results and one for the Microbiology test results. An inner join is performed on these two data sets so that the data set produced shows all patients with the CBC that also have a microbiology test in lab. 
 
 This report will be used to enroll whole blood specimens in a microbiology research study and allow the microbiology research team to request whole blood specimens from hematology before they are thrown out. 
