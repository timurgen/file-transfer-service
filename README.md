Simple file transfer service for Sesam.io powered applications

##Usage
Service has only one endpoint `POST:/transfer` that takes list of json entities as input and return   
(currently) nothing. Each entity must contain url to attachment and name of file.  
Default properties `file_url` and `file_ide` (may be overridden with FILE_URL/FILE_ID env variables)

Downloaded attachment will then be uploaded to URL from property UPLOAD_URL(required)




