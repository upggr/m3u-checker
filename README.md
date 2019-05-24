# m3u-checker

1.Enter the url of the file you want to check in : target_addr.py

2.Use the SQL file helper/db.schema.sql.gz and create the db on some remote mysql server

3.Enter remote mysql server details in : rds_config_db.py (rename the sample)

4.Run pip3 install pymysql --target .   (in this directory)

5.use the serverless.yml to deploy to lambda and run it from the region you want (for example user serverless dashboard in atom or the command line)
