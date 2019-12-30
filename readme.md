##Newmark Knight Frank Webscrape ##

This repo is primarily to test the various webscrapes for Newmark Knight Frank websites. 
We will be using it for the sale and lease portion of their sites.

Once pulled, the repo aims to have this pushed into database structure. The typical structure for this database schema
is included in the .sql file. This is only a template and by no means should prevent any changes if needed. The sql is
written in MSSQL format and can be tweaked to line up with a different SQL syntax.

### To dos ###
* Creating environment variables
    * For the dictionary for classes needed for the scrape
    * For the connection string to database
    * Links needed for the website scrape
* Making sure the sql connection works well
* Need to figure out how to use the site key in a way that they don't block you after a few pulls

### Using the repo ###
When running the repo on airflow, please make sure you create environment variables that include the links. In the
future, we need to make sure that we the scrape is using a dictionary to pull all the various tags that are needed.
Eventually, we can even use a variable in airflow to pass that through, so that in the event
the site changes substantially, we do not need to go into the code and change the repo.

### Installation ###
To run locally, just make sure that you have access to the internet.
Need to make sure you install the requirements.txt. 
Until you are able figure out how to use the site key properly, take the page source and store it in the file_lease.txt
or file_sale.txt.
Eventually, we will build out and make sure that it connects to the database as well.