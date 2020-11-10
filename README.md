# Movies ETL
## Background
Amazing Prime loves the dataset and wants to keep it updated on a daily basis. Britta needs your help to create an automated pipeline that takes in new data, performs the appropriate transformations, and loads the data into existing tables. You’ll need to refactor the code from this module to create one function that takes in the three files—Wikipedia data, Kaggle metadata, and the MovieLens rating data—and performs the ETL process by adding the data to a PostgreSQL database.

## Overview of Project
> Wikipedia has a ton of information about movies, including budgets and box office returns, cast and crew, production and distribution, and so much more. Luckily, one of Britta's coworkers created a script to go through a list of movies on Wikipedia from 1990 to 2018 and extract the data from the sidebar into a JSON. Unfortunately, her coworker can't find the script anymore and just has the JSON file. We'll need to load in the JSON file into a Pandas DataFrame.

## Project Deliverables

1. ***Deliverable 1***: Write an ETL Function to Read Three Data Files
2. ***Deliverable 2***: Extract and Transform the Wikipedia Data
3. ***Deliverable 3***: Extract and Transform the Kaggle Data
4. ***Deliverable 3***: Create the Movie Database
5. ***Deliverable 5***: A written report on the Movie Database analysis [`README.md`](https://github.com/emmanuelmartinezs/Movies-ETL). 

## Resources and Before Start Notes:

* Data Source: `ETL Deliverable 1`, `ETL Deliverable 2` and `ETL Deliverable 3` 
* Data Tools: PostgreSQL, pgAdmin
* Software: pgAdmin 4.26, Python 3.8.3, Visual Studio Code 1.50.0, Flask Version 1.0.2

For more information, read the [`Documentation on PostgreSQL and other data typess`](https://www.postgresql.org/docs/manuals/). 

## Database Keys
Database keys identify records from tables and establish relationships between tables. There are numerous types of keys. For our purposes, we will focus on primary keys and foreign keys.

## Primary Keys
The departments.csv file has a dept_no column with unique identifiers for each row (one department number per department). For example, d001 will always reference the Marketing department, across other worksheets. This unique identifier is known as a primary key.

Primary keys are an important part of database design. When a database is being created, each table added must include a primary key in the architecture. Primary keys serve as a link between these tables.

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/s1.PNG?raw=true)


## Deliverable 1:  Write an ETL Function to Read Three Data Files
### Deliverable Requirements:
Using your knowledge of Python, Pandas, the ETL process, and code refactoring, write a function that reads in the three data files and creates three separate DataFrames.

1. An ETL function is written to read in the three data files. 
2. The function converts the Wikipedia JSON file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb file`.
3. ​The function converts the Kaggle metadata file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb file`.
4. The function converts the MovieLens ratings data file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb` file.

 
### Results with detail analysis:

1. **An ETL function is written to read in the three data files.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
import json
import pandas as pd
import numpy as np
import re
import time

!pip install psycopg2

from sqlalchemy import create_engine
from config import db_password

# Create the path to your file directory and variables for the three files. 
file_dir = "C://Users/Emmanuel/Google Drive/Columbia University/GitHub/Movies-ETL/"

# Wikipedia data
wiki_file = f'{file_dir}/wikipedia.movies.json'

# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'

# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv'
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.1.PNG?raw=true)

2. **The function converts the Wikipedia JSON file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb file`.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
    # Open the read the Wikipedia data JSON file.
    f'{file_dir}wikipedia-movies.json'
    
    # Read in the raw wiki movie data as a Pandas DataFrame.
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, ratings
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.2.PNG?raw=true)

3. ​***The function converts the Kaggle metadata file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb file`.**
4. **The function converts the MovieLens ratings data file to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_function_test.ipynb` file.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
    # Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv') 
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.3.PNG?raw=true)

4. **All Code related to `ETL_function_test.ipynb` file.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
import json
import pandas as pd
import numpy as np
import re
import time

!pip install psycopg2

from sqlalchemy import create_engine
from config import db_password

# 1. Create a function that takes in three arguments;
# Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)

def three_arguments_func():
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')    

    # 3. Open the read the Wikipedia data JSON file.
    f'{file_dir}wikipedia-movies.json'
    
    # 4. Read in the raw wiki movie data as a Pandas DataFrame.
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # 5. Return the three DataFrames
    return wiki_movies_df, kaggle_metadata, ratings

# 6. Create the path to your file directory and variables for the three files. 
file_dir = "C://Users/Emmanuel/Google Drive/Columbia University/GitHub/Movies-ETL/"

# Wikipedia data
wiki_file = f'{file_dir}/wikipedia.movies.json'

# Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'

# MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv'

# 7. Set the three variables in Step 6 equal to the function created in Step 1.
wiki_file, kaggle_file, ratings_file = three_arguments_func()

# 8. Set the DataFrames from the return statement equal to the file names in Step 6. 
wiki_movies_df = wiki_file
kaggle_metadata = kaggle_file
ratings = ratings_file

# 9. Check the wiki_movies_df DataFrame.
wiki_movies_df.head()

# 10. Check the kaggle_metadata DataFrame.
kaggle_metadata.head()

# 11. Check the ratings DataFrame.
ratings.head()

#by Emmanuel Martinez
````

> Check the wiki_movies_df DataFrame. `wiki_movies_df.head()`

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.4.1.PNG?raw=true)


> Check the kaggle_metadata DataFrame. `kaggle_metadata.head()`

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.4.2.PNG?raw=true)


> Check the ratings DataFrame. `ratings.head()`

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/1.4.3.PNG?raw=true)



## Deliverable 2: Extract and Transform the Wikipedia Data
### Deliverable Requirements:
Using your knowledge of Python, Pandas, the ETL process, and code refactoring, extract and transform the Wikipedia data so you can merge it with the Kaggle metadata. While extracting the IMDb IDs using a regular expression string and dropping duplicates, use a `try-except` block to catch errors.

1. The TV shows are filtered out, and the `wiki_movies_df` DataFrame is created.
2. A `try-except` block is used to catch errors while extracting the IMDb IDs with a regular expression and dropping duplicate IDs.
3. The extraction and transformation of the Wikipedia data in the ETL function does the following:
    1. A list comprehension is used to keep columns with non-null values.
    2. The non-null box office data is converted to string values using the lambda and join functions.
    3. A regular expression is used to match the six elements of "form_one" of the box office data.
    4. A regular expression is used to match the three elements of "form_two" of the box office data.
    5. The following columns are cleaned in the Wikipedia DataFrame:
        1. The box office column
        2. The budget column
        3. The release date column
        4. The running time column​
4. The cleaned Wikipedia data is converted to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_clean_wiki_movies.ipynb` file.


### Results with detail analysis:

1. **The TV shows are filtered out, and the `wiki_movies_df` DataFrame is created.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
# 1. Add the clean movie function that takes in the argument, "movie".
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    return movie 
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/2.1.PNG?raw=true)



2. **A `try-except` block is used to catch errors while extracting the IMDb IDs with a regular expression and dropping duplicate IDs.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    except: 
        print("No link avialable")
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/2.2.PNG?raw=true)


3. ​***The extraction and transformation of the Wikipedia data in the ETL function does the following:**
    1. A list comprehension is used to keep columns with non-null values.
    2. The non-null box office data is converted to string values using the lambda and join functions.
    3. A regular expression is used to match the six elements of "form_one" of the box office data.
    4. A regular expression is used to match the three elements of "form_two" of the box office data.
    5. The following columns are cleaned in the Wikipedia DataFrame:
        1. The box office column
        2. The budget column
        3. The release date column
        4. The running time column

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
# Complete Code:

def three_arguments_func():
    # 2. Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')


    # Open and read the Wikipedia data JSON file.
    f'{file_dir}wikipedia-movies.json'
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    # 3. Write a list comprehension to filter out TV shows.
    wiki_tv = [tvshows for tvshows in wiki_movies_raw 
        if 'Television series' in tvshows] 

    # 4. Write a list comprehension to iterate through the cleaned wiki movies list
    # and call the clean_movie function on each movie.
    clean_movies = [clean_movie(movie) for movie in wiki_movies_raw]    

    # 5. Read in the cleaned movies list from Step 4 as a DataFrame.
    wiki_movies_df = pd.DataFrame(clean_movies)

    # 6. Write a try-except block to catch errors while extracting the IMDb ID using a regular expression string and
    #  dropping any imdb_id duplicates. If there is an error, capture and print the exception.
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    except: 
        print("No link avialable")

    #  7. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]  

    # 8. Create a variable that will hold the non-null values from the “Box office” column.
    box_office = wiki_movies_df['Box office'].dropna()

    # 9. Convert the box office data created in Step 8 to string values using the lambda and join functions.
    box_office[box_office.map(lambda x: type(x) != str)] 

    # 10. Write a regular expression to match the six elements of "form_one" of the box office data.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    
    # 11. Write a regular expression to match the three elements of "form_two" of the box office data.
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'    
    
    # 12. Add the parse_dollars function.
    def parse_dollars(s):
        if type(s) != str:
            return np.nan
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**6
            return value
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**9
            return value
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            s = re.sub('\$|,','', s)
            value = float(s)
            return value
        else:
            return np.nan

    # 13. Clean the box office column in the wiki_movies_df DataFrame.
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # 14. Clean the budget column in the wiki_movies_df DataFrame.
    budget = wiki_movies_df['Budget'].dropna()
    
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]
    
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    budget[~matches_form_one & ~matches_form_two]
    
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    wiki_movies_df.drop('Budget', axis=1, inplace=True) 
    
    # 15. Clean the release date column in the wiki_movies_df DataFrame.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    
    # 16. Clean the running time column in the wiki_movies_df DataFrame.
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    # Return three variables. The first is the wiki_movies_df DataFrame
    return wiki_movies_df, kaggle_metadata, ratings

# 17. Create the path to your file directory and variables for the three files.
file_dir = "C://Users/Emmanuel/Google Drive/Columbia University/GitHub/Movies-ETL/" 
# The Wikipedia data
wiki_file = f'{file_dir}/wikipedia.movies.json'
# The Kaggle metadata
kaggle_file = f'{file_dir}/movies_metadata.csv'
# The MovieLens rating data.
ratings_file = f'{file_dir}/ratings.csv'

# 18. Set the three variables equal to the function created in D1.
wiki_file, kaggle_file, ratings_file = three_arguments_func()

# 19. Set the wiki_movies_df equal to the wiki_file variable. 
wiki_movies_df = wiki_file

````

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/3.1.PNG?raw=true)
![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/3.2.PNG?raw=true)
![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/3.3.PNG?raw=true)



4. **The cleaned Wikipedia data is converted to a Pandas DataFrame, and the DataFrame is displayed in the `ETL_clean_wiki_movies.ipynb` file.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````python
# 20. Check that the wiki_movies_df DataFrame looks like this. 
wiki_movies_df.head() 

# 21. Check that wiki_movies_df DataFrame columns are correct. 
wiki_movies_df.columns.to_list()
````
> Check that the wiki_movies_df DataFrame looks like this.

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/4.1.PNG?raw=true)

> Check that wiki_movies_df DataFrame columns are correct. 

![name-of-you-image](https://github.com/emmanuelmartinezs/Movies-ETL/blob/main/Resources/images/4.2.PNG?raw=true)



## Deliverable 3: Extract and Transform the Kaggle Data
### Deliverable Requirements:
Using your knowledge of Python, Pandas, the ETL process, and code refactoring, extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then, you’ll merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the `movies_df` DataFrame. Finally, you’ll merge the MovieLens rating data DataFrame with the `movies_df` DataFrame to create the `movies_with_ratings_df`.

1. The extraction and transformation of the Kaggle metadata using the ETL function does the following:
    1. The Kaggle metadata is cleaned.
    2. The Wikipedia and Kaggle DataFrames are merged.
    3. The following is performed on the merged Wikipedia and Kaggle DataFrames to create the `movies_df`:
        1. Unnecessary columns are dropped.
        2. A function is used to fill in the missing Kaggle data.
        3. The `movies_df` DataFrame is filtered to keep specific columns.
        4. The `movies_df` DataFrame columns are renamed.
2. The extraction and transformation of the MovieLens ratings data using the ETL function does the following:
    1. The ratings counts are cleaned.
    2. The `movies_df` DataFrame is merged with the cleaned ratings DataFrame to create the `movies_with_ratings_df` DataFrame.
    3. The empty values in the `movies_with_ratings_df` DataFrame are filled with “0”.
3. The `movies_with_ratings_df` and the `movies_df DataFrames` are displayed in the `ETL_clean_kaggle_data.ipynb` file.

 
### Results with detail analysis:

1. **The extraction and transformation of the Kaggle metadata using the ETL function does the following:**
    1. The Kaggle metadata is cleaned.
    2. The Wikipedia and Kaggle DataFrames are merged.
    3. The following is performed on the merged Wikipedia and Kaggle DataFrames to create the `movies_df`:
        1. Unnecessary columns are dropped.
        2. A function is used to fill in the missing Kaggle data.
        3. The `movies_df` DataFrame is filtered to keep specific columns.
        4. The `movies_df` DataFrame columns are renamed.


> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````SQL
-- Follow the instructions below to complete Deliverable 1.
SELECT e.emp_no,
       e.first_name,
       e.last_name,
       t.title,
       t.from_date,
       t.to_date
INTO retirement_titles
FROM employees as e
INNER JOIN titles as t
ON (e.emp_no = t.emp_no)
WHERE (e.birth_date BETWEEN '1952-01-01' AND '1955-12-31')
order by e.emp_no;
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.1.PNG?raw=true)

2. **The extraction and transformation of the MovieLens ratings data using the ETL function does the following:**
    1. The ratings counts are cleaned.
    2. The `movies_df` DataFrame is merged with the cleaned ratings DataFrame to create the `movies_with_ratings_df` DataFrame.
    3. The empty values in the `movies_with_ratings_df` DataFrame are filled with “0”.


> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.1r.PNG?raw=true)

3. ​***The `movies_with_ratings_df` and the `movies_df DataFrames` are displayed in the `ETL_clean_kaggle_data.ipynb` file.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````SQL
-- Use Dictinct with Orderby to remove duplicate rows
SELECT DISTINCT ON (emp_no) emp_no,
first_name,
last_name,
title
INTO unique_titles
FROM retirement_titles
ORDER BY emp_no, title DESC;
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.2.PNG?raw=true)



## Deliverable 4: Create the Movie Database
### Deliverable Requirements:
Use your knowledge of Python, Pandas, the ETL process, code refactoring, and PostgreSQL to add the `movies_df` DataFrame and MovieLens rating CSV data to a SQL database.

1. The data from the `movies_df` DataFrame replaces the current data in the movies table in the SQL database, as determined by the `movies_query.png`.
2. The data from the MovieLens rating CSV file is added to the `ratings` table in the SQL database, as determined by the `ratings_query.png`.
3. ​The elapsed time to add the data to the database is displayed in the `ETL_create_database.ipynb` file.

 
### Results with detail analysis:

1. **The data from the `movies_df` DataFrame replaces the current data in the movies table in the SQL database, as determined by the `movies_query.png`.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````SQL
-- Follow the instructions below to complete Deliverable 1.
SELECT e.emp_no,
       e.first_name,
       e.last_name,
       t.title,
       t.from_date,
       t.to_date
INTO retirement_titles
FROM employees as e
INNER JOIN titles as t
ON (e.emp_no = t.emp_no)
WHERE (e.birth_date BETWEEN '1952-01-01' AND '1955-12-31')
order by e.emp_no;
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.1.PNG?raw=true)

2. **The data from the MovieLens rating CSV file is added to the `ratings` table in the SQL database, as determined by the `ratings_query.png`.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.1r.PNG?raw=true)

3. ​***​The elapsed time to add the data to the database is displayed in the `ETL_create_database.ipynb` file.**

> Image with `SQL`, `pgAdmin` , `QuickDBD` & `Python` Code below.

**Code and Image**


````SQL
-- Use Dictinct with Orderby to remove duplicate rows
SELECT DISTINCT ON (emp_no) emp_no,
first_name,
last_name,
title
INTO unique_titles
FROM retirement_titles
ORDER BY emp_no, title DESC;
````

![name-of-you-image](https://github.com/emmanuelmartinezs/Pewlett-Hackard-Analysis/blob/main/Resources/Images/1.2.PNG?raw=true)



#### Movies ETL Analysis Completed by Emmanuel Martinez
