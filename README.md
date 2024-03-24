
# Data Engineering on Sample Health Data usign Airflow

EDA, pipelining and Analysis on a sample health care data



# Usage
1. Clone the repository and make it working directory.

### Data Assessment and Analysis

1. Open the notbook `Data Analysis Health Care.ipynb` and run data Assessment and Data Analysis on merged data once you run the pipeline. Make sure jupyter notebook is installed or you can use Google Colab to run the notebook.

### Data pipeline using Apache Airflow

1. create a python virtual environment `$python -m venv .airflow_venv` 

3. Activate virtual environment `$source .airflow_venv/bin/activate`
4. Upgrade pip `$pip install --upgrade pip`
4. Install requirements `$pip install requirements.txt` or flollow the Airflow [docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi) to install Airflow. Some important commands are as flollows 

    a. `$export AIRFLOW_HOME="$(pwd)"`

    b. then pip install by following docs

    c. `$airflow db init`
    
    d. `$airflow db migrate`
    
    e. `airflow users create \
    --username admin \
    --firstname Younis \
    --lastname Ali \
    --role Admin \
    --email man@super.org`

    f. `$airflow standalone`

5. Then visit http://0.0.0.0:8080/dags/health_data/grid to trigger the pipeline.

6. Before trigger the pipeline make sure you have required files in `airflow/data/` directory in the given format.
# Data Assessment
## Database schema
Visit the link to see database schema that gives sense how different tables are linked [Database Schema](https://miro.com/app/board/uXjVKe8-jzA=/?share_link_id=25922620625)

### EDA

I am doing EDA and Data Validation on         Patients data only. The same mechanism can be used  for other four data sets, though there will be a little difference in implementation but the core concept remains the same.

### Great Expectations
    
I used great expectations to add expectations or rules on patients data. I get the json file of expectations and saved the file in `./gx/expectations` folder. This json can be used later on to validate the data to check and examine the dtaset according to our set expectations.

#### Note 

I did the EDA and data corerectness only on `patients` data same can be followed on other datasets.




# Data Pipelining

### 1 Connect datasets

Using dataframes I read 6 different datasets in respective dataframes. I use XCom to return dataframes as json so that dataframes can be used by another tasks in the pipeline.

### 2. Clean data

In cleaning process I check the data quality issues using great expectations. There is a lot of scope for data cleaning like managing null values, handling data types etc. I resolved two main issues:

1.  In `patients` raw data the `GENDER` column has null values so I used `patient_gender` meta data to update the `patient` data with actual values.
2. `Symptoms` table, that corresponds to `Observation` in Tuva model has no primary key so I added the PK to this table.
3. I drop the columns from datasets that didnot match the Tuva model schema.

### 3. Structure
1. Check the schema of the tuvap roject for each dataset.
2. Drop the columns from the given datasets that are not in the respective schema in tuva input layer schema.
3. There is file `./resources/tuva_schema_map.json`, this file contains the mapping of our dataset feilds with tuva shema. This will enable user to handle the mapping eassily by simply configuring this file.
4. Rename the remaining columns in each dataset and then dump the poressed csv files into `processed_data` directory. Though the pipeline gets data of different types, but we use the single type, `csv` to store the data.
5. In future we can use the open database like `postgresssql` to read and save the data in the pipeline.

### 4. Merge
1. On the basis of patient_id we merge the whole data into a singe csv `./processed_data/merged_data`
3. This merged data can be accessed in jupyter notebook for Data Analysis purpose.



## Data Analysis
### Tools and technologies used in this project
1. [Miro](https://miro.com/). I used this tool to create the ER schema of Database that given the sense how different realtions are linked together

2. [Great Expectations](https://legacy.docs.greatexpectations.io/en/latest/). I used great expectations, an open source python based framework to validate data and maintain data quality and corectness.

3. [Pandas](https://pandas.pydata.org/). Pandas a data manipulation pyhton based framework is used to perform basic level of EDA