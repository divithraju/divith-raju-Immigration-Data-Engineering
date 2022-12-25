# Immigration Data Dictionary
The data model consists of a fact table and 7 dimension tables to cover the analysis needs and purposes. It's designed as Star schema and structured as the following:

## Fact Table
A fact table consists of the measurements, metrics or facts of a business process, which is the immigration here.

### Immigration Table
It tracks immigration instances information regarding several dimensions, measures, and facts.

| Feature  | Description
| --------|-----------
| (PK) cicid | Unique identifier 
| (FK) date_id_arrdate | Arrival Date in the USA
| (FK) date_id_depdate | Departure Date from the USA
| (FK) country_code_i94cit | 3 digit code for immigrant country of citizenship
| (FK) country_code_i94res | 3 digit code for immigrant country of residence
| (FK) iata_code_i94port | Port of admission
| i94mode | Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported)
| (FK) state_code_i94addr | USA State of arrival
| (FK) visa_id | Identifier for visa instance info.
| (FK) status_flag_id | Identifier for status instance info.
| (FK) migrant_id | Identifier for migrant instance info.
| airline | Airline used to arrive in U.S.
| admnum | Admission Number
| fltno | Flight number of Airline used to arrive in U.S.


## Dimension Tables
A dimension table stores attributes, features, that describe the objects in a fact table. It describes an aspect of a business process.

### 1) Migrant Table
It tracks different migrant instances information, it also specifies the migrant object in the fact table (migrant_id).

| Feature  | Description
| --------|-----------
| (PK) migrant_id | Unique identifier 
| birth_year | 4 digit year of birth
| gender | Non-immigrant sex

### 2) Status Table
It tracks different status immigration instances information, it also specifies the status object in the fact table (status_flag_id).

| Feature  | Description
| --------|-----------
| (PK) status_flag_id | Unique identifier 
| arrival_flag | Arrival flag - admitted or paroled into the U.S.and others
| departure_flag | Departure flag - Departed, lost I-94 or is deceased and others
| match_flag | Match flag - Match of arrival and departure records

### 3) Visa Table
It tracks different visa immigration instances information, it also specifies the visa object in the fact table (visa_id).

| Feature  | Description
| --------|-----------
| (PK) visa_id | Unique identifier 
| i94visa | Visa codes collapsed into three categories
| visapost | Department of State where Visa was issued
| visatype | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

### 4) Demographics Table
It tracks different U.S states demographics information, it also specifies the demographics object in the fact table (state_code_i94addr).

| Feature  | Description
| --------|-----------
| (PK) state_code | Code of the state 
| state | US State
| median_age | The median population age
| male_population | Male population total
| female_population | Female population total
| total_population | Total population
| num_foreign_born | Number of residents who were not born in the state
| avg_household_size | Average size of houses in the state

### 5) Airport Table
It tracks different airports information, it also specifies the airport object in the fact table (iata_code_i94port).

| Feature  | Description
| --------|-----------
| (PK) iata_code | Airport IATA Code
| type | Airport type
| ident | airport random identifier
| name | Airport name
| elevation_ft | Airport altitude
| continent | The continent where the airport is located
| iso_country | ISO Code of the airport's country
| iso_region | ISO Code for the airport's region
| municipality | City/Municipality where the airport is located
| gps_code | Airport GPS Code
| local_code | Airport local code

### 6) Country Temperature Table
It tracks different Temperature and country code information, it also specifies the Temperature and country object in the fact table (country_code_i94cit & country_code_i94res).

| Feature  | Description
| --------|-----------
| (PK) country_code | Country code
| country_name | Country name
| average_temperature | Average temperature in celsius
| average_temperature_uncertainty | 95% confidence interval around average temperature

### 7) Date Table
It tracks different date information, it also specifies the date object in the fact table (date_id_arrdate & date_id_depdate).

| Feature  | Description
| --------|-----------
| (PK) date_id | Unique Identifier 
| date | Date
| year | Year of the date
| month | Month of the date
| day | Day of the date within the month
| weekday | Number of the day within the week











