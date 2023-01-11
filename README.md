## WHAT ? 

Validly is a fast-api application that helps to validate specific domain of columns in csv datasets with the help of Great-expectation

------ 

## WHY ? 

Lets consider an arbitrary dataset which gives you detail about Agriculture production every year : 

|year|category|value|unit|note|
|-|-|-|-|-|
|2009|Rice|150|Metric Tonne||
|2009-10|Wheat|175|value in Metric Tonne||
|2009|Pulses|"110"|value in Metric Tonne||

Suppose , if there is no data-quality check after, data-cleaning then the would be certain issues that an analyst might face like : 
- There are mixed representation of year `2009` and `2009-10` 
- Numeric Values are represented as string as `"110"` for pulses
- No consistent manner of representing units, 
..... *and the list may goes on*

So, ideally the dataset should look like : 

|year|category|value|unit|note|
|-|-|-|-|-|
|2009|Rice|150|value in Metric Tonne||
|2009|Wheat|175|value in Metric Tonne||
|2009|Pulses|110|value in Metric Tonne||

If there would be a tool where a user can upload its `dataset / csv` file and can figure out its potential problems then data-cleaning can be revisited and a much proper dataset can be acquired.

------

## HOW ? 

#### To run the application ? 

```
cd validly
docker compose up
```
> Serveer will be up and running at :  http://localhost:8000


#### To stop the application ? 

- Command to stop Validly 

```
docker compose stop
```

- Command to stop the application and remove all the containers and networks that were created:

```
docker compose down
```

----------------------------------------------------------------

# Metadata Validations


## WHAT ? 

Validly is a fast-api application that helps to validate specific domain of columns in csv metadata datasets with the help of Great-expectation

------ 

## WHY ? 

Lets consider an arbitrary metadata sheet which gives you detail about AISHE : 

|sector|organization|short_form|....|time_saved_in_hours|price|
|-|-|-|-|-|-|
|Educations||AISHE|....|4|1996|

Suppose , if there is no data-quality check after, data-cleaning then there would be certain issues that an analyst might face like : 
- Sector, organization, etc columns values should take only few expected values only
- Time Saved in Hours should be in the range of 2 - 6 hours
- There should be few columns which doesnt accept null values 
..... *and the list may goes on*

So, ideally the dataset should look like : 

|sector|organization|short_form|....|time_saved_in_hours|price|
|-|-|-|....|-|-|
|Education|All India Survey on Higher Education|AISHE|....|4|1996|

If there would be a tool where a user can upload its `metadata/csv` file and can figure out its potential problems then metadata sheet can be revisited and updated properly.

------