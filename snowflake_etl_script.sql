USE ROLE ACCOUNTADMIN;
USE DATABASE DS5559_DATA;
USE SCHEMA PUBLIC;

-- Show warehouses available, just an example of a sql command that is snowflake specific... this 
-- shoud show you the warehouses available, to start it is only the default, COMPUTE_WH
SHOW WAREHOUSES;

-- Another snowflake specific command. Note how much metadata is related to the table
-- The 'information_schema' contains info about all the tables available
SELECT * FROM information_schema.columns;

-- This should narrow it down to the PUBLIC schema
SELECT * FROM information_schema.columns WHERE "TABLE_SCHEMA" = 'PUBLIC';

-- To get just the columns from our table you have to get really specific....
SELECT "COLUMN_NAME"
FROM information_schema.columns
WHERE "TABLE_SCHEMA" = 'PUBLIC'
    AND "TABLE_NAME" = 'IRIS_DATA';
    
-- This version will FAIL... because of the specific syntax, which varies from say pyspark and snowflake...
-- Note that IRIS_DATA has double quotes... you'll see the error message "...Invalid Identifier IRIS_DATA"
SELECT "COLUMN_NAME"
FROM information_schema.columns
WHERE "TABLE_SCHEMA" = 'PUBLIC'
    AND "TABLE_NAME" = "IRIS_DATA";


--  AND NOW FOR A SHORT PROCEDURE
-- Show the procedures available under this schema, initially should only contain two defaults unrelated to user
SHOW PROCEDURES;


-- THIS is the procedure, note the first part is straight SQL followed by javascript encircled by $$
-- Place your cursor anywhere in the statement and hit <CTL>-ENTER
-- Aside from the SQL header note the following lines
--    snowflake.createStatement({sqlText: validations[idx].sql_statement}), // this is the snowflake API to LOAD a sql statement.
--                                                                          // and store it in a javascript object
--
--          res = stmt.execute();                                           // This is where we RUN the statement
--
--          row_count = res.getRowCount();                                  // As mentioned in class, one standard used for tests is 'no rows means good'
--                                                                          // so in this case we get the row count...
-- The rest of the javascript is setup and processing.  While this example is a validation, you can do just about any work
--    you want.  As mentioned in class, it is now possible to use python for these procedures too.
CREATE OR REPLACE PROCEDURE udf1()
    RETURNS string
    LANGUAGE JAVASCRIPT
    COMMENT = 'sample udf'
    EXECUTE AS CALLER
AS
$$
var validations = [
{name: `_AUDIT_DBT_TESTS_--DbtTestsPassed`, sql_statement: `SELECT * FROM IRIS_DATA WHERE 'CLASS' IS NOT NULL;`}
];
    var return_results = {};
    return_results["validation_failures"] = 0;

    return_results["fail_num"] = 0;
    return_results["pass_num"] = 0;
    return_results["exce_num"] = 0;

    return_results["total_num"] = 0;

    return_results["fail_queries"] = [];
    return_results["exce_queries"] = [];

    return_results["pass_names"] = [];
    return_results["fail_names"] = [];
    return_results["exce_names"] = [];

    return_results["exce_err_messages"] = [];

    for (idx in validations) {
        stmt = snowflake.createStatement({sqlText: validations[idx].sql_statement})
        return_results["total_num"] += 1;

        try {
            res = stmt.execute();
            row_count = res.getRowCount();
            if (row_count === 0) {
                return_results["pass_names"].push(validations[idx].name);
                return_results["pass_num"] += 1;
            } else {
                return_results["fail_queries"].push(validations[idx].sql_statement);
                return_results["fail_names"].push(validations[idx].name);
                return_results["fail_num"] += 1;
                return_results["validation_failures"] += 1;
            }
        } catch(err) {
                return_results["exce_queries"].push(validations[idx].sql_statement);
                return_results["exce_err_messages"].push(err.message);
                return_results["exce_names"].push(validations[idx].name);
                return_results["exce_num"] += 1;
                return_results["validation_failures"] += 1;
        }
    }
    return JSON.stringify(return_results);
$$;

-- You should now see the procedure listed
SHOW PROCEDURES;

-- Call it and you should see the result of the return from javascript
CALL udf1();

-- This should get you the text, run this and click on the last rows value for 'body'
DESCRIBE PROCEDURE udf1();

-- showing off some sql on your table, ... for example get the counts of different names,
-- there's like 50 each...  (Screenshot should cover the SQL and resulting table displayed)
SELECT DISTINCT (CLASS), COUNT (*) as "Class Counts" FROM IRIS_DATA
GROUP BY CLASS;

-- more advanced use of SQL, maybe dig through the snowflake docs 
-- and see if you can get the average of each of the four numeric fields. 
SELECT CLASS, AVG(SEPAL_LENGTH) as "Average Sepal Length",AVG(SEPAL_WIDTH) as "Average Sepal Width",
AVG(PETAL_LENGTH) as "Average Petal Length", AVG(PETAL_WIDTH) as "Average Petal Width" FROM IRIS_DATA
GROUP BY CLASS
ORDER BY "Average Sepal Length" DESC;


-- Above and beyond
CREATE OR REPLACE PROCEDURE BuzzLightYear(TABLE_NAME VARCHAR, COL_NAMES ARRAY)
    RETURNS VARIANT NOT NULL
    LANGUAGE JAVASCRIPT
    COMMENT = 'above and beyond'
    EXECUTE AS CALLER
AS
$$

// Run SQL statement(s) and get a resultSet.
var command = "SELECT DISTINCT (CLASS), COUNT (*) as \"Class Counts\" FROM " + TABLE_NAME+" GROUP BY CLASS;";
var cmd1_dict = {sqlText: command};
var stmt = snowflake.createStatement(cmd1_dict);
var result = stmt.execute();

// This variable will hold a JSON data structure that holds ONE row.
var row_as_json = {};

// This array will contain all the rows.
var array_of_rows = [];

// This variable will hold a JSON data structure that we can return as
// a VARIANT.
// This will contain ALL the rows in a single "value".
var table_as_json = {};

    
// Read each row and add it to the array we will return.
var row_num = 1;
while (result.next())  {
  // Put each row in a variable of type JSON.
  row_as_json = {};
  
  // For each column in the row...
  for (var col_num = 0; col_num < COL_NAMES.length; col_num = col_num + 1) {
    var col_name = COL_NAMES[col_num];
    row_as_json[col_name] = result.getColumnValue(col_num + 1);
  }
  
  
  // Add the row to the array of rows.
  array_of_rows.push(row_as_json);
  ++row_num;
}

// Put the array in a JSON variable (so it looks like a VARIANT to
// Snowflake).  The key is "key1", and the value is the array that has
// the rows we want.
table_as_json = { "key1" : array_of_rows };

// Return the rows to Snowflake, which expects a JSON-compatible VARIANT.
return table_as_json;
$$;


SHOW PROCEDURES;

call BuzzLightYear();

CALL BuzzLightYear(
  -- Table name
  'IRIS_DATA',
  -- Array of column names.
  ARRAY_APPEND(TO_ARRAY('CLASS'), 'Class Counts')
);

SELECT VALUE:"CLASS" AS "Class", value:"Class Counts" AS "Class Counts"
    FROM (SELECT $1:key1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))) AS res,
    LATERAL FLATTEN(input => res.$1)
    ORDER BY "Class";