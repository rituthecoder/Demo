# Demo

a.	Lambda environment variables is used to help setup region specific configuration of Lambda dependencies (if needed)
For example:
{“CustomEnvironment”}: {"dynamodbTableName":"work_orders"}
This helps us read the DynamoTable name for this specific region, especially helpful if other region has different name for the table.
