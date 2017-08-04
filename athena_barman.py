import boto3
import logging
import time


class AthenaBarman:

    DrinkWithIngredients, RandomDrink, DrinkIngredient, DrinkIngredients = range(4)

    # ---------------- Athena handling -----------------
    def get_drink_from_athena(self, drink):
        client = boto3.client('athena')
        sql_drink = self.__get_sql_statement(self.DrinkWithIngredients, "AMARETTO ROSE")
        query_execution_id = self.__submit_athena_query(client, sql_drink)
        self.__wait_for_athena_query_complete(client, query_execution_id)

        # self.__my_logging_handler(response)
        response = self.__process_result_rows(client, query_execution_id)
        return response

    def __submit_athena_query(self, client, query):

        # query = 'SELECT * FROM drinks_db.ingredients;' ## WHERE iddrink = 12652;'
        # query = "SELECT * FROM drinks_db.drinks d " \
        #         " LEFT JOIN drinks_db.map_ingredient_drink m " \
        #         " ON d.iddrink = m.drinkid " \
        #         " LEFT JOIN drinks_db.ingredients i " \
        #         " ON m.ingredientid = i.ingredientid " \
        #         " WHERE UPPER(d.strdrink) LIKE '%APPLE%';"

        self.__my_logging_handler(query)
        response = client.start_query_execution(
            QueryString=query,
            # ClientRequestToken='string',
            QueryExecutionContext={
                'Database': 'drinks_db'
            },
            ResultConfiguration={
                'OutputLocation': 's3://drinkslistvir/results/'
            }
        )
        return response['QueryExecutionId']

    def __wait_for_athena_query_complete(self, client, query_execution_id):

        is_query_still_running = True
        while is_query_still_running:
            state_response = client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            self.__my_logging_handler(state_response)
            state = state_response['QueryExecution']['Status']['State']

            if state == 'FAILED':
                self.__my_logging_handler(state_response)
                is_query_still_running = False
            elif state == 'CANCELED':
                self.__my_logging_handler(state_response)
                is_query_still_running = False
            elif state == 'SUCCEEDED':
                self.__my_logging_handler(state_response)
                is_query_still_running = False
            time.sleep(1)

            # if (queryState.equals(QueryExecutionState.FAILED).toString()) {
            #     throw new RuntimeException("Query Failed to run with Error Message: "
            #  + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            # }
            # else if (queryState.equals(QueryExecutionState.CANCELED.toString())) {
            #     throw new RuntimeException("Query was cancelled.");
            # }
            # else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
            #    isQueryStillRunning = false;
            # }
            # // Sleep an amount before retrying again.
            # System.out.println("Current Status is: " + queryState);
            # Thread.sleep(ExampleConstants.SLEEP_AMOUNT_IN_MS);

    def __process_athena_results(self, client, query_execution_id):
        result = client.get_query_results(
            QueryExecutionId=query_execution_id
        )
        return result

    def __get_sql_statement(self, query_type, drinkname):
        if query_type == self.DrinkWithIngredients:
            sql = "SELECT * FROM drinks_db.drinks d " \
                "LEFT JOIN drinks_db.map_ingredient_drink m " \
                "ON d.iddrink = m.drinkid " \
                "LEFT JOIN drinks_db.ingredients i " \
                "ON m.ingredientid = i.ingredientid " \
                "WHERE d.iddrink IN (SELECT MAX(d.iddrink) iddrink " \
                "FROM drinks_db.drinks d " \
                "WHERE UPPER(d.strdrink) LIKE '%" + drinkname + "%')"
        elif query_type == self.RandomDrink:
            sql = "n is a perfect square\n"
        elif query_type == self.DrinkIngredient:
            sql = "n is an even number\n"
        elif query_type == self.DrinkIngredients:
            sql = "n is a prime number\n"
        else:
            sql = "SELECT 1"
        return sql

    def __process_result_rows(self, client, query_execution_id):

        query_results = self.__process_athena_results(client, query_execution_id)
        print query_results

        column_info_list = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        is_first_row_processed = False
        while True:

            results = query_results['ResultSet']

            for row in results:
                if not is_first_row_processed:
                    # Process the row. The first row of the first page holds the column names.
                    continue
                # processRow(row, columnInfoList)

            # If the nextToken is null, there are no more pages to read. Break out of the loop.
            next_token = query_results.get('NextToken', None)
            if next_token is None:
                break

            query_results = client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token
            )
#
# private static void processRow(Row row, List<ColumnInfo> columnInfoList)
# {
# for (int i = 0; i < columnInfoList.size(); ++i) {
# switch (columnInfoList.get(i).getType()) {
# case "varchar":
# // Convert and Process as String
# break;
# case "tinyint":
# // Convert and Process as tinyint
# break;
# case "smallint":
# // Convert and Process as smallint
# break;
# case "integer":
# // Convert and Process as integer
# break;
# case "bigint":
# // Convert and Process as bigint
# break;
# case "double":
# // Convert and Process as double
# break;
# case "boolean":
# // Convert and Process as boolean
# break;
# case "date":
# // Convert and Process as date
# break;
# case "timestamp":
# // Convert and Process as timestamp
# break;
# default:
# throw new RuntimeException("Unexpected Type is not expected" + columnInfoList.get(i).getType());
# }
# }
# }





    def __my_logging_handler(self, event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))

