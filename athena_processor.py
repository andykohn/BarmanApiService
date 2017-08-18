import boto3
# import entities
import logging
import time


class AthenaProcessor:

    def __init__(self, entity):
        self.__client = boto3.client('athena')
        self.__entity = entity
        self.__database = 'drinks_db'
        self.__output_location = 's3://drinkslistvir/results/'

    def __submit_athena_query(self):

        # self.__my_logging_handler(query)
        response = self.__client.start_query_execution(
            QueryString=self.__entity.sql_statement(),
            QueryExecutionContext={
                'Database': self.__database
            },
            ResultConfiguration={
                'OutputLocation': self.__output_location
            }
        )
        return response['QueryExecutionId']

    def __wait_for_athena_query_complete(self, query_execution_id):

        is_query_still_running = True
        while is_query_still_running:
            state_response = self.__client.get_query_execution(
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
            else:
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

    def get_athena_results(self):

        query_execution_id = self.__submit_athena_query()
        self.__wait_for_athena_query_complete(query_execution_id)

        result = self.__client.get_query_results(
                QueryExecutionId=query_execution_id
            )
        return result

    def __my_logging_handler(self, event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))