import logging
import entities
import athena_processor

class AthenaBarman:

    DrinkWithIngredients, RandomDrink, DrinkIngredient, DrinkIngredients = range(4)

    # ---------------- Athena handling -----------------
    def get_drink_from_athena(self, drink_name):
        drink = entities.DrinkEntity(drink_name)
        athena = athena_processor.AthenaProcessor(drink)

        # self.__my_logging_handler(response)
        response = athena.get_athena_results()
        return drink.process_result_rows(response)

    def __my_logging_handler(self, event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))

