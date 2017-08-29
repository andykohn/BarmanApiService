import logging

from chalicelib import entities


class ApiProvider:

    DrinkWithIngredients, RandomDrink, DrinkIngredient, DrinkIngredients = range(4)

    # ---------------- Athena handling -----------------
    def get_drink(self, drink_name):
        drink_entity = entities.DrinkEntity(drink_name)
        return drink_entity.get_drink_with_ingredients()

    def __my_logging_handler(self, event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))

