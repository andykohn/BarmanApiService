from chalicelib.helpers.postgres_processor import PostgresProcessor
from itertools import groupby
from operator import itemgetter
import logging

class BarmanEntity(object):

    def __init__(self, name):
        self.name = name


class DrinkEntity(BarmanEntity):

    valid_columns = ['drink_id', 'drink_name', 'drink_category', 'drink_glass', 'drink_instructions']

    def __init__(self, name):
        super(DrinkEntity, self).__init__(name)

    def sql_drink_with_ingredients(self):
        return "SELECT d.drink_id drink_id, " \
               "d.drink_name drink_name, " \
               "d.drink_category drink_category, " \
               "d.drink_glass drink_glass, " \
               "d.drink_instructions drink_instructions, " \
               "i.ingredient_id ingredient_id, " \
               "m.ingredient_measurement ingredient_measurement, " \
               "m.ingredient_order ingredient_order, " \
               "i.ingredient_name ingredient_name " \
               "FROM drinks d " \
               "LEFT JOIN map_drink_ingredients m " \
               "ON d.drink_id = m.drink_id " \
               "LEFT JOIN ingredients i " \
               "ON m.ingredient_id=i.ingredient_id " \
               "WHERE d.drink_id IN (SELECT MAX(d.drink_id) drink_id " \
               "FROM drinks d " \
               "WHERE UPPER(d.drink_name) LIKE UPPER('{}')) " \
               "ORDER BY d.drink_id, m.ingredient_order".format(self.name)

    def get_drink_with_ingredients(self):
        self.__my_logging_handler('DEBUG-1')
        postgres = PostgresProcessor(self.sql_drink_with_ingredients())
        rec_rows = postgres.execute_sql()
        self.__my_logging_handler('DEBUG-2')
        return_results = {'drinks': {}}
        grouper = itemgetter(*self.valid_columns)
        for key, grp in groupby(rec_rows, grouper):
            return_results['drinks'][key[0]] = dict(zip(self.valid_columns, key))
            for item in grp:
                if 'ingredients' not in return_results['drinks'][key[0]]:
                    return_results['drinks'][key[0]]['ingredients'] = {}
                # Grab only the ingredients column
                ingredients = dict((k, v) for k, v in item.dataset.dict[0].items()
                                   if k in IngredientEntity.valid_columns)

                return_results['drinks'][key[0]]['ingredients'][item['ingredient_id']] = ingredients
        self.__my_logging_handler('DEBUG-3')
        return return_results

    def __my_logging_handler(self, event):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logger.info('DEBUG:{}'.format(event))

class IngredientEntity(DrinkEntity):

    name = 'amaretto almond liqueur'
    sql_statement = "SELECT ingredient_id ingredient_id, name ingredient_name " \
                    "FROM ingredients i " \
                    "WHERE UPPER(i.ingredient_name) LIKE UPPER('" + name + "')"

    valid_columns = ['ingredient_id', 'ingredient_name', 'ingredient_measurement', 'ingredient_order']

    def __init__(self, name):
        super(DrinkEntity, self).__init__(name)
