from chalicelib.helpers.postgres_processor import PostgresProcessor
import json

class BarmanEntity:

    def __init__(self, name):
        self.name = name

    def _process_row(self, row, column_info_list, entity):
        item = {}

        valid_columns = entity.valid_column

        for i, column in enumerate(column_info_list):
            column_name = column_info_list[i]['Name']
            if column_name in valid_columns:
                if len(row[i]):
                    item[column_name] = row[i][list(row[0])[0]]
        return item


class DrinkEntity(BarmanEntity):

    #name = 'AMARETTO ROSE'

    valid_column = ['drink_id', 'drink_name', 'drink_category', 'drink_glass', 'drink_instructions']

    def __init__(self, name):

        super().__init__(name)

    def sql_statement(self):
        return "SELECT d.drink_id drink_id, " \
               "d.drink_name drink_name, " \
               "d.drink_category drink_category, " \
               "d.drink_glass drink_glass, " \
               "d.drink_instructions drink_instructions, " \
               "i.ingredient_id ingredient_id, " \
               "m.ingredient_measurement ingredient_ingredient_measurement, " \
               "m.ingredient_order ingredient_order, " \
               "i.ingredient_name ingredient_ingredient_name " \
               "FROM drinks d " \
               "LEFT JOIN map_drink_ingredients m " \
               "ON d.drink_id = m.drink_id " \
               "LEFT JOIN ingredients i " \
               "ON m.ingredient_id=i.ingredient_id " \
               "WHERE d.drink_id IN (SELECT MAX(d.drink_id) drink_id " \
               "FROM drinks d " \
               "WHERE UPPER(d.drink_name) LIKE UPPER('" + self.name + "')) " \
               "ORDER BY d.drink_id, m.ingredient_order"


    def get_drink_with_ingredients(self):

        postgres = PostgresProcessor(self)
        rows = postgres.execute_sql()
        #print(rows)
        return json.dumps(rows)
        # athena = athena_processor.AthenaProcessor(self)
        # query_results = athena.get_athena_results()
        # return_results = {}
        # return_results['drink'] = {}
        # drink_no = 0
        # current_drink_id = None
        #
        # column_info_list = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        #
        # results = query_results['ResultSet']
        #
        # for i, row in enumerate(results['Rows']):
        #     # Process the row. The first row of the first page holds the column names.
        #     if 0 == i:
        #         continue
        #     # First item we grab the drink
        #     #elif 1 == i:
        #     #    current_drink_id = row['Data'][0][list(row['Data'][0])[0]]
        #
        #     if current_drink_id != row['Data'][0][list(row['Data'][0])[0]]:
        #         drink_no += 1
        #         if drink_no not in return_results['drink']:
        #             return_results['drink'][drink_no] = {}
        #         drink = self._process_row(row['Data'], column_info_list, DrinkEntity)
        #         # return_results['drink'][drink_no].update(drink)
        #         return_results['drink'][drink_no] = drink
        #         current_drink_id = row['Data'][0][list(row['Data'][0])[0]]
        #
        #     ingredient = self._process_row(row['Data'], column_info_list, IngredientEntity)
        #     if 'ingredients' not in return_results['drink'][drink_no]:
        #         return_results['drink'][drink_no]['ingredients'] = {}
        #     return_results['drink'][drink_no]['ingredients'][str(i)] = ingredient
        # return return_results


class IngredientEntity:

    name = 'amaretto almond liqueur'
    sql_statement = "SELECT ingredient_id ingredient_id, name ingredient_name " \
                    "FROM ingredients i " \
                    "WHERE UPPER(i.ingredient_name) LIKE UPPER('" + name + "')"

    valid_column = ['ingredient_id', 'ingredient_name', 'ingredient_measurement', 'ingredient_order', 'ingredient_name']

    def __init__(self, name):
        self.name = name
