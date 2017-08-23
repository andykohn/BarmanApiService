from chalicelib import athena_processor


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
        return "SELECT d.iddrink drink_id, " \
               "d.strdrink drink_name, " \
               "d.strcategory drink_category, " \
               "d.strglass drink_glass, " \
               "d.strinstructions drink_instructions, " \
               "i.ingredientid ingredient_id," \
               "m.measurement ingredient_measurement, " \
               "m.iorder ingredient_order, " \
               "i.name ingredient_name " \
               "FROM drinks_db.drinks d " \
               "LEFT JOIN drinks_db.map_ingredient_drink m " \
               "ON d.iddrink = m.drinkid " \
               "LEFT JOIN drinks_db.ingredients i " \
               "ON m.ingredientid = i.ingredientid " \
               "WHERE d.iddrink IN (SELECT MAX(d.iddrink) iddrink " \
               "FROM drinks_db.drinks d " \
               "WHERE UPPER(d.strdrink) LIKE UPPER('" + self.name + "')) " \
               "ORDER BY d.iddrink, m.iorder"

    def get_drink_with_ingredients(self):

        athena = athena_processor.AthenaProcessor(self)
        query_results = athena.get_athena_results()
        return_results = {}
        return_results['drink'] = {}
        drink_no = 0
        current_drink_id = None

        column_info_list = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']

        results = query_results['ResultSet']

        for i, row in enumerate(results['Rows']):
            # Process the row. The first row of the first page holds the column names.
            if 0 == i:
                continue
            # First item we grab the drink
            #elif 1 == i:
            #    current_drink_id = row['Data'][0][list(row['Data'][0])[0]]

            if current_drink_id != row['Data'][0][list(row['Data'][0])[0]]:
                drink_no += 1
                if drink_no not in return_results['drink']:
                    return_results['drink'][drink_no] = {}
                drink = self._process_row(row['Data'], column_info_list, DrinkEntity)
                # return_results['drink'][drink_no].update(drink)
                return_results['drink'][drink_no] = drink
                current_drink_id = row['Data'][0][list(row['Data'][0])[0]]

            ingredient = self._process_row(row['Data'], column_info_list, IngredientEntity)
            if 'ingredients' not in return_results['drink'][drink_no]:
                return_results['drink'][drink_no]['ingredients'] = {}
            return_results['drink'][drink_no]['ingredients'][str(i)] = ingredient
        return return_results



class IngredientEntity:

    name = 'amaretto almond liqueur'
    sql_statement = "SELECT ingredientid ingredient_id, name ingredient_name " \
                    "FROM drinks_db.ingredients i " \
                    "WHERE UPPER(i.name) LIKE UPPER('" + name + "')"

    valid_column = ['ingredient_id', 'ingredient_name', 'ingredient_measurement', 'ingredient_order', 'ingredient_name']

    def __init__(self, name):
        self.name = name
