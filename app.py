from urllib.parse import unquote


from chalice import Chalice

from chalicelib import api_provider

app = Chalice(app_name='barman-api-service')
app.debug = True

# drink = "AMARETTO ROSE"
# drink = unquote(drink)
# print(drink)
# api = api_provider.ApiProvider()
# print(api.get_drink(drink))
#

# import sys; print(sys.path)
@app.route('/')
def index():
    return {'hello': 'world'}


# Get full drink details
@app.route('/drink/{drink}')
def get_drink(drink):
    api = api_provider.ApiProvider()
    drink = unquote(drink)
    return api.get_drink(drink)


# Get random drink
@app.route('/drink/random')
def get_drink():
    return {'drink': 'random drink', 'instructions': 'test instructions'}


# Get specific ingredient in drink
@app.route('/drink/{drink}/{ingredient}')
def get_ingredient_in_drink(ingredient, drink):
#    ingredient = urllib.parse.unquote(ingredient)
#    drink = urllib.parse.unquote(drink)
    return {'drink': drink, 'ingredient': ingredient, 'instructions': 'test instructions'}


# Get ingredients for drink
@app.route('/ingredients/{drink}')
def get_ingredients(drink):
#   drink = urllib.parse.unquote(drink)
    return {'drinkName': drink, 'ingredients': {'ingredient1': 'ing1', 'measure1': 'mea1',
                                                'ingredient2': 'ing2', 'measure2': 'mea2',
                                                'ingredient3': 'ing3', 'measure3': 'mea3'}}


# Get specific ingredient in drink
@app.route('/test')
def get_test():
    # athena = AthenaBarman
    # return athena.get_drink_from_athena
    return {'drink': drink, 'ingredient': 'test', 'instructions': 'test instructions'}


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
