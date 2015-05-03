from database import DataBase

db = DataBase()

command = None
while command != 'END':
    command = raw_input()
    try:
        result = db.execute(command)
        print(result if result else '')
    except ValueError as ex:
        print(ex)