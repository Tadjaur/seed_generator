#!/usr/bin/python3.6
import threading
import multiprocessing
from functools import reduce
from time import sleep
from time import strftime
import mysql.connector
import json
from os.path import abspath, join, dirname
import random
import sys
import time
import itertools
import psycopg2

user_input = None


def func():
    global user_input
    user_input = input(":\t")


class TaurSeedGenerator:
    def __init__(self, args):
        self._is_psql = True
        if self.main(args) == -1:
            exit()
        self.init_database_connection()

    __SGBD = ['mysql', 'psql']  # ['mysql', 'sqlite', 'postgres']
    # initialisation du fichier de configuration
    u_config = {
        "dbms": "mysql",
        "db": None,
        "user": None,
        "password": None,
        "host": "127.0.0.1",
        "port": 3306,
        "ignore": [],
        "len_row": 20,
        "equal": {},
        "choice": {},
        "combine": {},
        "border": {
            "_def": [0, 10000000]
        },
        "unit": {
            "_def": 1
        }
    }
    # initialisation des variables local
    _db_cursor = _db_connector = None
    queue = _finalseed = []

    def appendExec(self, target=None, args=(), kwargs=None):
        if kwargs is None:
            kwargs = {}
        if target:
            idx = 0
            while idx < len(self.queue):
                if (not self.queue[idx]) or (not self.queue[idx].is_alive()):
                    if self.queue[idx]:
                        self.queue[idx].terminate()
                    process = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
                    self.queue[idx] = process
                    self.queue[idx].start()
                    break
                idx = idx + 1
            if idx == len(self.queue):
                sleep(2)
                self.appendExec(target, args, kwargs)

    def waitAllFinish(self):
        alldone = 0
        while alldone < len(self.queue):
            sleep(5)
            alldone = 0
            for process in self.queue:
                if (not process) or process.exitcode or (not process.is_alive()):
                    alldone = alldone + 1

    @staticmethod
    def full_path(filename):
        return abspath(join(dirname(__file__), filename))

    def files(self):
        return {
            'first:homme': self.full_path('homme.taur'),
            'first:femme': self.full_path('femme.taur'),
            'last': self.full_path('prenom.taur'),
            'word': self.full_path('words.taur'),
        }

    def get_name(self, val=0, typ=None):
        selected = filename = None
        if typ is not None:
            typ = typ.lower()
            if str(typ).__contains__('pseudo') \
                    or str(typ).__contains__('ickname') \
                    or str(typ).__contains__('prenom'):
                val = 1
            elif str(typ).__contains__('name') or str(typ).__contains__('nom'):
                val = random.randint(2, 3)
        if val == 0:  # c'est un mot chercher
            # selected = random.randint(0, 455000)
            selected = random.randint(0, 53)
            filename = self.files()['word']
        elif val == 1:  # c'est un prenom rechercher
            # selected = random.randint(0, 88000)
            selected = random.randint(0, 53)
            filename = self.files()['last']
        elif val == 2:  # c'est un homme rechercher
            # selected = random.randint(0, 12000)
            selected = random.randint(0, 53)
            filename = self.files()['first:homme']
        elif val == 3:  # c'est un prenom rechercher
            # selected = random.randint(0, 42000)
            selected = random.randint(0, 53)
            filename = self.files()['first:femme']
        with open(filename) as name_file:
            c = 0
            namer = ["", ""]
            for line in name_file:
                c = c + 1
                namer.append(line.strip().split()[0])
                if c > selected:
                    if val == 0:
                        if (c > 5) and not str(typ).__contains__('titre') and not str(typ).__contains__('title'):
                            return reduce(lambda x, y: str(x) + " " + str(y), namer[-7:])
                        return reduce(lambda x, y: str(x) + " " + str(y), namer[-2:])
                    else:
                        return namer[-1]
        return "taur string"  # Return empty string if file is empty

    @staticmethod
    def get_doc():
        res = "*" * 10 + "Taur seed generator" + "*" * 10
        res = res + "\n sgbd\tpour specifier le gestionaire de base de donnee. NB: si ommit 'mysql' sera utiliser"
        res = res + "\n -u\t\tpour specifier le nom de l'utilisateur de la base de donnee. ce parametre est requis"
        res = \
            res + "\n -h\t\tpour specifier l'address hote de la base de donnee. NB:si ommit 'localhost' sera utiliser"
        res = res + "\n -p\t\tpour specifier le mot de passe de l'utilisateur de la base de donnee."
        res = res + "\n -db\tpour specifier la base de donnee a utiliser. ce parametre est requis"
        res = res + "\n -l\t\tpour specifier la limite de donnee a inserer. sit omit la limit sera de 20"
        res = res + "\n -i\t\tpour specifier la liste des tables a ignore pendant l'insertion."
        res = res + "\n\t\tsi ce parametre est ommit, toute les tables soront modifier."
        res = res + "\n\t\tNB: on souhaite souvant ignorer les tables detier pour les frameworks"
        res = res + "\n\nexample:\n\tpython3 t_g_seed.py ? "
        res = res + "\n\tpython3 t_g_seed.py -conf ~/config.json"
        res = res + "\n\tpython3 t_g_seed.py -o -conf ./config.json"
        res = \
            res + "\n\nexample configuration:" + \
            '\n{\
        \n\t"dbms": "mysql",\
        \n\t"db": test,\
        \n\t"user": test,\
        \n\t"password": test,\
        \n\t"host": "127.0.0.1",\
        \n\t"port": 3306,\
        \n\t"ignore": ["SequelizeMeta"],\
        \n\t"len_row": 50,\
        \n\t"equal": { \
        \n\t    "your_colone": 0\
        \n\t    "your_colone": "test"\
        \n\t},\
        \n\t"choice": { \
        \n\t    "your_colone": ["val1", "val2", "val3"]\
        \n\t    "your_colone": [1, 5, 3]\
        \n\t},\
        \n\t"combine": { \
        \n\t    "your_colone":{ \
        \n\t        "val":[1,2,3,5,5,6,7,8,9]\
        \n\t        "join":"-"\
        \n\t    }\
        \n\t    "your_colone":{ \
        \n\t        "val":[[1,2],[3],[5],[5,6,7],8,9]\
        \n\t        "join":[]\
        \n\t    }\
        \n\t},\
        \n\t"border": {\
        \n\t    "_def": [0, 10000000]\
        \n\t    "your_colone": [5000, 10000000]\
        \n\t},\
        \n\t"unit": {\
        \n\t    "_def": 1\
        \n\t    "your_colone": 500\
        \n\t}\
    \n} '
        res = res + "\n\nNB: tout autre parametre sera ignorer\n"
        return res + "*" * 39

    def main(self, args):
        sleep(1)
        print(args)
        if args.__len__() <= 1:
            return self.loadConfig()
        if args.__contains__('?'):
            print(self.get_doc())
            return -1
        if args.__contains__('-conf'):
            try:
                idx = args.index('-conf')
                if len(args) > idx + 1:
                    return self.loadConfig(args[idx + 1] or None)
                else:
                    print("Erreur: parametre de commande incorrecte")
            except Exception as e:
                print(e)
                print("Erreur: fichier de configuration incorrecte ou introuvavble")
        return 0

    @staticmethod
    def get_arg_value(idx, args):
        arg = str(args[idx]).split('"')
        if arg.__len__() == 3:
            arg = arg[1]
        else:
            arg = arg[0].split("'")
            if arg.__len__() == 3:
                arg = arg[1]
            else:
                arg = arg[0]
        return arg

    def special_reduce(self, stri):
        if type(self.u_config['combine'][stri]['join']) is str:
            return lambda _x, y: str(_x) + self.u_config['combine'][stri]['join'] + str(y)
        if type(self.u_config['combine'][stri]['join']) is list:
            return lambda _x, y: (_x if type(_x) is list else [_x]) + self.u_config['combine'][stri]['join'] + (
                y if type(y) is list else [y])
        return lambda _x, y: _x + self.u_config['combine'][stri]['join'] + y

    @staticmethod
    def generate(val, ln):
        if val == 0:  # generer un nombre
            return random.randint(0, 10 ** ln)
        elif val == 1:  # generer une date
            return strftime('%Y-%m-%d %H:%M:%S')
        elif val == 3:  # generer une chaine de charactere
            pass

    def get_config(self, strin, who=1):
        if self.u_config['equal'].__contains__(strin):
            return self.u_config['equal'][strin]

        if self.u_config['choice'].__contains__(strin):
            return random.choice(self.u_config['choice'][strin])

        if self.u_config['combine'].__contains__(strin):
            nb = random.randint(2, len(self.u_config['combine'][strin]['val']))
            a = list(itertools.combinations(self.u_config['combine'][strin]["val"], nb))
            return reduce(self.special_reduce(strin), random.choice(a))

        if who == 0:
            return self.get_name(0, strin)

        if self.u_config['unit'].__contains__(strin):
            unit_key = self.u_config['unit'][strin]
        else:
            unit_key = self.u_config['unit']['_def']
        if self.u_config['border'].__contains__(strin):
            a = round(self.u_config['border'][strin][0] / unit_key)
            b = round(self.u_config['border'][strin][1] / unit_key)
            return unit_key * random.randint(a, b)
        else:
            a = round(self.u_config['border']['_def'][0] / unit_key)
            b = round(self.u_config['border']['_def'][1] / unit_key)
            return unit_key * random.randint(a, b)

    def addseed(self, tb, args, fin):
        finalseed = []
        idx = len(finalseed)
        finalseed.append([tb, [], [], None])
        print("add seed table", tb)
        for pos in range(self.u_config['len_row']):
            finalseed[idx][1].append({})
            for arg in args:
                if arg[3] == "PRI":
                    typ = arg[1][:3]
                    if finalseed[idx][3] is None:
                        finalseed[idx][3] = arg[0]  # on affecte cette cle primaire al la table
                    if pos == 0:
                        if typ == 'var':
                            finalseed[idx][1][pos][arg[0]] = self.get_name(0, arg[0])
                        elif typ == 'dat':
                            finalseed[idx][1][pos][arg[0]] = strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            finalseed[idx][1][pos][arg[0]] = 1
                    else:
                        if typ == 'dat':
                            finalseed[idx][1][pos][arg[0]] = strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            old = finalseed[idx][1][pos - 1][arg[0]]
                            finalseed[idx][1][pos][arg[0]] = old + str(1) if type(old) is str else old + 1
                elif arg[3] == "MUL":
                    # cle secondaire detecter
                    if not finalseed[idx][2].__contains__(arg[0]):
                        finalseed[idx][2].append(
                            arg[0])  # on ajoute cette cle secondaire a la table si elle nexiste pas deja
                    finalseed[idx][1][pos][arg[0]] = random.randint(1, self.u_config['len_row'])
                elif arg[3] == "UNI":
                    typ = arg[1][:3]
                    if pos == 0:
                        if typ == 'var':
                            finalseed[idx][1][pos][arg[0]] = self.get_config(arg[0], 0)
                        elif typ == 'dat':
                            finalseed[idx][1][pos][arg[0]] = strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            finalseed[idx][1][pos][arg[0]] = 1
                    else:
                        if typ == 'dat':
                            finalseed[idx][1][pos][arg[0]] = strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            old = finalseed[idx][1][pos - 1][arg[0]]
                            finalseed[idx][1][pos][arg[0]] = old + 1 if type(old) is int else old + str(1)
                else:
                    typ = arg[1][:3]
                    if typ == 'var':
                        finalseed[idx][1][pos][arg[0]] = self.get_config(arg[0], 0)
                    elif (typ == 'boo') | (typ == 'BOO'):
                        finalseed[idx][1][pos][arg[0]] = random.choice([True, False])
                    elif typ == 'dat':
                        finalseed[idx][1][pos][arg[0]] = strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        finalseed[idx][1][pos][arg[0]] = self.get_config(arg[0])
        fin.append(finalseed[0])

    def init_database_connection(self):
        try:
            if self.u_config['dbms'] == 'mysql':
                self._db_connector = mysql.connector.connect(
                    host=self.u_config['host'],
                    port=self.u_config['port'],
                    database=self.u_config['db'],
                    user=self.u_config['user'],
                    passwd=self.u_config['password']
                )
                self._is_psql = False
            else:
                self._db_connector = psycopg2.connect(
                    host=self.u_config['host'],
                    port=self.u_config['port'],
                    database=self.u_config['db'],
                    user=self.u_config['user'],
                    password=self.u_config['password']
                )


        except Exception as e:
            sleep(1)
            print(e)
            print("Erreur Taur n'arrive pas a se connecter avec les parametres fourni.")
            print("\t{\n\t\tsgbd :\t\t'" + str(self.u_config['dbms']) + "'\n\t\t", end=' ')
            print("user :\t\t'" + str(self.u_config['user']) + "'\n\t\t", end=' ')
            print("password :\t'" + str(self.u_config['password']) + "'\n\t\t", end=' ')
            print("host :\t\t'" + str(self.u_config['host']) + "'\n\t\t", end=' ')
            print("database :\t'" + str(self.u_config['db']) + "'\n\t}\n\n", end=' ')
            exit()
        print("connection au sgbd '" + str(self.u_config['dbms']) + "' reussi")
        self._db_cursor = self._db_connector.cursor()
        print('*' * 8 ** 3, end='\n\n')
        if self._is_psql:
            self._db_cursor.execute("SELECT \
    table_schema || '.' || table_name \
FROM \
    information_schema.tables \
WHERE \
    table_type = 'BASE TABLE' \
AND \
    table_schema NOT IN ('pg_catalog', 'information_schema');")
        else:
            self._db_cursor.execute("SHOW TABLES")

        table_list = []
        for x in self._db_cursor:
            print(x[0], end=', ')
            table_list.append(x[0])
        self._db_cursor.close()
        print("\nList des tables trouver:\n\t", end=' ')
        print(table_list)
        if type(self.u_config['ignore']) is str:
            if table_list.__contains__(self.u_config['ignore']):
                table_list.remove(self.u_config['ignore'])
        elif type(self.u_config['ignore']) is list:
            for ignore in self.u_config['ignore']:
                if table_list.__contains__(ignore):
                    table_list.remove(ignore)
        sleep(2)
        self._db_cursor = self._db_connector.cursor()
        _finaltable = []
        ''' table final contient le nom de la table, la liste de colon
                       il sera representer comme suit:
                       [('table0',[('colonne1','type','isMul') # mul est mis ici pour les cles secondaires])]
                   '''
        print("\n\nListe final des tables a modifier", table_list)
        print("preparation de l'insertion")

        manager = multiprocessing.Manager()
        finalseed = manager.list()
        for table in table_list:
            assert isinstance(table, str)
            # self._db_cursor.execute("SHOW CREATE TABLE " + el)
            if self._is_psql:
                self._db_cursor.execute("SELECT * \
FROM information_schema.columns \
WHERE table_schema = " + table)
            else:
                self._db_cursor.execute("SHOW COLUMNS FROM " + table)
            _finaltable.append((table, []))
            flen = len(_finaltable)
            for nxt in self._db_cursor:
                _finaltable[flen - 1][1].append(list(nxt))

            self.appendExec(self.addseed, (table, _finaltable[flen - 1][1], finalseed))
        self.waitAllFinish()
        self._finalseed = finalseed
        print()
        print("seed a inserer")
        print(self._finalseed)
        print('verification de cle secondaire')
        remplacement = True
        idx = 0
        count = 0  # pour verifier l'indexation recurssive
        fln = len(self._finalseed)
        fln2 = fln ** 3
        precedent_primarys_key = []
        while (remplacement | (idx < fln)) & (count < fln2):
            remplacement = False
            precedent_primarys_key.append((self._finalseed[idx][0], self._finalseed[idx][3]))
            ln = len(self._finalseed[idx][2])
            if ln != 0:
                for foreign_id in range(ln):
                    if not self.string_contain_tuple_in_array(self._finalseed[idx][2][foreign_id],
                                                              precedent_primarys_key):
                        _el = self._finalseed[idx]
                        self._finalseed.remove(_el)
                        self._finalseed.append(_el)
                        remplacement = True
                        break

            idx = idx + 1
            if remplacement:
                print("# on reinitialise les compteur: table: " + str(self._finalseed[fln - 1][0]))
                print(list(map(lambda _x: _x[0], self._finalseed)))
                count = count + 1
                precedent_primarys_key = []
                idx = 0

        if count >= fln2:
            print("\n\n**********************\n\tErreur: indexsation recurssive")
            print("\tverifier les cles secondaire\n*******************\n")
            exit()
        print("\n\ncommencer l'insertion?\n")
        print("vous avez 30 secondes pour repondre y ou o pour oui et tout autres lettre pour nom")
        res = self.get_input(30)
        if (res == 'y') | (res == 'Y') | (res == 'O') | (res == 'o'):
            for table in self._finalseed:
                into = reduce(lambda _x, _y: str(_x) + ", " + str(_y), table[1][0].keys())
                valu = list(map(lambda _s: tuple(_s.values()), table[1]))
                print(into)
                print(valu)
                sql = "INSERT INTO " + str(table[0]) + " (" + into + ") VALUES (" + reduce(lambda _x, y: _x + "%s,",
                                                                                           into.split(', '), "")[
                                                                                    :-1] + ")"
                self._db_cursor.executemany(sql, valu)
                self._db_connector.commit()
        else:
            if res is None:
                print("delai depasser")
        print('\n' * 2, "bye!", end='\n')
        exit()

    @staticmethod
    def get_input(timeout=10):
        global user_input
        user_input = None

        th = threading.Thread(target=func)
        th.start()
        count = 0
        while count < timeout:
            if not th.is_alive():
                break
            count = count + 1
            time.sleep(1)
        th._delete()
        return user_input

    @staticmethod
    def string_contain_tuple_in_array(string, arr_tuple):
        string = string.lower()
        for tupl in arr_tuple:
            _table_name = tupl[0][:-1].lower()
            if tupl[0][-1] == 's'.lower():
                _table_name = tupl[0][:-1].lower()
            _primary_key = tupl[1].lower()
            lns = len(string)
            lnt = len(_table_name)
            lnp = len(_primary_key)
            if string.__contains__(_table_name) and string.__contains__(_primary_key) and (
                    lnp + lnt <= lns < lnp + lnt + 2) and (string.index(_table_name) == 0) and (
                    string.index(_primary_key) == lns - lnp):
                return True
        return False

    def loadConfig(self, file_path='./config.seed.json'):
        try:
            if file_path[-5:] != '.json':
                raise Exception('veuillez le fichier de configuration doit etre un fichier extenstion json')
            json_file = open(str(file_path))
            _config = json.load(json_file)
            if _config.__contains__('dbms'):
                self.u_config['dbms'] = _config['dbms']

            if _config.__contains__('user'):
                self.u_config['user'] = _config['user']
            else:
                raise Exception('utilisateur nom defini')

            if _config.__contains__('password'):
                self.u_config['password'] = _config['password']

            if _config.__contains__('process_number'):
                for proc in range(_config['process_number']):
                    self.queue.append(None)
            else:
                for proc in range(2):
                    self.queue.append(None)

            if _config.__contains__('host'):
                self.u_config['host'] = _config['host']

            if _config.__contains__('port'):
                self.u_config['port'] = _config['port']
            elif self.u_config['dbms'] == 'psql':
                self.u_config['port'] = 5432

            if _config.__contains__('db'):
                self.u_config['db'] = _config['db']
            else:
                raise Exception('la base de donnee n\'est pas specifier')

            if _config.__contains__('len_row'):
                self.u_config['len_row'] = _config['len_row']
            else:
                print('le nombre de colonne n\'a pas ete specifier, 50 sera utiliser par default')

            if _config.__contains__('ignore'):
                self.u_config['ignore'] = _config['ignore']

            if _config.__contains__('equal'):
                self.u_config['equal'] = _config['equal']

            if _config.__contains__('choice'):
                self.u_config['choice'] = _config['choice']

            if _config.__contains__('combine'):
                for key in _config['combine']:
                    if _config['combine'][key].__contains__('val') and (type(_config['combine'][key]['val']) is list):
                        self.u_config['combine'][key] = _config['combine'][key]
                        if not _config['combine'][key].__contains__('join'):
                            self.u_config['combine'][key]['join'] = " "

            if _config.__contains__('border'):
                self.u_config['border'] = _config['border']
                if not _config['border'].__contains__('_def'):
                    self.u_config['unit']['_def'] = 1

            if _config.__contains__('unit'):
                self.u_config['unit'] = _config['unit']
                if not _config['unit'].__contains__('_def'):
                    self.u_config['unit']['_def'] = 1

            return 0
        except Exception as e:
            print(e)
            return -1


if __name__ == "__main__":
    TaurSeedGenerator(sys.argv)
