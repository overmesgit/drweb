# coding=utf-8
import copy


class DataBase:
    """
    Основной интерфейс к БД
    """
    def __init__(self, storage=None):
        self.parser = QueryParser()
        self.storage = storage or DataBaseStorage()
        self.executor = QueryExecutor(self.storage)

    def execute(self, str_command):
        query = self.parser.execute(str_command)
        return self.executor.execute(query)


class Query:
    """
    Запрос, который может быть выполнен для изменения или получения данных
    """
    def __init__(self):
        self.op = None
        self.pk = None
        self.value = None


class DataBaseStorage:
    """
    Основное хранилище данных
    """
    def __init__(self):
        self._block = None
        self._storage = {}

    def get_storage(self, client_id):
        if not self._block or self._block == client_id:
            return self._storage

    def block(self, client_id):
        self._block = client_id

    def free(self, client_id):
        if self._block == client_id:
            self._block = None


class QueryParser():
    """
    Парсер для проверки правильности запросов и получения Query
    """
    CREATE_QUERY = 'create_query'
    PARSE_OP = 'parse_op'
    PARSE_PK = 'parse_pk'
    PARSE_VALUE = 'parse_value'
    PARSE_DONE = 'parse_done'

    GET = 'GET'
    SET = 'SET'
    COUNTS = 'COUNTS'
    END = 'END'
    UNSET = 'UNSET'
    BEGIN = 'BEGIN'
    ROLLBACK = 'ROLLBACK'
    COMMIT = 'COMMIT'

    OPERATIONS = {GET, SET, COUNTS, END, UNSET, BEGIN, ROLLBACK, COMMIT}

    def __init__(self):
        self.states = {
            self.CREATE_QUERY: self._create_query,
            self.PARSE_OP: self._parse_op,
            self.PARSE_PK: self._parse_pk,
            self.PARSE_VALUE: self._parse_value,
            self.PARSE_DONE: self._parse_done
        }
        self.current_state = None

    def execute(self, str_command):
        command_list = str_command.split()
        next_state = self._set_state(self.CREATE_QUERY)

        for argument in command_list:
            if next_state:
                next_state = self._set_state(next_state, argument)
            else:
                raise ValueError("Wrong query")

        if next_state == self.PARSE_DONE:
            self._set_state(next_state)
            return self._get_query()
        else:
            raise ValueError("Wrong query")

    def _set_state(self, state_name, *args, **kwargs):
        self.current_state = state_name
        return self.states[state_name](*args, **kwargs)

    def _get_query(self):
        return self.current_query

    def _create_query(self):
        self.current_query = Query()
        return self.PARSE_OP

    def _parse_op(self, op_name):
        if op_name in self.OPERATIONS:
            self.current_query.op = op_name
            if op_name == self.COUNTS:
                return self.PARSE_VALUE
            elif op_name in [self.END, self.BEGIN, self.ROLLBACK, self.COMMIT]:
                return self.PARSE_DONE
            else:
                return self.PARSE_PK
        else:
            raise ValueError('Not available command')

    def _parse_pk(self, pk):
        self.current_query.pk = pk

        current_op = self.current_query.op
        if current_op == self.GET or current_op == self.UNSET:
            return self.PARSE_DONE
        elif current_op == self.SET:
            return self.PARSE_VALUE

    def _parse_value(self, value):
        self.current_query.value = value
        return self.PARSE_DONE

    def _parse_done(self, argument=None):
        pass


class QueryExecutor:
    """
    Класс для манипуляций с данными
    """
    def __init__(self, db_storage):
        self.client_id = id(self)
        self.db_storage = db_storage
        self.shadow_pages = []

    # Методы, организующие транзакции ========================================
    def _add_shadow_page(self):
        self.shadow_pages.append({})

    def _get_in_shadow_page(self, pk):
        for page in reversed(self.shadow_pages):
            if pk in page:
                return page[pk] if page[pk] else 'NULL'

    def _set_in_shadow_page(self, pk, value):
        self.shadow_pages[-1][pk] = value

    def _del_in_shadow_page(self, pk):
        self.shadow_pages[-1][pk] = None

    def _commit_shadow_page(self):
        self.db_storage.block(self.client_id)

        storage = self.db_storage.get_storage(self.client_id)
        for page in reversed(self.shadow_pages):
            for key, val in page.items():
                if val is not None:
                    storage[key] = val
                else:
                    del storage[key]

        self.db_storage.free(self.client_id)

    def _remove_shadow_page(self):
        self.shadow_pages.pop()

    def get_current_db_copy(self):
        storage = self.db_storage.get_storage(self.client_id)
        current_state = copy.deepcopy(storage)
        for page in reversed(self.shadow_pages):
            current_state.update(page)
        return current_state

    # Методы, организующие выполнение запросов ===============================
    def execute(self, query):
        op_executor = self._get_command_executor(query)
        return op_executor(query)

    def _get_command_executor(self, query):
        return getattr(self, '_{0}_executor'.format(query.op.lower()))

    def _get_executor(self, query):
        result = None
        storage = self.db_storage.get_storage(self.client_id)
        if self.shadow_pages:
            result = self._get_in_shadow_page(query.pk)
        return storage.get(query.pk, 'NULL') if result is None else result

    def _set_executor(self, query):
        storage = self.db_storage.get_storage(self.client_id)
        if self.shadow_pages:
            self._set_in_shadow_page(query.pk, query.value)
        else:
            storage[query.pk] = query.value

    def _unset_executor(self, query):
        storage = self.db_storage.get_storage(self.client_id)
        try:
            if self.shadow_pages:
                self._del_in_shadow_page(query.pk)
            else:
                del storage[query.pk]
        except KeyError:
            pass

    def _counts_executor(self, query):
        storage = self.db_storage.get_storage(self.client_id)
        if self.shadow_pages:
            current_state = self.get_current_db_copy()
            return len(filter(lambda v: v == query.value, current_state.values()))
        else:
            return len(filter(lambda v: v == query.value, storage.values()))

    def _begin_executor(self, query):
        self._add_shadow_page()

    def _rollback_executor(self, query):
        if self.shadow_pages:
            self._remove_shadow_page()

    def _commit_executor(self, query):
        if self.shadow_pages:
            self._commit_shadow_page()

    def _end_executor(self, query):
        pass
