import operator
import re
import subprocess
from .base import Base
from deoplete.util import getlines, parse_buffer_pattern
from subprocess import CalledProcessError


class Source(Base):
    def __init__(self, vim):
        super().__init__(vim)
        self.dup = True
        self.filetypes = ['sql']
        self.input_pattern = '((\w+)[.]\w*)|([@]\w+)|(\w+)'
        self.is_volatile = True
        self.mark = '[mssql]'
        self.min_pattern_length = 1
        self.name = 'mssql'
        self.rank = 350
        self.sorters = ['sorter_rank', 'sorter_word']

    def on_init(self, context):
        self.server = context['vars'].get('deoplete#sources#mssql#server')
        self.user = context['vars'].get('deoplete#sources#mssql#user')
        self.password = context['vars'].get('deoplete#sources#mssql#password')
        self.db = context['vars'].get('deoplete#sources#mssql#db')
        self.command = [
            'sqlcmd',
            '-S', self.server,
            '-U', self.user,
            '-P', self.password,
            '-d', self.db,
            '-h-1',
            '-W',
            '-s', ',',
            '-Q', QUERY_STRING,
        ]
        self._cache = {
            'tables': {},
            'variables': {}
        }

    def gather_candidates(self, context):
        self._make_cache(context)

        current = context['complete_str']  # current search text
        candidates = []

        if current.startswith("@"):
            # variables
            for variable in self._cache['variables']:
                candidates.append({
                    'word': f"@{variable}",
                    'menu': f'[{self._cache["variables"][variable]["type"]}]',
                    'kind': 'variable',
                })
            return candidates

        column_match = COLUMN_PATTERN.search(current)
        if column_match:
            # we are doing a column lookup
            # find the table or alias
            table_or_alias = column_match.group(1)
            for table in self._cache['tables']:
                is_table = table == table_or_alias.upper()
                is_alias = table_or_alias.upper() \
                    in self._cache['tables'][table]['aliases']
                if is_table or is_alias:
                    # append columns
                    for column in self._cache['tables'][table]['columns']:
                        type_string = (
                            f'{column["type"]}({column["length"]})'
                            f'{"" if column["nullabe"] else "NOT "}NULL'
                            if column["length"] is not None
                            else (
                                f'{column["type"]}'
                                f'{"" if column["nullabe"] else "NOT "}NULL')
                            )
                        candidates.append({
                            'word': f"{table_or_alias}."
                                    f"{column['column_name']}",
                            'menu': f'[{type_string}]',
                            'kind': 'col'
                        })
            candidates.sort(key=operator.itemgetter('word'))
            return candidates

        # otherwise, fill candidates with all tables and aliases
        for table in self._cache['tables']:
            candidates.append({
                'word': table,
                'kind': f"[{self._cache['tables'][table]['type']}]",
            })

            for alias in self._cache['tables'][table]['aliases']:
                candidates.append({
                    'word': alias,
                    'kind': 'alias',
                })

        candidates.sort(key=operator.itemgetter('word'))
        return candidates

    def get_last_match(self, matches):
        return_val = None
        for item in matches:
            return_val = item
        return return_val

    def get_complete_position(self, context):
        variable = self.get_last_match(
                VARIBLE_PATTERN.finditer(context['input']))
        column = self.get_last_match(
                COLUMN_PATTERN.finditer(context['input']))
        other = re.search(self.input_pattern+"$", context['input'])
        return max(
            variable.start() if variable is not None else -1,
            column.start() if column is not None else -1,
            other.start() if other is not None else -1
        )

    def _make_cache(self, context):
        # gather variables
        self._cache['variables'] = {}
        variable_hits = parse_buffer_pattern(
            getlines(self.vim, 1),
            r'(@)(\w+)(\s+)(\w+)'
        )
        for variable_hit in variable_hits:
            variable = variable_hit[1]
            type = variable_hit[3]
            if variable not in self._cache['variables']:
                self._cache['variables'][variable] = {
                    'type': type.upper()
                }

        # populate tables and columns
        if not self._cache['tables']:
            try:
                command_results = (
                        subprocess
                        .check_output(self.command, universal_newlines=True)
                        .split('\n')
                )
            except CalledProcessError:
                return None

            for row in command_results:
                if ',' not in row:
                    continue

                match = re.match(r'(.*),(.*),(.*),(.*),(.*),(.*)', row.strip())
                table_or_view = match.group(1).strip()
                type_name = match.group(2).strip()
                column = match.group(3).strip()
                column_type = match.group(4).strip()
                column_nullable = (
                        True if match.group(5).strip() == '1' else False)
                column_length = match.group(6).strip()
                column_def = {
                    'column_name': column,
                    'type': column_type,
                    'nullabe': column_nullable,
                    'length': (column_length
                               if column_length != 'NULL'
                               else None),
                }
                if table_or_view not in self._cache['tables']:
                    self._cache['tables'][table_or_view] = {
                        'type': type_name,
                        'columns': [column_def],
                        'aliases': [],
                    }
                elif column not in self._cache['tables'][table_or_view]:
                    self._cache['tables'][table_or_view]['columns'].append(column_def)

        # gather aliases
        alias_hits = parse_buffer_pattern(
            getlines(self.vim),
            r'(FROM|from|JOIN|join)\s+(\w+)\s+(\w+)',
        )

        # clear existing aliases
        for table in self._cache['tables']:
            self._cache['tables'][table]['aliases'] = []

        for alias_hit in alias_hits:
            table = alias_hit[1].upper()
            alias = alias_hit[2].upper()
            if table not in self._cache['tables']:
                continue

            if alias not in self._cache['tables'][table]['aliases']:
                self._cache['tables'][table]['aliases'].append(alias)


COLUMN_PATTERN = re.compile(r'(\w+)[.]\w*')
VARIBLE_PATTERN = re.compile(r'[@]\s?')
QUERY_STRING = """
set nocount on;
with tables_and_views as (
    select
        name as [object_name]
        ,'table' as [type]
    from sys.tables
    where type_desc = 'user_table'
    union all
    select
        name as [object_name]
        ,'view' as [type]
    from sys.views
)
select
    upper(tav.object_name) as [table_name]
    ,tav.type as [type]
    ,upper(c.name) as [column_name]
    ,upper(t.name) as [column_type]
    ,c.isnullable as [column_nullable]
    ,case
        when t.name in ('varchar','char','nvarchar','nchar') then c.length
        else null
    end as [column_length]
from syscolumns c
inner join sysobjects o
    on c.id=o.id
inner join systypes t
    on c.xtype = t.xtype
    and t.name <> 'sysname'
inner join tables_and_views tav
    on o.name = tav.[object_name]"""
