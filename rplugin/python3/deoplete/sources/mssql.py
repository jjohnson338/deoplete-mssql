import re
import subprocess
from subprocess import CalledProcessError
from .base import Base
from deoplete.util import error_vim, getlines, parse_buffer_pattern


class Source(Base):

    def __init__(self, vim):
        super().__init__(vim)

        self.rank = 350
        self.name = 'mssql'
        self.mark = '[mssql]'
        self.min_pattern_length = 0
        self.filetypes = ['sql']


    def on_init(self, context):
        self.server = context['vars'].get('deoplete#sources#mssql#server')
        self.user = context['vars'].get('deoplete#sources#mssql#user')
        self.password = context['vars'].get('deoplete#sources#mssql#password')
        self.db = context['vars'].get('deoplete#sources#mssql#db')
        self.query = """
set nocount on;
with tables_and_views as (
  select name as [object_name]
  from sys.tables
  where type_desc = 'user_table'
  union all
  select name as [object_name]
  from sys.views
)
select
  upper(tav.object_name) as [table_name]
  ,upper(c.name) as [column_name]
from syscolumns c
inner join sysobjects o
  on c.id=o.id
inner join tables_and_views tav
  on o.name = tav.[object_name]"""
        self.command = [
            'sqlcmd'
            ,'-S', self.server
            ,'-U', self.user
            ,'-P', self.password
            ,'-d', self.db
            ,'-h-1'
            ,'-W'
            ,'-s', ','
            ,'-Q', self.query
        ]
        self._cache = {}

    def gather_candidates(self, context):
        self.min_pattern_length = 1
        self._make_cache(context)

        current = context['complete_str']
        line = context['position'][1]
        line_text = getlines(self.vim,line,line)[0]

        candidates = []

        if re.search(f'[.]{current}$', line_text):
            self.min_pattern_length = 0
            # we are doing some column lookup
            # find the table or alias
            matchObj = re.match(f'\s*(\w+)[.]\w*$', line_text)
            table_or_alias = matchObj.group(1)
            for table in self._cache:
                if table == table_or_alias.upper() or table_or_alias.upper() in self._cache[table]['aliases']:
                    # append columns
                    for column in self._cache[table]['columns']:
                        candidates.append({ 'word': column })
                        candidates.append({ 'word': column.lower() })
            return candidates


        # otherwise, fill candidates with all tables, cols, and aliases
        for table in self._cache:
            candidates.append({ 'word': table })
            candidates.append({ 'word': table.lower() })
            for column in self._cache[table]['columns']:
                candidates.append({ 'word': column })
                candidates.append({ 'word': column.lower() })
            for alias in self._cache[table]['aliases']:
                candidates.append({ 'word': alias })
                candidates.append({ 'word': alias.lower() })

        return candidates

    def _make_cache(self, context):
        # populate tables and columns
        if not self._cache:
            try:
                command_results = subprocess.check_output(self.command, universal_newlines=True).split('\n')
            except CalledProcessError as e:
                error_vim(self.vim, e)

            for row in command_results:
                if ',' not in row:
                    continue

                match = re.match(r'(.*),(.*)', row.strip())
                table = match.group(1).strip()
                column = match.group(2).strip()
                if table not in self._cache:
                    self._cache[table] = {
                            'columns': [],
                            'aliases': []
                    }
                elif not column in self._cache[table]:
                    self._cache[table]['columns'].append(column)

        # gather aliases
        alias_hits = parse_buffer_pattern(
                    getlines(self.vim),
                    r'(FROM|from|JOIN|join)\s+(\w+)\s+(\w+)',
                )

        # clear existing aliases
        for table in self._cache:
            self._cache[table]['aliases'] = []

        for alias_hit in alias_hits:
            table = alias_hit[1].upper()
            alias = alias_hit[2].upper()
            if table not in self._cache:
                continue

            if alias not in self._cache[table]['aliases']:
                self._cache[table]['aliases'].append(alias)
