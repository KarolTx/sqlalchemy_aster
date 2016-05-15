# aster/base.py
# Copyright (C) 2007-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
# Copyright (C) 2007 Paul Johnston, paj@pajhome.org.uk
# Portions derived from jet2sql.py by Matt Keranen, mksql@yahoo.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

'''
support for the Teradata Aster database

'''

from sqlalchemy import sql, schema, types, exc, pool
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base, reflection
from sqlalchemy import processors
from sqlalchemy import types as sqltypes
from sqlalchemy.types import (
    INTEGER, BIGINT, SMALLINT, FLOAT, NUMERIC, REAL,
    CHAR, VARCHAR, TEXT, DATE, BOOLEAN
)


RESERVED_WORDS = set(
    ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
     "asymmetric", "both", "case", "cast", "check", "collate", "column",
     "constraint", "create", "current_catalog", "current_date",
     "current_role", "current_time", "current_timestamp", "current_user",
     "default", "deferrable", "desc", "distinct", "do", "else", "end",
     "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
     "having", "in", "initially", "intersect", "into", "leading", "limit",
     "localtime", "localtimestamp", "new", "not", "null", "of", "off",
     "offset", "old", "on", "only", "or", "order", "placing", "primary",
     "references", "returning", "select", "session_user", "some", "symmetric",
     "table", "then", "to", "trailing", "true", "union", "unique", "user",
     "using", "variadic", "when", "where", "window", "with", "authorization",
     "between", "binary", "cross", "current_schema", "freeze", "full",
     "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
     "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
     ])



class BYTEA(sqltypes.LargeBinary):
    __visit_name__ = 'BYTEA'


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = 'DOUBLE_PRECISION'


class TIMESTAMP(sqltypes.TIMESTAMP):

    def __init__(self, timezone=False, precision=None):
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


class TIME(sqltypes.TIME):

    def __init__(self, timezone=False, precision=None):
        super(TIME, self).__init__(timezone=timezone)
        self.precision = precision


ischema_names = {
    'integer': INTEGER,
    'bigint': BIGINT,
    'smallint': SMALLINT,
    'character varying': VARCHAR,
    'character': CHAR,
    '"char"': sqltypes.String,
    'name': sqltypes.String,
    'text': TEXT,
    'numeric': NUMERIC,
    'float': FLOAT,
    'real': REAL,
    'double precision': DOUBLE_PRECISION,
    'timestamp': TIMESTAMP,
    'timestamp with time zone': TIMESTAMP,
    'timestamp without time zone': TIMESTAMP,
    'time with time zone': TIME,
    'time without time zone': TIME,
    'date': DATE,
    'time': TIME,
    'bytea': BYTEA,
    'boolean': BOOLEAN,
}


class AsterCompiler(compiler.SQLCompiler):

    def visit_any(self, element, **kw):
        return "%s%sANY (%s)" % (
            self.process(element.left, **kw),
            compiler.OPERATORS[element.operator],
            self.process(element.right, **kw)
        )

    def visit_all(self, element, **kw):
        return "%s%sALL (%s)" % (
            self.process(element.left, **kw),
            compiler.OPERATORS[element.operator],
            self.process(element.right, **kw)
        )

    def visit_ilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)

        return '%s ILIKE %s' % \
            (self.process(binary.left, **kw),
             self.process(binary.right, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def visit_notilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        return '%s NOT ILIKE %s' % \
            (self.process(binary.left, **kw),
             self.process(binary.right, **kw)) \
            + (
                ' ESCAPE ' +
                self.render_literal_value(escape, sqltypes.STRINGTYPE)
                if escape else ''
            )

    def render_literal_value(self, value, type_):
        value = super(AsterCompiler, self).render_literal_value(value, type_)

        if self.dialect._backslash_escapes:
            value = value.replace('\\', '\\\\')
        return value

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit_clause is not None:
            text += " \n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            if select._limit_clause is None:
                text += " \n LIMIT ALL"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def get_select_precolumns(self, select, **kw):
        if select._distinct is not False:
            if select._distinct is True:
                return "DISTINCT "
            elif isinstance(select._distinct, (list, tuple)):
                return "DISTINCT ON (" + ', '.join(
                    [self.process(col) for col in select._distinct]
                ) + ") "
            else:
                return "DISTINCT ON (" + \
                    self.process(select._distinct, **kw) + ") "
        else:
            return ""

    def visit_substring_func(self, func, **kw):
        s = self.process(func.clauses.clauses[0], **kw)
        start = self.process(func.clauses.clauses[1], **kw)
        if len(func.clauses.clauses) > 2:
            length = self.process(func.clauses.clauses[2], **kw)
            return "SUBSTRING(%s FROM %s FOR %s)" % (s, start, length)
        else:
            return "SUBSTRING(%s FROM %s)" % (s, start)





class AsterDDLCompiler(compiler.DDLCompiler):

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        impl_type = column.type.dialect_impl(self.dialect)
        if column.primary_key and \
            column is column.table._autoincrement_column and \
            (
                self.dialect.supports_smallserial or
                not isinstance(impl_type, sqltypes.SmallInteger)
            ) and (
                column.default is None or
                (
                    isinstance(column.default, schema.Sequence) and
                    column.default.optional
                )):
            if isinstance(impl_type, sqltypes.BigInteger):
                colspec += " BIGSERIAL"
            else:
                colspec += " SERIAL"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type,
                                                    type_expression=column)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

            if not column.nullable:
                colspec += " NOT NULL"
        return colspec

    def post_create_table(self, table):
        table_opts = []
        opts = table.dialect_options['aster']
        print(dir(opts.items()))
        print(table.primary_key)
        print(dir(table.primary_key))
        print(table.kwargs.items())

        distribute_by = opts.get('distribute_by')
        if distribute_by is not None:
            if not isinstance(distribute_by, (list, tuple)):
                inherits = (distribute_by, )
            table_opts = \
                '\n DISTRIBUTE BY HASH( ' + \
                ', '.join(self.preparer.quote(name) for name in distribute_by) + \
                ' )'
        else:
            table_opts = '\nDISTRIBUTE BY\n\tREPLICATION'

        return table_opts

    def visit_create_index(self, create):
        preparer = self.preparer
        index = create.element
        self._verify_index_table(index)
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX "

        text += "%s ON %s " % (
            self._prepared_index_name(index,
                                      include_schema=False),
            preparer.format_table(index.table)
        )

        using = index.dialect_options['aster']['using']
        if using:
            text += "USING %s " % preparer.quote(using)

        ops = index.dialect_options["aster"]["ops"]
        text += "(%s)" \
                % (
                    ', '.join([
                        self.sql_compiler.process(
                            expr.self_group()
                            if not isinstance(expr, expression.ColumnClause)
                            else expr,
                            include_table=False, literal_binds=True) +
                        (
                            (' ' + ops[expr.key])
                            if hasattr(expr, 'key')
                            and expr.key in ops else ''
                        )
                        for expr in index.expressions
                    ])
                )

        withclause = index.dialect_options['aster']['with']

        if withclause:
            text += " WITH (%s)" % (', '.join(
                ['%s = %s' % storage_parameter
                 for storage_parameter in withclause.items()]))

        whereclause = index.dialect_options["aster"]["where"]

        if whereclause is not None:
            where_compiled = self.sql_compiler.process(
                whereclause, include_table=False,
                literal_binds=True)
            text += " WHERE " + where_compiled
        return text


class AsterIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def _unquote_identifier(self, value):
        if value[0] == self.initial_quote:
            value = value[1:-1].\
                replace(self.escape_to_quote, self.escape_quote)
        return value



class AsterExecutionContext(default.DefaultExecutionContext):
    pass



class AsterDialect(default.DefaultDialect):
    name = 'aster'
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_sequences = False
    supports_default_values = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_smallserial = False
    default_paramstyle = 'qmark'
    max_identifier_length = 63

    ischema_names = ischema_names

    poolclass = pool.SingletonThreadPool
    statement_compiler = AsterCompiler
    ddl_compiler = AsterDDLCompiler
    preparer = AsterIdentifierPreparer
    execution_ctx_cls = AsterExecutionContext

    construct_arguments = [
        (schema.Index, {
            "using": False,
            "where": None,
            "with": {}
        }),
        (schema.Table, {
            "on_commit": None,
            "distribute_by": None
        })
    ]

    _backslash_escapes = True


    def has_table(self, connection, tablename, schema=None):
        result = connection.scalar(
                        sql.text(
                            "select count(*) from msysobjects where "
                            "type=1 and name=:name"), name=tablename
                        )
        return bool(result)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute("select name from msysobjects where "
                "type=1 and name not like 'MSys%'")
        table_names = [r[0] for r in result]
        return table_names


