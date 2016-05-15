# aster/jdbc.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

'''
support for Teradata Aster via jdbc

'''


from .base import AsterExecutionContext, AsterDialect


class AsterExecutionContext_jdbc(AsterExecutionContext):
    pass


class AsterDialect_jdbc(AsterDialect):

    execution_ctx_cls = AsterExecutionContext_jdbc
    jdbc_driver_name = 'Teradata Aster'


    @classmethod
    def dbapi(cls):
        return __import__('jaydebeapi')


    def create_connect_args(self, url):
        r = ([],
            {
                'jclassname': 'com.asterdata.ncluster.Driver',
                'jars': '{jar}'.format(**url.query),
                'driver_args': [
                    'jdbc:ncluster://{host}:{port}/{database}/?autocommit=false' \
                        .format(**url.translate_connect_args()),
                    url.username,
                    url.password
                ]
            }
        )
        return r


    def is_disconnect(self, e, connection, cursor):
        if not isinstance(e, self.dbapi.ProgrammingError):
            return False
        e = str(e)
        return 'connection is closed' in e or 'cursor is closed' in e


    def _get_server_version_info(self, connection):
        raise NotImplementedError()

