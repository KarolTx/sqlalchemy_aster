__version__ = '0.01'

from sqlalchemy.dialects import registry

registry.register('aster', 'sqlalchemy_aster.jdbc', 'AsterDialect_jdbc')
registry.register('aster.jdbc', 'sqlalchemy_aster.jdbc', 'AsterDialect_jdbc')
