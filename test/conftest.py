from sqlalchemy.dialects import registry

registry.register('aster', 'sqlalchemy_aster.jdbc', 'AsterDialect_jdbc')
registry.register('aster.jdbc', 'sqlalchemy_aster.jdbc', 'AsterDialect_jdbc')


from sqlalchemy.testing.plugin.pytestplugin import *
