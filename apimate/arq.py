from arq.connections import RedisSettings
from pydantic import AnyUrl


class RedisDsn(AnyUrl):
    allowed_schemes = {'redis'}
    user_required = False

    @property
    def settings(self) -> RedisSettings:
        try:
            database = int(self.path.replace('/', ''))
        except ValueError:
            database = 0
        return RedisSettings(host=self.host,
                             port=int(self.port) if self.port else None,
                             database=database)
