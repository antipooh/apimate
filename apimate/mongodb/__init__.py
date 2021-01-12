from pydantic import AnyUrl


class MongoDsn(AnyUrl):
    allowed_schemes = {'mongodb'}

    @property
    def database(self) -> str:
        return self.path.replace('/', '')


