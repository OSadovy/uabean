import collections
import itertools
import re

from beangulp import cache
from beangulp.importers.mixins.identifier import _PARTS, identify


class IdentifyMixin:
    """Converted for use with new beangulp Importer class."""

    def __init__(self, **kwds):
        """Pull 'matchers' and 'converter' from kwds."""

        self.remap = collections.defaultdict(list)
        matchers = kwds.pop("matchers", [])
        cls_matchers = getattr(self, "matchers", [])
        assert isinstance(matchers, list)
        assert isinstance(cls_matchers, list)
        for part, regexp in itertools.chain(matchers, cls_matchers):
            assert part in _PARTS, repr(part)
            assert isinstance(regexp, str), repr(regexp)
            self.remap[part].append(re.compile(regexp))

        # Converter is a fn(filename: Text) -> contents: Text.
        self.converter = kwds.pop("converter", getattr(self, "converter", None))

        super().__init__(**kwds)

    def identify(self, filepath):
        return identify(self.remap, self.converter, cache.get_file(filepath))
