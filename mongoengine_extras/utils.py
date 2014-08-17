import re
import unicodedata
from mongoengine.python_support import PY3

STRIP_REGEXP = re.compile(r'[^\w\s-]')
HYPHENATE_REGEXP = re.compile(r'[-\s]+')


def slugify(value):
    if not PY3:
        value = unicode(value)
    value = unicodedata.normalize('NFKD', value)
    value = value.encode('ascii', 'ignore').decode('ascii')
    value = STRIP_REGEXP.sub('', value).strip().lower()
    return HYPHENATE_REGEXP.sub('-', value)
