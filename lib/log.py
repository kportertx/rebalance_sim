from lib import config


class log(object):
    indent_level = 0
    indentation = ""

    def __init__(self, fmt, *args, **kwargs):
        if config.OUTPUT:
            msg = fmt.format(*args, **kwargs)

            print("{}{}".format(log.indentation, msg))

    def __enter__(self):
        log.indent_level += 1
        self._update_indentation()

    def __exit__(self, *args):
        log.indent_level -= 1
        self._update_indentation()

    def _update_indentation(self):
        indentation = "│  " * (log.indent_level - 1)

        if log.indent_level > 0:
            indentation += "├─ "

        log.indentation = indentation
