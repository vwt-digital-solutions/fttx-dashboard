class Data(dict):
    """
    The data class is an extension of the python dictionary. It also allows to set and get data using attribures.

    >>> my_data = Data()
    >>> my_data.test = 'some data'
    >>> my_data.test
    'some data'

    This is equivalent to:

    >>> my_data = Data()
    >>> my_data['test'] = 'some data'
    >>> my_data['test']
    'some data'
    """

    def __getattr__(self, attr):
        if not attr.startswith("__"):
            return self[attr]

    def __setattr__(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__dict__.update({key: value})

    def __delattr__(self, item):
        self.__delitem__(item)

    def __delitem__(self, key):
        super().__delitem__(key)
        del self.__dict__[key]

    def __getstate__(self):
        return self.copy()

    def __setstate__(self, state):
        self = state  # noqa:F841
