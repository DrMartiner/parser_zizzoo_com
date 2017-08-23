from configparser import ConfigParser


class ConfigReader(ConfigParser):
    cfg = {}
    _types_params = {}

    def __init__(self, *args, **kwargs):
        try:
            self._file = kwargs.pop('file_config')
        except KeyError:
            self._file = 'settings.ini'
        super(ConfigReader, self).__init__(*args, **kwargs)

    def config_read(self, types_params={}):
        """
        Read from config file to dict in format: {"section: {parameter: value}}
        """
        self._types_params = types_params

        self.read(self._file)
        # Read all sections from config
        for section in self.sections():
            for key in self.options(section):
                self.cfg[key] = self.get(section, key)
        return self.check_and_set_config()

    def check_and_set_config(self):
        """
        Checking a dict config that corresponds to their types.
        After, creating the new dict config with valid types variables python.

        Arguments:
            config dict in format is {parameter: value}
            dict of typesParams in format is {parameter: 'type variable'}
            variable can pass follows type: 'bool', 'int', and 'str'
        """
        myconfig = {}
        for key, value in self.cfg.items():
            value_params = self._types_params.get(key, None)
            if value_params is not None:
                if value_params == 'bool':
                    myconfig[key] = bool(int(self.cfg[key]))
                elif value_params == 'int':
                    myconfig[key] = int(self.cfg[key])
                else:
                    myconfig[key] = self.cfg[key]
            else:
                myconfig[key] = self.cfg[key]
        return myconfig
