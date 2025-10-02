from trading.data.metadata.trading_system_attributes import classproperty


class Price:

    __OPEN = 'open'
    __HIGH = 'high'
    __LOW = 'low'
    __CLOSE = 'close'
    __VOLUME = 'volume'
    __DT = 'date'
    __TIMESTAMP = 'timestamp'

    @classproperty
    def OPEN(cls):
        return cls.__OPEN

    @classproperty
    def HIGH(cls):
        return cls.__HIGH

    @classproperty
    def LOW(cls):
        return cls.__LOW

    @classproperty
    def CLOSE(cls):
        return cls.__CLOSE

    @classproperty
    def VOLUME(cls):
        return cls.__VOLUME

    @classproperty
    def DT(cls):
        return cls.__DT

    @classproperty
    def TIMESTAMP(cls):
        return cls.__TIMESTAMP