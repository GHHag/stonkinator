class PortfolioData:#(IPortfolioData):
    
    def __init__(self, max_positions):
        self.__max_positions = max_positions

    @property
    def max_positions(self):
        return self.__max_positions
