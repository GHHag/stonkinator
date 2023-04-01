import json
from pprint import pprint

from tet_trading_systems.trading_system_state_handler.portfolio.portfolio import Portfolio


class ManualPortfolioHandler(Portfolio):

    def __init__(self, *args):
        super().__init__(*args)
        self.__target_portfolio_data = json.loads(self.portfolio_db.get_target_portfolio_data(self.system_name))
        if self.__target_portfolio_data:
            for target_position_data in self.__target_portfolio_data['target_portfolio_data']:
                print('Position data:')
                pprint(target_position_data)
                if(self._position_entry_input(target_position_data)):
                    self._position_entry_input_validation(target_position_data)
                print()

    def _position_entry_input(self, target_position_data):
        position_taken = input('Confirm if the position is active. Y/N: ')
        if position_taken.upper() == 'Y':
            target_position_data['quantity'] = float(input('Enter quantity: '))
            target_position_data['entry_price'] = float(input('Enter entry price: '))
            return True
        else:
            return False

    def _position_entry_input_validation(self, target_position_data):
        print('Position entry data:')
        pprint(target_position_data)
        correct_input_check = input('Confirm if the data is correct. Y/N: ')
        if correct_input_check.upper() == 'Y':
            self.portfolio_db.insert_position(self.system_name, target_position_data)
 