'''
Template Component main class.

'''

from kbc.env_handler import KBCEnvHandler
import logging
import job_runner

MANDATORY_PARS = [
    'username',
    '#password',
    # 'endpoints',
    'clear_state'
]

APP_VERSION = '0.0.38'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if(self.cfg_params.get('debug')):
            debug = True

        self.set_default_logger('DEBUG' if debug else 'INFO')
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validateConfig()
        except ValueError as e:
            logging.error(e)
            exit(1)

    def run(self, debug=True):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        username = params.get('username')
        password = params.get('#password')
        # endpoints = params.get('endpoints')
        clear_state = params.get('clear_state')
        tables = self.configuration.get_input_tables()

        job_runner.run(username, password, clear_state, tables)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    comp = Component()
    comp.run()
