from airflow.hooks.http_hook import HttpHook


class BambooHRHook(HttpHook):
    """
    Hook for BambooHR.

    Inherits from the base HttpHook to make a request to BambooHR.
    Uses basic authentication via a never expiring token that should
    be stored in the 'Login' field in the Airflow Connection panel.

    Only allows GET requests.

    """

    def __init__(self, bamboo_conn_id, method='GET'):
        super().__init__(method, http_conn_id=bamboo_conn_id)

    def get_conn(self, headers=None):
        session = super().get_conn(headers=headers)
        return session

    def run(self, company_name, endpoint, payload=None):
        self.endpoint = '{0}/v1/{1}'.format(company_name, endpoint)

        # Hard code hook to return JSON
        headers = {"Accept": "application/json"}

        return super().run(self.endpoint, data=payload, headers=headers)
