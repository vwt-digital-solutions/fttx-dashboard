import logging
import flask_login
from jwkaas import JWKaas
from .auth import Auth

from flask import redirect, url_for, Response, abort
from flask_dance.contrib.azure import azure, make_azure_blueprint


class AzureOAuth(Auth):
    def __init__(self, app, client_id, client_secret, expected_issuer, expected_audience, jwks_url, tenant,
                 session_secret, role, scopes=None):
        super(AzureOAuth, self).__init__(app)
        azure_bp = make_azure_blueprint(
            client_id=client_id,
            client_secret=client_secret,
            scope=scopes,
            tenant=tenant,
        )
        app.server.register_blueprint(azure_bp, url_prefix="/login")
        app.server.secret_key = session_secret
        self._jwkaas = JWKaas(expected_audience, expected_issuer, jwks_url=jwks_url)
        self.client_id = client_id
        self.logout_url = None
        self.user = None
        self.role = role

    def is_authorized(self):
        if not azure.authorized or azure.token['expires_in'] < 10:
            # send to azure login
            return False

        token_info = self._jwkaas.get_token_info(azure.access_token)

        self.user = token_info['unique_name']
        self.logout_url = "https://login.microsoftonline.com/{}/oauth2/v2.0/logout".format(token_info['tid'])

        if token_info:
            if 'roles' in token_info and self.role in token_info['roles']:
                return True
            else:
                logging.warning('Missing required role')
                return abort(401)
        else:
            logging.warning('Invalid access token')
            return abort(401)

    def login_request(self):
        # send to azure auth page
        return redirect(url_for("azure.login"))

    def logout_user(self):
        flask_login.logout_user()

    def get_logout_url(self):
        # returns logout url
        return self.logout_url

    def get_user(self):
        # returns username
        return self.user

    def auth_wrapper(self, f):
        def wrap(*args, **kwargs):
            if not self.is_authorized():
                return Response(status=401)

            response = f(*args, **kwargs)
            logging.info(response)
            return response
        return wrap

    def index_auth_wrapper(self, original_index):
        def wrap(*args, **kwargs):
            if self.is_authorized():
                return original_index(*args, **kwargs)
            else:
                return self.login_request()
        return wrap
