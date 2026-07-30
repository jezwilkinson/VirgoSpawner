from keycloakauthenticator import KeyCloakAuthenticator
from tornado import web

class VirgoAuthHelper(KeyCloakAuthenticator):
   async def authenticate(self,handler,data=None):
      """
      Wrapper for standard KeyCloakAuthenticator flow.
      Handles case where token from KeyCloak is missing a 'preferred_username' claim (i.e. no POSIX 
      account in user database).
      """
      try:
         user = await super().authenticate(handler,data=data)
      except ValueError:
         self.log.error("No username found for attempted login")
         raise web.HTTPError(403,"No Linux username found for your account. A valid POSIX account is required to use this service.")
      return user