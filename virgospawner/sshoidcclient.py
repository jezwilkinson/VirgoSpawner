from asyncssh import SSHClient


class SSHOIDCClient(SSHClient):
   """
      Extension for the asyncssh client to allow responding to kbd-interactive challenge for access token.
   """
   def __init__(self, oidc_token: str):
      super().__init__()
      self._oidc_token = oidc_token

   async def kbdint_auth_requested(self):
      return ""
   
   async def kbdint_challenge_received(self, name, instructions, lang, prompts):
      if prompts:
         oidc_token = self._oidc_token
         return [oidc_token]
      
      else:
         return []

