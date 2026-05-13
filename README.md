# JupyterHub VirgoSpawner
A JupyterHub spawner plugin allowing job submission over a separated Slurm controller over SSH. Developed for use by the JupyterHub deployment at GSI Darmstadt, see [1] for further details. Has support for validating user options, handling certificates for JupyterHub's internal SSL, and provides error handling for issues with Slurm job submission.

Designed for use with [SWAN KeyCloakAuthenticator](https://github.com/swan-cern/jupyterhub-extensions/tree/master/KeyCloakAuthenticator) for authentication and token exchange. An OIDC token for SSH login is exchanged with the login provider, and passed to the Slurm submitter node as a login credential. This is used by a dedicated PAM module on the submitter node for user authentication.

[1] J. Wilkinson, "[Deploying Infrastructure-as-a-Service at GSI with JupyterHub](https://doi.org/10.14279/eceasst.v85.2691)", *ECEASST, vol. 85, Dec. 2025.*
