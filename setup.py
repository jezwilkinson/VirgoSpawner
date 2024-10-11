from setuptools import setup

with open("README.md") as f:
   long_description = f.read()

setup(
   name="virgospawner",
   packages=["virgospawner"],
   description="""VirgoSpawner: a JupyterHub spawner to spawn notebooks to a Slurm cluster via a remote submit node.""",
   long_description=long_description,
   long_description_content_type="text/markdown",
   author="Jeremy Wilkinson",
   author_email="j.wilkinson@gsi.de",
   license="BSD",
   version="0.1.0",
   platforms="Linux",
   keywords=["Jupyter"],
   classifiers=[
      "Intended Audience :: Developers",
      "Intended Audience :: System Administrators",
      "Intended Audience :: Science/Research",
      "Programming Language :: Python",
      "Programming Language :: Python :: 3",
   ],
   python_requires=">=3.6",
   install_require={
      "jupyterhub>=5.1.0",
      "asyncssh",
      "traitlets"
   },
   entry_points={
      'jupyterhub.spawners': [
         'virgoservice = virgospawner:VirgoSpawner',
         'sshservice = virgospawner:SSHSpawner',
      ]
   },
)