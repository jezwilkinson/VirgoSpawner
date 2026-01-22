
from jupyterhub.spawner import Spawner
from traitlets import Bool, Integer, List, Unicode, default
from tornado import web
import asyncssh
from .sshoidcclient import SSHOIDCClient
import re
import os.path

class VirgoSpawner(Spawner):

   apptainer_path = Unicode("/scratch/alice/jwilkins/singularity/singleuser_5.1.0.sif",
      help="Path to Apptainer container for single-user notebooks",
      config=True)

   singleuser_path = Unicode("/opt/jupyterhub/bin",
      help="Path to single-user server's python environment within container",
      config=True)

   slurm_job_hostname = Unicode("",
      help="Hostname for Slurm node running notebook job")

   slurm_job_hostport = Integer(0,
      help="Port assigned to job on Slurm node")   

   slurm_job_id = Integer(0,
      help="Slurm job ID for Jupyter single-user notebook")

# Admin-defined limits for resource requests (configurable)
   slurm_max_requestable_CPUs = Integer(32,
      help="Maximum number of CPUs that the user can request",
      config=True)


   
# Default values for user-addressable job options

   slurm_chgroup = Unicode("",
      help="DEBUG: run singleuser server as this group's GID",
      config=True)

   slurm_default_gpu = Bool(False,
      help="Switch to request GPU in job (default false)")

   slurm_default_num_cpus = Integer(1,
      help="Requested number of CPUs for job")

   slurm_default_partition = Unicode("main",
      help="Requested Slurm partition for job",
      config=True)

   slurm_default_reservation = Unicode("",
      help="Reservation to submit Slurm job under",
      config=True)
   
   slurm_default_working_directory = Unicode("",
      help="Cluster storage directory to run session in",
      config=True)

   slurm_node_suffix = Unicode("",
      help="Domain suffix for hosts on target cluster",
      config=True)   
##

   ssh_endpoint=Unicode("virgo.hpc.gsi.de",
      help="Endpoint for SSH connection to submitter node",
      config=True)



   def _options_form_default(self):
      """
      Options form that is presented to the user after login, before spawning the single-user notebook
      """
      
      maxcpu = self.slurm_max_requestable_CPUs
      if 'partition' in self.user_options:
         default_partition = self.user_options['partition']
      else:
         default_partition = self.slurm_default_partition
      if 'reservation' in self.user_options:
         default_reservation = self.user_options['reservation']
      else:
         default_reservation = self.slurm_default_reservation
      if 'workdir' in self.user_options:
         default_workdir = self.user_options['workdir']
      else:
         default_workdir = ""

      return f"""

      <div class="form-group">
         <label for="reservation">Slurm Reservation</label>
         <input name="reservation" class="form-control" value="{default_reservation}"></input>
      </div>
      <br />
      <div class="form-group">
         <label for="partition">Slurm Partition (main, long)</label>
         <input name="partition" class="form-control" value="{default_partition}"></input>
      </div>
      <br />
      <div class="form-group">
         <label for="workdir">Your working directory on /lustre / Ihre Arbeitsverzeichnis auf Lustre</label>
         <input name="workdir" class="form-control" placeholder="e.g. /lustre/group/username/..." value="{default_workdir}"></input>
      </div>
      <br />
      <div class="form-group">
         <label>Does your task require a GPU? / Braucht Ihre Anwendung einen Grafikprozessor?</label>
         &nbsp; <label for="gpu_yes"><input name="req_GPU" id="gpu_yes" type="radio" value="1"> Yes</label>
         &nbsp; <label for="gpu_no"> <input name="req_GPU" id="gpu_no"  type="radio" value="0" checked> No</label>
      </div>
      <br />
      <div class="form-group">
         <label for="num_cpus">Number of CPUs to assign to job / Anzahl Prozessoren (1-{maxcpu})</label>
         <input name="num_cpus" class="form-control" type="number" value="1" min="1" max="{maxcpu}" onkeyup="if(value>{maxcpu}) value = {maxcpu}; if(value<0 || value == '0') value=1" required/>
      </div>
      """
      

   def options_from_form(self, formdata):
      """
      Parse options from user web form.
      """
      options = {}
      options['reservation'] = formdata['reservation'][0].strip()
      options['partition'] = formdata['partition'][0].strip()
      options['workdir'] = formdata['workdir'][0].strip()
      options['req_GPU'] = bool(int(formdata['req_GPU'][0]))
      options['num_cpus'] = int(formdata['num_cpus'][0].strip() or self.slurm_default_num_cpus)
      self.validate_useroptions(options)

      return options

   def defaults(self, key):
      default_val = {
         "reservation": self.slurm_default_reservation,
         "partition": self.slurm_default_partition,
         "workdir": self.slurm_default_working_directory,
         "num_cpus": self.slurm_default_num_cpus
      }
      return default_val[key]

   async def start(self):
      """
      Connect to job submission node and start notebook server job.
      Returns hostname and listening port of the cluster node running the single-user server.
      """

      # Parse relevant job options from defined user_options
      workdir = self.user_options['workdir']
      partition = self.user_options['partition']
      reservation = self.user_options['reservation']
      num_cpus = self.user_options['num_cpus']
      req_GPU = self.user_options['req_GPU']

      ssh_username = self.user.name

      """
      SLURM steps:
      = Connect to Virgo login node
      = salloc --no-shell (allocation options here): Define resource allocation that job steps run on. Return job ID.
      = retrieve job ID and hostname via squeue
      = Step 0 of job: srun --jobid {JOBID} -- $(print random port and hostname) : retrieve port, return it
      = Step 1 of job: srun --jobid {JOBID} /path/to/jupyterhub-singleuser : Actually run the server (having passed the hostname and port to the jupyterhub URL variable)
      """

      slurmscript = "unset SSH_ORIGINAL_COMMAND; "
      slurmscript += f"export WORKDIR='{workdir}' JUPYTERHUB_CONTAINER={self.apptainer_path} NUM_CPUS={num_cpus} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path}; " # export PARTITION={partition}; export RESERVATION={reservation};
      if self.slurm_chgroup: # DEBUG: Switch GID for allocation if required
         slurmscript += f"sg {self.slurm_chgroup} -c \""

      slurmscript +=  "salloc --no-shell --job-name=jupyterhub_reservation --cpus-per-task=${NUM_CPUS}"
      if partition:
        slurmscript += f" --partition={partition}"
      if reservation:
         slurmscript += f" --reservation={reservation}"
      if req_GPU:
         slurmscript += " --constraint=mi100 --gres=gpu:1"
      if self.slurm_chgroup: # terminate chgroup command if invoked
         slurmscript += "\""
      slurmscript += "; "
      slurmscript += "if [ $? != 0 ]; then exit 1; fi ; " ##  check return code from salloc ; if error in allocation, quit here
      slurmscript += "QUEUE=$(squeue -u ${USER} -o '%i %N' -S +i | tail -n 1); " ## retrieve last job ID for user
      slurmscript += "JOBID=$(cut -d' ' -f1 <<< $QUEUE); "
      slurmscript += "HOSTNAME=$(cut -d' ' -f2 <<< $QUEUE); "

      # Job Step 0: check for free port on allocated host
      slurmscript += "PORT=$( srun --jobid=$JOBID --job-name='jupyterhub_port' -- /usr/bin/apptainer exec ${JUPYTERHUB_CONTAINER} ${JUPYTERHUB_SINGLEUSER_PATH}/python3 -c  'from jupyterhub.utils import random_port; print(random_port())'); "
      slurmscript += "if [ \"$PORT\" = 0 ] || [ \"$PORT\" == '' ]; then scancel $JOBID; exit 2; fi; " # Tear down allocation and halt spawning if port allocation failed
      slurmscript += "unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY; "
      # Job Step 1
      # Load JupyterHub environment variables as inputs to srun
      env = super(VirgoSpawner, self).get_env()
      slurmscript += "RET=$("
      
      for item in env.items():
         if ("SCOPE" in item[0]):  # JUPYTERHUB_*_SCOPE variables are JSON arrays and need to be specially quoted
            slurmscript += " %s='%s' " % item  
         elif ("JUPYTERHUB_SERVICE_URL" not in item[0] and "SSH_ACCESS_TOKEN" not in item[0]): # Listening port for single-user server set dynamically here
            slurmscript += " %s=%s " % item
      slurmscript += " JUPYTERHUB_SERVICE_URL=https://0.0.0.0:$PORT/user/${USER}/ "
      
      # Additional variables for Jupyter runtime to avoid trying to use home dir
      slurmscript += "JUPYTER_DATA_DIR=\"${WORKDIR}/.jupy/data\" "
      slurmscript += "JUPYTER_CONFIG_PATH=\"${WORKDIR}/.jupy/conf\" "
      
      slurmscript += "srun --output=\"${WORKDIR}/.jupy/jupy_out.out\" --chdir=\"${WORKDIR}\"  --jobid=$JOBID --job-name='jupyterhub_singleuser' -- "
      slurmscript += "/usr/bin/apptainer exec -B /tmp -B /lustre ${JUPYTERHUB_CONTAINER} ${JUPYTERHUB_SINGLEUSER_PATH}/jupyterhub-singleuser --no-browser >> \"${WORKDIR}/.jupy/jupyterlog.log\" 2>&1 & ); "
      slurmscript += "sleep 5; "
      slurmscript += "STATUS=$(squeue --step \"${JOBID}.1\" | grep ${JOBID}); "      # Check that the job step containing the server didn't immediately fail
      slurmscript += "if [ \"$STATUS\" == \"\" ]; then scancel $JOBID; exit 3; fi; " # Tear down allocation if creation of single-user notebook failed
      slurmscript += "echo $JOBID $HOSTNAME $PORT;"
      self.log.debug(slurmscript)

      token = self.environment['SSH_ACCESS_TOKEN']
      # Perform SSH connection and run generated script
      async with asyncssh.connect(self.ssh_endpoint, username=ssh_username, known_hosts=None, client_factory = lambda: SSHOIDCClient(token) ) as conn:
         result = await conn.run(slurmscript)
         status = result.exit_status
         stdout = result.stdout
         stderr = result.stderr

         self.log.debug(f"stdout was {stdout}")
         self.log.debug(f"stderr was {stderr}")
         if status != 0:
            if status == 1:
               reason = "Job allocation failed with the following error: "
               reason += re.search(r'.*salloc: error:.*',stderr).string
            elif status == 2:           
               reason = "Port allocation on worker node failed."
            elif status == 3:
               reason = "Failed to start single-user server process."
            else:
               reason = "Undefined reason."
            self.log.error(f"Server for user {self.user.name} failed to start: {reason}")
            conn.close()
            raise web.HTTPError(500, f"Unable to start server over SLURM. {reason}")

         job_id, hostname, port = stdout.split()
         self.slurm_job_id = int(job_id)
         self.slurm_job_hostname = hostname + self.slurm_node_suffix
         self.slurm_job_hostport = int(port)
         self.log.debug(f"Server for user {self.user.name} was spawned with job ID {self.slurm_job_id} on hostname {self.slurm_job_hostname}, listening on port {self.slurm_job_hostport}")
         conn.close()
      return (self.slurm_job_hostname, self.slurm_job_hostport)

   async def poll(self): # TODO (return "None" if running; cleanup and other exit code if not)
      return None

   async def stop(self, now=False):
      """
      Terminate the remote process and clear up the spawner state. By default, "scancel" to the allocated process will send signal 15 (sigterm)
      """
      status = await(self.send_slurm_command("scancel"))
      self.clear_state()
      return
   
   async def send_slurm_command(self, command):
      """
      Send a SLURM command to the process (used by poll() and stop())
      """
      ssh_username = self.user.name
      auth_state = await self.user.get_auth_state()
      token = auth_state['exchanged_tokens']['ssh']
      async with asyncssh.connect(self.ssh_endpoint, username=ssh_username, client_factory = lambda: SSHOIDCClient(token), known_hosts=None) as conn:
         result = await conn.run("%s %d" % (command, self.slurm_job_id))
         stdout = result.stdout
         stderr = result.stderr
         retcode = result.exit_status
      self.log.debug(f"sending command {command} to job {self.slurm_job_id} for user {self.user.name} returned {retcode} --- {stdout} --- {stderr}")         
      
      return (retcode == 0)


   def get_state(self):
      """Get current state"""
      state = super().get_state()
      if self.slurm_job_id:
         state['slurm_job_id'] = self.slurm_job_id
      return state

   def load_state(self, state):
      """load spawner state from DB"""
      super().load_state(state)
      if 'slurm_job_id' in state:
         self.slurm_job_id = state['slurm_job_id']

   def clear_state(self):
      """Clear current state after shutdown"""
      super().clear_state()
      self.slurm_job_id = 0

   def apply_user_options(self, spawner, user_options):
      """
      Hook before spawn to validate user options
      """
      # Validation of user inputs (raise exception in case any invalid values have been entered)
      if self.validate_useroptions(user_options) is not None:
         raise web.HTTPError(400, f"User options could not be validated. Please try again.")
      # If any user options were unset, set them to their default (per admin config)
      if 'reservation' not in user_options or user_options['reservation'] == "":
         self.user_options['reservation'] = self.slurm_default_reservation
      if 'partition' not in user_options or user_options['partition'] == "":
         self.user_options['partition'] = self.defaults('partition')
      if 'workdir' not in user_options or user_options['workdir'] == "":
         self.user_options['workdir'] = self.defaults('workdir')
      if 'num_cpus' not in user_options or user_options['num_cpus'] == None:
         self.user_options['num_cpus'] = self.defaults('num_cpus')

   def validate_useroptions(self, opts):
      """
      Function to validate values passed to user options.
      If everything is fine, return None.
      If any issues are identified, reset the value to its default and raise an error.
      """
      pathspec_whitelist=r'^[a-zA-Z0-9/._ -]*$'
      options_whitelist=r'^[a-zA-Z0-9_-]*$'

      if opts['num_cpus'] > self.slurm_max_requestable_CPUs or opts['num_cpus'] < 1:
         raise ValueError(
            f"Invalid number of CPUs given / Ungültige Anzahl von CPUs gegeben (1-32)"
         )
      failed_items = list()
      # Test user strings against whitelisted characters and throw error if invalid
      for item in opts:
         if 'num_cpus' in item:
            if opts[item] > self.slurm_max_requestable_CPUs or opts[item] < 1:
               failed_items.append(item)
         elif 'workdir' in item:
            if not re.match(pathspec_whitelist, str(opts[item])):
               failed_items.append(item)
         elif not re.match(options_whitelist, str(opts[item])):
            failed_items.append(item)
      if failed_items:
         for item in failed_items:
            self.user_options[item] = self.defaults(item)
         raise web.HTTPError(400,f"User options for {', '.join(str(i) for i in failed_items)} were invalid. Values have been reset to their defaults. Please try spawning again.")
         return failed_items
      else:
         return None

   async def move_certs(self, paths):
      """
      Copies the internally-signed key/cert pair for SSL termination from the 
      webserver to the remote server via SCP.
      The certificates are generated locally on the JupyterHub node in a location defined
      by the config `internal_certs_location` (provided in the `paths` dict).
      `.move_certs` is called after certs for the singleuser notebook have been created by create_certs,
      and before the server is spawned with start().
      
      Arguments:
         paths (dict): a dict of paths for key, cert, and CA.
             These paths will be resolvable and readable by the Hub process,
             but not by the notebook server.
      Returns:
         remote_cert_paths (dict): a dict of paths for key, cert, and CA
             on the remote storage.
             These paths should be resolvable and readable by the notebook
             server to be launched.
            
      """

      workdir = self.user_options['workdir']      
      ssh_username = self.user.name
      token = self.environment['SSH_ACCESS_TOKEN']
      remote_cert_dir = workdir + '/.jupy/.certs/'
      async with asyncssh.connect(self.ssh_endpoint, username=ssh_username, known_hosts=None, client_factory = lambda: SSHOIDCClient(token) ) as conn:
         # Create target directory for SSL certs and ensure it is only user-accessible
         try: 
            result = await conn.run(f"mkdir -p \"{remote_cert_dir}\"; chmod 0700 \"{remote_cert_dir}\";")
         except Exception as e:
            self.log.error(f"Singleuser server for user {ssh_username} failed to start: Could not establish SSH connection while creating certificate dir for user. Exception {type(e).__name__,}, arguments: {e.args}")
            raise web.HTTPerror(400, f"Failed to connect to job submitter node as user {ssh_username}.")
         status = result.exit_status
         if status != 0:
            self.log.error(f"Singleuser server for user {ssh_username} failed to start: Could not create directory {workdir} for internal SSL certificates")
            raise web.HTTPError(400, f"Failed to initialise working directory. Please check that you have permissions to write to {workdir}, or specify a different path.")
         # Copy locally generated cert/key to server ("preserve" to keep proper permission modes)
         try: await asyncssh.scp(paths.values(), (conn, remote_cert_dir), preserve = True)
         except asyncssh.SFTPError as exc:
            self.log.error(f"Singleuser server for user {ssh_username} failed to start: Failed to copy internal SSL certificates to {workdir}")
            raise web.HTTPError(400, f"Failed to initialise working directory. Please check that you have permissions to write to {workdir}, or specify a different path.")

         conn.close()
      remote_cert_paths = {key: remote_cert_dir + os.path.basename(localpath) for (key,localpath) in paths.items()}
      return remote_cert_paths

