
from jupyterhub.spawner import Spawner
from jupyterhub.utils import can_connect
from traitlets import Bool, Dict, Integer, List, Unicode, default
from tornado import web
import asyncssh
import asyncio
from .sshoidcclient import SSHOIDCClient
import re
import os.path

class VirgoSpawner(Spawner):

   apptainer_paths = Dict({},
      help="Dict of possible Apptainer containers for user to pick from. Entries should be structured as 'name': 'path'.",
      config=True)

   default_container = Unicode("",
      help="Default container to use if unspecified",
      config=True)

   singleuser_config_path = Unicode("",
      help="Path to default config for single-user server within container",
      config=True)
   
   singleuser_extra_args = List([],
      help="List of additional args to pass to the single-user server",
      config=True)

   singleuser_path = Unicode("/opt/jupyterhub/bin",
      help="Path to 'bin' directory in single-user server's python environment within container",
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

   slurm_default_args = Unicode("",
      help="Default additional arguments to pass to Slurm job allocation",
      config=True)

   slurm_default_gpu = Bool(False,
      help="Switch to request GPU in job (default false)")

   slurm_default_num_cpus = Integer(1,
      help="Default value for number of CPUs to allocate",
      config=True)

   slurm_default_partition = Unicode("main",
      help="Default Slurm partition to request for job",
      config=True)

   slurm_default_reservation = Unicode("",
      help="Default reservation for Slurm allocation",
      config=True)
   
   slurm_default_working_directory = Unicode("",
      help="Default working directory on cluster storage for user sessions",
      config=True)

   slurm_gpu_reservation_args = Unicode("--gres=gpu:1",
      help="Argument(s) to add to `salloc` command if user requests GPU. Default is 'any GPU'. Can be reconfigured to add custom constraints like model numbers (e.g. `--constraint=mi100 --gres=gpu:1` if GPU jobs should always use an MI100)",
      config=True)

   slurm_node_suffix = Unicode("",
      help="DNS suffix for hosts on target cluster",
      config=True)   

   slurm_partition_options = List([],
      help="List of possible Slurm partitions to use for job, presented to user as a dropdown list",
      config=True)

   spawn_progress_events = List([],
      help="List of progress events for rendering spawn progress bar.")

   ssh_endpoint=Unicode("",
      help="Endpoint for SSH connection to submitter node",
      config=True)

   ssh_known_hosts = Unicode("/etc/ssh/ssh_known_hosts",
      help="Known hosts to be used for SSH host verification. If set to None, host verification is disabled.",
      config=True)

   workdir_required_prefix = Unicode("/",
      help="Required directory prefix for validating user dir (e.g. mount point for shared storage)",
      config=True)
   
   def _options_form_default(self):
      """
      Options form that is presented to the user after login, before spawning the single-user notebook
      """
      newline = '\n'
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
      if 'container' in self.user_options:
         default_container = self.user_options['container']
      else:
         default_container = ""
      return f"""

      <div class="form-group">
         <label for="reservation">Slurm Reservation</label>
         <input name="reservation" class="form-control" value="{default_reservation}"></input>
      </div>
      <br />
      <div class="form-group">
         <label for="partition">Slurm Partition</label>
         <select name="partition" class="form-select">
           {newline.join(f'<option value="{name}"{" selected" if name==default_partition else ""}>{name}</option>' for name in self.slurm_partition_options) }
 value="{default_partition}">
         </select>
      </div>
      <br />
      <div class="form-group">
         <label for="container">Container runtime for the session</label>
         <select name="container" class="form-select">
            {newline.join(f'<option value="{name}"{" selected" if name==default_container else ""}>{name}</option>' for name in self.apptainer_paths.keys()) }
         </select>
      <br />
      <div class="form-group">
         <label for="workdir">Your working directory on /lustre / Ihre Arbeitsverzeichnis auf Lustre</label>
         <input name="workdir" class="form-control" placeholder="e.g. /lustre/group/username/..." value="{default_workdir}"></input>
      </div>
      <br />
      <div class="form-group">
         <label>Does your task require a GPU? / Braucht Ihre Anwendung einen Grafikprozessor?</label>
         &nbsp; <label for="gpu_yes"><input name="req_gpu" id="gpu_yes" type="radio" value="1"> Yes</label>
         &nbsp; <label for="gpu_no"> <input name="req_gpu" id="gpu_no"  type="radio" value="0" checked> No</label>
      </div>
      <br />
      <div class="form-group">
         <label for="num_cpus">Number of CPUs to assign to job / Anzahl Prozessoren (1-{maxcpu})</label>
         <input name="num_cpus" class="form-control" type="number" value="1" min="1" max="{maxcpu}" onkeyup="if(value>{maxcpu}) value = {maxcpu}; if(value<0 || value == '0') value=1" required/>
      </div>
      """
      
   @default("options_from_form")
   def _default_options_from_form(formdata):
      def options_from_form(formdata,spawner):
         """
         Parse options from user web form. Should be redefined in JupyterHub config if any of the default user options are missing from the configured web form.
         """
         options = {}
         options['reservation'] = formdata['reservation'][0].strip()
         options['partition'] = formdata['partition'][0].strip()
         options['workdir'] = formdata['workdir'][0].strip()
         options['req_gpu'] = bool(int(formdata['req_gpu'][0]))
         options['num_cpus'] = int(formdata['num_cpus'][0].strip() or self.slurm_default_num_cpus)
         options['container'] = formdata['container'][0].strip()
         # Ensure running validate method here, to avoid faulty values being committed to DB
         spawner.validate_useroptions(options)
         return options
      return options_from_form

   def defaults(self, key):
      default_val = {
         "reservation": self.slurm_default_reservation,
         "partition": self.slurm_default_partition,
         "workdir": self.slurm_default_working_directory,
         "num_cpus": self.slurm_default_num_cpus,
         "req_gpu": False,
         "container": self.default_container or 0,
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
      req_gpu = self.user_options['req_gpu']
      container = self.user_options['container']

      slurm_default_args = self.slurm_default_args
      ssh_username = self.user.name

      """
      SLURM steps:
      = Connect to Virgo login node
      = salloc --no-shell (allocation options here): Define resource allocation that job steps run on. Return job ID.
      = retrieve job ID and hostname via squeue
      = Step 0 of job: srun --jobid {JOBID} -- $(print random port and hostname) : retrieve port, return it
      = Step 1 of job: srun --jobid {JOBID} /path/to/jupyterhub-singleuser : Actually run the server (having passed the hostname and port to the jupyterhub URL variable)
      """
      slurmscript_salloc = "unset SSH_ORIGINAL_COMMAND; "
      slurmscript_salloc += f"export WORKDIR='{workdir}' JUPYTERHUB_CONTAINER={self.apptainer_paths[container]} NUM_CPUS={num_cpus}; " # export PARTITION={partition}; export RESERVATION={reservation};
      if self.slurm_chgroup: # DEBUG: Switch GID for allocation if required
         slurmscript_salloc += f"sg {self.slurm_chgroup} -c \""

      slurmscript_salloc +=  "salloc --no-shell --job-name=jupyterhub_alloc --cpus-per-task=${NUM_CPUS}"
      if partition:
        slurmscript_salloc += f" --partition={partition}"
      if reservation:
         slurmscript_salloc += f" --reservation={reservation}"
      if req_gpu:
         slurmscript_salloc += f" {self.slurm_gpu_reservation_args}"
      if slurm_default_args:
         slurmscript_salloc += f" {slurm_default_args}"
      if self.slurm_chgroup: # terminate chgroup command if invoked
         slurmscript_salloc += "\""
      slurmscript_salloc += "; "
      slurmscript_salloc += "if [ $? != 0 ]; then exit 1; fi ; " ##  check return code from salloc ; if error in allocation, quit here
      slurmscript_salloc += "QUEUE=$(squeue -u ${USER} -o '%i %N' -S +i | tail -n 1); " ## retrieve last job ID for user
      slurmscript_salloc += "JOBID=$(cut -d' ' -f1 <<< $QUEUE); "
      slurmscript_salloc += "HOSTNAME=$(cut -d' ' -f2 <<< $QUEUE); "
      slurmscript_salloc += "echo $JOBID $HOSTNAME;"

      
      self.push_status("Establishing SSH connection to submitter...")
      # Perform SSH connection and run generated script
      try:
         async with await self.connect_ssh() as conn:
            # Perform task allocation
            self.push_status("Waiting for Slurm cluster allocation...")
            
            result_salloc = await self.send_ssh_command(conn, slurmscript_salloc)
            # salloc script returns string of "$JOBID $HOSTNAME". Parse this here"
            stdout_salloc = result_salloc.stdout
            job_id, hostname = stdout_salloc.split()
            self.slurm_job_id = int(job_id)
            self.slurm_job_hostname = hostname + self.slurm_node_suffix
            self.push_status(f"Slurm job started with ID {job_id}. Starting JupyterLab session...")
            # Job Step 0: check for free port on allocated host
            slurmscript_portdef = f"export JOBID={job_id} JUPYTERHUB_CONTAINER={self.apptainer_paths[container]} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path}; " # export PARTITION={partition}; export RESERVATION={reservation};
            slurmscript_portdef += "PORT=$( srun --jobid=$JOBID --job-name='jupyterhub_port' -- /usr/bin/apptainer exec ${JUPYTERHUB_CONTAINER} ${JUPYTERHUB_SINGLEUSER_PATH}/python3 -c  'from jupyterhub.utils import random_port; print(random_port())'); "
            slurmscript_portdef += "if [ \"$PORT\" = 0 ] || [ \"$PORT\" == '' ]; then scancel $JOBID; exit 2; fi; " # Tear down allocation and halt spawning if port allocation failed
            slurmscript_portdef += "echo $PORT;"
            result_portdef = await self.send_ssh_command(conn,slurmscript_portdef)
            # slurmscript_portdef returns the assigned port number as stdout
            stdout_portdef = result_portdef.stdout
            self.slurm_job_hostport = int(stdout_portdef)

            # Start the singleuser server with the defined port

            # Job Step 1
            # Load JupyterHub environment variables as inputs to srun
            env = super(VirgoSpawner, self).get_env()
            slurmscript_singleuser = f"export WORKDIR={workdir} JOBID={job_id} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path} JUPYTERHUB_CONTAINER={self.apptainer_paths[container]}; "
            slurmscript_singleuser += "RET=$("

            for item in env.items():
               if ("JUPYTERHUB_SERVICE_URL" not in item[0] and "SSH_ACCESS_TOKEN" not in item[0]): # Listening port for single-user server set dynamically here
                  slurmscript_singleuser += " %s='%s' " % item
            slurmscript_singleuser += f" JUPYTERHUB_SERVICE_URL=https://0.0.0.0:{self.slurm_job_hostport}/user/${self.user.name}/ "
            # Additional variables for Jupyter runtime to avoid trying to use home dir
            slurmscript_singleuser += "JUPYTER_DATA_DIR=\"${WORKDIR}/.jupy/data\" "
            slurmscript_singleuser += "JUPYTER_CONFIG_DIR=\"${WORKDIR}/.jupy/conf\" "

            slurmscript_singleuser += "srun --output=\"${WORKDIR}/.jupy/jupy_out.out\" --chdir=\"${WORKDIR}\"  --jobid=$JOBID --job-name='jupyterhub_session' -- "
            slurmscript_singleuser += "/usr/bin/apptainer exec -B /tmp -B /lustre ${JUPYTERHUB_CONTAINER} ${JUPYTERHUB_SINGLEUSER_PATH}/jupyterhub-singleuser --no-browser "
            if self.singleuser_config_path:
               slurmscript_singleuser += f"--config {self.singleuser_config_path} "
            if self.singleuser_extra_args:
               for arg in self.singleuser_extra_args:
                  slurmscript_singleuser += f"{arg} "
            slurmscript_singleuser += ">> \"${WORKDIR}/.jupy/jupyterlog.log\" 2>&1 & ); "
            slurmscript_singleuser += "sleep 5; "
            slurmscript_singleuser += "STATUS=$(squeue --step \"${JOBID}.1\" | grep ${JOBID}); "      # Check that the job step containing the server didn't immediately fail
            slurmscript_singleuser += "if [ \"$STATUS\" == \"\" ]; then scancel $JOBID; exit 3; fi; " # Tear down allocation if creation of single-user notebook failed
            slurmscript_singleuser += "exit 0;"

            result_singleuser = await self.send_ssh_command(conn,slurmscript_singleuser)
            stdout = result_singleuser.stdout
            stderr = result_singleuser.stderr
            self.log.debug(f"stdout was {stdout}")
            self.log.debug(f"stderr was {stderr}")

            self.push_status(f"Started JupyterLab server on allocated job {job_id}. Establishing connection to session...")
            self.log.debug(f"Server for user {self.user.name} was spawned with job ID {self.slurm_job_id} on hostname {self.slurm_job_hostname}, listening on port {self.slurm_job_hostport}")
            conn.close()
      # Catch exceptions from establishing SSH connection
      except asyncssh.Error as e:
         self.log.error(f"SSH error: Failed to spawn session for user {ssh_username}. Reason: {e.reason}.")
         raise web.HTTPError(500, f"Connection to Slurm submitter node failed while starting server: {e.reason}")
      return (self.slurm_job_hostname, self.slurm_job_hostport)

   async def poll(self): # TODO (return "None" if running; cleanup and other exit code if not)
      """
      Check whether job's hostname:port can be resolved (returns None if working), using "can_connect" from jupyterhub.utils.
      If connection fails, assume remote job has ended and return error status (1).
      """
      if can_connect(self.slurm_job_hostname, self.slurm_job_hostport):
         return None
      else:
         return 1

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
      if self.slurm_job_id == 0: # Job ID does not exist; do nothing
         self.log.error(f"send_slurm_command: {ssh_username}'s server tried to send '{command}' to undefined job")
         return True
      try:
         async with await self.connect_ssh() as conn:
            result = await conn.run("%s %d" % (command, self.slurm_job_id))
            stdout = result.stdout
            stderr = result.stderr
            retcode = result.exit_status
      except asyncssh.Error as e:
         self.log.error(f"SSH error: Failed to connect to Slurm node while sending {command} to job {self.slurm_job_id} for user {ssh_username}: {e.reason}")
         raise web.HTTPError(500, f"Failed to connect to job submitter node as user {ssh_username}: {e.reason}") 
      self.log.debug(f"sending command {command} to job {self.slurm_job_id} for user {ssh_username} returned {retcode} --- {stdout} --- {stderr}")         
      
      return (retcode == 0)


   def get_state(self):
      """Get current state"""
      state = super().get_state()
      if self.slurm_job_id:
         state['slurm_job_id'] = self.slurm_job_id
      if self.slurm_job_hostname:
         state['slurm_job_hostname'] = self.slurm_job_hostname
      if self.slurm_job_hostport:
         state['slurm_job_hostport'] = self.slurm_job_hostport
      return state

   def load_state(self, state):
      """load spawner state from DB"""
      super().load_state(state)
      if 'slurm_job_id' in state:
         self.slurm_job_id = state['slurm_job_id']
      if 'slurm_job_hostname' in state:
         self.slurm_job_hostname = state['slurm_job_hostname']
      if 'slurm_job_hostport' in state:
         self.slurm_job_hostport = state['slurm_job_hostport']

   def clear_state(self):
      """Clear current state after shutdown"""
      super().clear_state()
      self.slurm_job_id = 0
      self.slurm_job_hostname = ''
      self.slurm_job_hostport = 0
      self.spawn_progress_events = []

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
      if 'req_gpu' not in user_options:
         self.user_options['req_gpu'] = self.defaults('req_gpu')
      if 'container' not in user_options or user_options['container'] == "":
         self.user_options['container'] = self.defaults('container')

   def validate_useroptions(self, opts):
      """
      Function to validate values passed to user options.
      If everything is fine, return None.
      If any issues are identified, reset the value to its default and raise an error.
      """
      # Allowable characters in user-definable options
      pathspec_whitelist=re.compile(r'^[a-zA-Z0-9/._ -]*$')
      options_whitelist=re.compile(r'^[a-zA-Z0-9_-]*$')
      # Check for required path prefix if set in config
      if self.workdir_required_prefix:
         pathspec_prefix=re.compile(f"^{self.workdir_required_prefix}")
      # Reject path specs containing ".." to prevent traversal out of required prefix
      traversal=re.compile(r'\.\.')
      failed_items = list()
      validation_errors = list()

      # Test user strings against whitelisted characters and throw error if invalid
      for item, opt in opts.items():
         if 'num_cpus' in item:
            if not isinstance(opt, int) or opt > self.slurm_max_requestable_CPUs or opt < 1:
               failed_items.append(item)
               validation_errors.append(f"Invalid number of CPUs given (must be between 1-{self.slurm_max_requestable_CPUs})")
         elif 'workdir' in item:
            workdir_ok = True
            # For workdir: check that there are no invalid characters; string is not empty; required mount point is used if configured by admin
            if (not pathspec_whitelist.match(str(opt)) or traversal.findall(str(opt))):
               workdir_ok = False
               validation_errors.append("Working directory path contains invalid characters.")
            if (opt == ""):
               workdir_ok = False
               validation_errors.append("Working directory must not be blank")
            if (self.workdir_required_prefix and not pathspec_prefix.match(str(opt))):
               workdir_ok = False
               validation_errors.append(f"Working directory must begin with {self.workdir_required_prefix}")
            if not workdir_ok: # Only flag workdir in issue list once, even if multiple problems
               failed_items.append(item)
         elif 'req_gpu' in item:
            if not isinstance(opt, bool):
               failed_items.append(item)
               validation_errors.append("Invalid option for GPU request (true or false)")
         elif 'partition' in item:
            if not options_whitelist.match(str(opt)):
               failed_items.append(item)
               validation_errors.append(f"Invalid characters in option for {item}")
               break
            if not opt in self.slurm_partition_options:
               failed_items.append(item)
               validation_errors.append(f"Partition name not found in defined parition list: {', '.join(str(partition) for partition in self.slurm_partition_options)}")
         elif 'container' in item:
            if not options_whitelist.match(str(opt)):
               failed_items.append(item)
               validation_errors.append(f"Invalid characters in option for {item}")
               break
            if not opt in self.apptainer_paths.keys():
               failed_items.append(item)
               validation_errors.append(f"Container name not found in defined container list: {', '.join(str(key) for key in self.apptainer_paths.keys())}")
         #Fallthrough for string options not explicitly handled by other checks
         elif not options_whitelist.match(str(opt)):
            failed_items.append(item)
            validation_errors.append(f"Invalid characters in option for {item}")
      if failed_items:
         for item in failed_items:
            self.user_options[item] = self.defaults(item)
         raise web.HTTPError(400,f"Invalid user options: {'; '.join(str(i) for i in validation_errors)}. Values have been reset to their defaults. Please check these issues and try again.")
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
      remote_cert_dir = workdir + "/.jupy/.certs/"
      self.push_status("Initialising working directory...") 
      try:
         async with await self.connect_ssh() as conn:
            # Create target directory for SSL certs and ensure it is only user-accessible
            try: 
               result = await conn.run(f"mkdir -p \"{remote_cert_dir}\"; chmod 0700 \"{remote_cert_dir}\";")
            except Exception as e:
               self.log.error(f"Singleuser server for user {ssh_username} failed to start: Failure while running mkdir command. Exception {type(e).__name__}, arguments: {e.args}")
               raise web.HTTPerror(400, f"Failed to connect to job submitter node as user {ssh_username}.")
            status = result.exit_status
            if status != 0:
               self.log.error(f"Singleuser server for user {ssh_username} failed to start: Could not create directory {remote_cert_dir} for internal SSL certificates")
               raise web.HTTPError(400, f"Failed to initialise working directory. Please check that you have permissions to write to {workdir}, or specify a different path.")
            # Copy locally generated cert/key to server ("preserve" to keep proper permission modes)
            try: await asyncssh.scp(paths.values(), (conn, remote_cert_dir), preserve = True)
            except asyncssh.SFTPError as exc:
               self.log.error(f"Singleuser server for user {ssh_username} failed to start: Failed to copy internal SSL certificates to {remote_cert_dir}")
               raise web.HTTPError(400, f"Failed to initialise working directory. Please check that you have permissions to write to {workdir}, or specify a different path.")
            self.push_status("Working directory initialised")
            conn.close()
      except asyncssh.Error as e:
         self.log.error(f"Singleuser server for user {ssh_username} failed to start: Unable to establish SSH connection to submitter. Exception {type(e).__name__}, arguments: {e.args}")
         raise web.HTTPError(500, f"Failed to connect to job submitter node as user {ssh_username}: {e.reason}")
      remote_cert_paths = {key: remote_cert_dir + os.path.basename(localpath) for (key,localpath) in paths.items()}
      return remote_cert_paths

   def push_status(self, message):
      """
      Append event message to self.spawn_progress_events to be printed on spawner progress bar.
      """
      self.spawn_progress_events.append({"message": message})

   async def progress(self):
      """
      Handler for spawning progress bar
      """
      progress = 0
      next_event = 0
      len_events = 0
      while True:
         events = self.spawn_progress_events
         len_events = len(events)
         if next_event < len_events:
            for i in range (next_event, len_events):
               event = events[i]
               progress += (90 - progress) / 6
               yield {
                  'progress': int(progress),
                  'raw_event': event, 
                  'message': event['message']
               }
            next_event = len_events
         if self.ready: # Stop progress loop when server finishes spawning
            break
         await asyncio.sleep(1)

   async def connect_ssh(self):
      auth_state = await self.user.get_auth_state()
      token = auth_state['exchanged_tokens']['ssh']
      return await asyncssh.connect(self.ssh_endpoint, username=self.user.name, known_hosts=self.ssh_known_hosts, client_factory=lambda: SSHOIDCClient(token))

   async def send_ssh_command(self, conn, script):
      """
      Send command to remote session and handle response.
      """
      result = await conn.run(script)
      status = result.exit_status
      stderr = result.stderr
      if status != 0:
         if status == 1:
            reason = "Slurm job allocation failed with the following error: "
            reason += re.search(r'salloc: error: (.*)$',stderr).group(1)
         elif status == 2:
            reason = "Port allocation on worker node failed."
         elif status == 3:
            reason = "Failed to start single-user server process."
         else:
            reason = "Undefined reason."
         self.log.error(f"Server for user {self.user.name} failed to start: {reason}")
         self.log.error(f"stderr was {result.stderr}")
         conn.close()
         raise web.HTTPError(500, f"{reason}")
      return result
