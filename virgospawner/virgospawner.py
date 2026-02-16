
from jupyterhub.spawner import Spawner
from traitlets import Bool, Integer, List, Unicode, default
from tornado import web
import asyncssh
import asyncio
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
      help="Domain suffix for hosts on target cluster",
      config=True)   

   spawn_progress_events = List([],
      help="List of progress events for rendering spawn progress bar.")

   ssh_endpoint=Unicode("",
      help="Endpoint for SSH connection to submitter node",
      config=True)

   ssh_known_hosts = Unicode("/etc/ssh/ssh_known_hosts",
      help="Known hosts to be used for SSH host verification. If set to None, host verification is disabled.",
      config=True)

   workdir_required_prefix = Unicode("",
      help="Required directory prefix for validating user dir (e.g. mount point for shared storage)",
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
      slurmscript_salloc = "unset SSH_ORIGINAL_COMMAND; "
      slurmscript_salloc += f"export WORKDIR='{workdir}' JUPYTERHUB_CONTAINER={self.apptainer_path} NUM_CPUS={num_cpus} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path}; " # export PARTITION={partition}; export RESERVATION={reservation};
      if self.slurm_chgroup: # DEBUG: Switch GID for allocation if required
         slurmscript_salloc += f"sg {self.slurm_chgroup} -c \""

      slurmscript_salloc +=  "salloc --no-shell --job-name=jupyterhub_reservation --cpus-per-task=${NUM_CPUS}"
      if partition:
        slurmscript_salloc += f" --partition={partition}"
      if reservation:
         slurmscript_salloc += f" --reservation={reservation}"
      if req_GPU:
         slurmscript_salloc += f" {self.slurm_gpu_reservation_args}"
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
            slurmscript_portdef = f"export JOBID={job_id} JUPYTERHUB_CONTAINER={self.apptainer_path} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path}; " # export PARTITION={partition}; export RESERVATION={reservation};
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
            slurmscript_singleuser = f"export WORKDIR={workdir} JOBID={job_id} JUPYTERHUB_SINGLEUSER_PATH={self.singleuser_path} JUPYTERHUB_CONTAINER={self.apptainer_path}; "
            slurmscript_singleuser += "RET=$("

            for item in env.items():
               if ("SCOPE" in item[0]):  # JUPYTERHUB_*_SCOPE variables are JSON arrays and need to be specially quoted
                  slurmscript_singleuser += " %s='%s' " % item  
               elif ("JUPYTERHUB_SERVICE_URL" not in item[0] and "SSH_ACCESS_TOKEN" not in item[0]): # Listening port for single-user server set dynamically here
                  slurmscript_singleuser += " %s=%s " % item
            slurmscript_singleuser += f" JUPYTERHUB_SERVICE_URL=https://0.0.0.0:{self.slurm_job_hostport}/user/${self.user.name}/ "
            # Additional variables for Jupyter runtime to avoid trying to use home dir
            slurmscript_singleuser += "JUPYTER_DATA_DIR=\"${WORKDIR}/.jupy/data\" "
            slurmscript_singleuser += "JUPYTER_CONFIG_PATH=\"${WORKDIR}/.jupy/conf\" "

            slurmscript_singleuser += "srun --output=\"${WORKDIR}/.jupy/jupy_out.out\" --chdir=\"${WORKDIR}\"  --jobid=$JOBID --job-name='jupyterhub_singleuser' -- "
            slurmscript_singleuser += "/usr/bin/apptainer exec -B /tmp -B /lustre ${JUPYTERHUB_CONTAINER} ${JUPYTERHUB_SINGLEUSER_PATH}/jupyterhub-singleuser --no-browser >> \"${WORKDIR}/.jupy/jupyterlog.log\" 2>&1 & ); "
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
      try:
         async with await self.connect_ssh() as conn:
            result = await conn.run("%s %d" % (command, self.slurm_job_id))
            stdout = result.stdout
            stderr = result.stderr
            retcode = result.exit_status
      except asyncssh.Error as e:
         self.log.error(f"SSH error: Failed to connect to Slurm node while sending {command} to job {self.slurm_job_id} for user {self.user.name}: {e.reason}")
         raise web.HTTPError(500, f"Failed to connect to job submitter node as user {ssh_username}: {e.reason}") 
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

   def validate_useroptions(self, opts):
      """
      Function to validate values passed to user options.
      If everything is fine, return None.
      If any issues are identified, reset the value to its default and raise an error.
      """
      pathspec_whitelist=re.compile(r'^[a-zA-Z0-9/._ -]*$')
      options_whitelist=re.compile(r'^[a-zA-Z0-9_-]*$')
   
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
            # For workdir: check that there are no invalid characters; string is not empty; required mount point is used if configured by admin
            if (not pathspec_whitelist.match(str(opts[item]))) or (item == "") or (self.workdir_required_prefix and not re.match(f"^{self.workdir_required_prefix}")):
               failed_items.append(item)
         elif not options_whitelist.match(str(opts[item])):
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
         self.log.error(f"stderr was {result.stderr}")
         conn.close()
         raise web.HTTPError(500, f"Unable to start server over SLURM. {reason}")
      return result