
from jupyterhub.spawner import Spawner
from traitlets import Bool, Integer, List, Unicode, default, observe
from tornado import web
import asyncssh
import re
from shlex import quote



class VirgoSpawner(Spawner):

   apptainer_path = Unicode("/scratch/alice/jwilkins/singularity/singleuser_5.1.0.sif",
      help="Path to Apptainer container for single-user notebooks",
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
##

   ssh_endpoint=Unicode("virgo.hpc.gsi.de",
      help="Endpoint for SSH connection to submitter node",
      config=True)

   ssh_keyfile=Unicode("",
      help="Filepath for SSH key (Proof-of-concept only, not for production)",
      config=True)

   ssh_keyphrase=Unicode("",
      help="keyphrase for SSH key (Proof-of-concept only, not for production)")

   def _options_form_default(self):
      """
      Options form that is presented to the user after login, before spawning the single-user notebook
      """
      async def render_form(spawner):
         maxcpu = spawner.slurm_max_requestable_CPUs
         default_partition = spawner.slurm_default_partition
         default_reservation = spawner.slurm_default_reservation

         return f"""

         <div class="form-group">
            <label for="reservation">Slurm Reservation</label>
            <input name="reservation" class="form-control" default="{default_reservation}"></input>
         </div>
         <br />
         <div class="form-group">
            <label for="partition">Slurm Partition (main, long)</label>
            <input name="partition" class="form-control" default="{default_partition}"></input>
         </div>
         <br />
         <div class="form-group">
            <label for="workdir">Your working directory on /lustre / Ihre Arbeitsverzeichnis auf Lustre</label>
            <input name="workdir" class="form-control" placeholder="e.g. /lustre/group/username/..."></input>
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
            <input name="num_cpus" class="form-control" type="number" value="1" min="1" max="{maxcpu}" onkeyup="if(value>{maxcpu}) value = {maxcpu}; if(value<0 || value == '0') value=1"/>
         </div>
         <br />
         <div class="form-group">
            <label for="ssh_keyphrase">SSH keyphrase</label>
            <input name="ssh_keyphrase" class="form-control" type="password" />
         </div>
         """
      return render_form

   def options_from_form(self, formdata):
      """
      Parse options from user web form.
      """
      options = {}
      options['reservation'] = formdata['reservation'][0]
      options['partition'] = formdata['partition'][0]
      options['workdir'] = formdata['workdir'][0]
      options['req_GPU'] = bool(int(formdata['req_GPU'][0]))
      options['num_cpus'] = int(formdata['num_cpus'][0])
      options['ssh_keyphrase'] = formdata['ssh_keyphrase'][0]

      return options
      
   async def start(self):
      """
      Connect to job submission node and start notebook server job
      """
      
      # If any user options were unset, set them to their default (per admin config)
      if 'reservation' not in self.user_options or self.user_options['reservation'] == "":
         self.user_options['reservation'] = self.slurm_default_reservation
      if 'partition' not in self.user_options or self.user_options['partition'] == "":
         self.user_options['partition'] = self.slurm_default_partition
      if 'workdir' not in self.user_options or self.user_options['workdir'] == "":
         self.user_options['workdir'] = self.slurm_default_working_directory
      if 'num_cpus' not in self.user_options:
         self.user_options['num_cpus'] = self.slurm_default_num_cpus

      # Validation of user inputs (raise exception in case any invalid values have been entered)
      if self.user_options['num_cpus'] > self.slurm_max_requestable_CPUs or self.user_options['num_cpus'] < 1:
         raise ValueError(
            f"Invalid number of CPUs given / Ungültige Anzahl von CPUs gegeben (1-32)"
         )

      # Sanitise Unicode inputs to be used in bash scripts with shlex.quote()
      workdir = (self.user_options['workdir'])
      partition = quote(self.user_options['partition'])
      reservation = quote(self.user_options['reservation'])
      
      num_cpus = self.user_options['num_cpus']
      
      ssh_username = self.user.name
      ssh_keyfile = self.ssh_keyfile
      self.ssh_keyphrase = self.user_options['ssh_keyphrase'] # DEBUG: to be replaced with proper auth
      try:
         k = asyncssh.read_private_key(ssh_keyfile,self.ssh_keyphrase)
      except:
         raise web.HTTPError(500,
            f"Unable to decrypt private key."
         )

      """
      SLURM steps:
      = Connect to Virgo login node
      = salloc --no-shell (allocation options here): Define resource allocation that job steps run on. Return job ID.
      = retrieve job ID and hostname via squeue
      = Step 0 of job: srun --jobid {JOBID} -- $(print random port and hostname) : retrieve port, return it
      = Step 1 of job: srun --jobid {JOBID} /path/to/jupyterhub-singleuser : Actually run the server (having passed the hostname and port to the jupyterhub URL variable)
      """

      slurmscript = ""
      if self.slurm_chgroup: # DEBUG: Switch GID for allocation if required
         slurmscript += f"sg {self.slurm_chgroup} -c '"

      slurmscript +=  f"salloc --no-shell --chdir={workdir} --cpus-per-task={num_cpus}"
      if self.user_options['partition']: 
        slurmscript += f" --partition={self.user_options['partition']}"
      if self.user_options['reservation']:
         slurmscript += f" --reservation={self.user_options['reservation']}"
      if self.user_options['req_GPU']:
         slurmscript += " --constraint=mi100 --gres=gpu:1"
      if self.slurm_chgroup: # terminate chgroup command if invoked
         slurmscript += "'"
      slurmscript += "; "
      slurmscript += "if [ $? != 0 ]; then exit 1; fi ; " ##  check return code from salloc ; if error in allocation, quit here
      slurmscript += "QUEUE=$(squeue -u $USER -o '%F %N' | tail -n 1); " ## retrieve last job ID for user
      slurmscript += "JOBID=$(cut -d' ' -f1 <<< $QUEUE); "
      slurmscript += "HOSTNAME=$(cut -d' ' -f2 <<< $QUEUE); "

      # Step 0
      slurmscript += f"PORT=$( srun --chdir={workdir} --jobid=$JOBID --job-name='jupyterhub_port' -- /usr/bin/apptainer exec {self.apptainer_path} /opt/conda/bin/python3 -c  'from jupyterhub.utils import random_port; print(random_port())'); "
      slurmscript += "if [ $? != 0 ]; then scancel $JOBID; exit 2; fi; " # Tear down allocation and halt spawning if port allocation failed

      # Step 1

      # Load JupyterHub envirnonment variables as inputs to srun
      env = super(VirgoSpawner, self).get_env()
      slurmscript += "RET=$("
      
      for item in env.items():
         if ("SCOPE" in item[0]):  # JUPYTERHUB_*_SCOPE variables are JSON arrays and need to be specially quoted
            slurmscript += " %s='%s' " % item  
         elif (item[0] != "JUPYTERHUB_SERVICE_URL"): # Listening port for single-user server set dynamically here
            slurmscript += " %s=%s " % item
      slurmscript += f" JUPYTERHUB_SERVICE_URL=http://0.0.0.0:$PORT/user/{ssh_username}/ "
      
      # Additional variables for Jupyter runtime to avoid trying to use home dir
      slurmscript += f" JUPYTER_DATA_DIR={workdir}/.jupy/data "
      slurmscript += f" JUPYTER_CONFIG_PATH={workdir}/.jupy/conf "
      
      slurmscript += f"srun --output={workdir}/jupy_out.out --chdir={workdir}  --jobid=$JOBID --job-name='jupyterhub_singleuser' -- "

      slurmscript += " /usr/bin/apptainer exec -B /tmp -B /lustre {self.apptainer_path} /opt/conda/bin/jupyterhub-singleuser >> {workdir}/jupyterlog.log 2>&1 &); "
 
      slurmscript += "if [ $? != 0 ]; then scancel $JOBID; exit 3; fi; " # Tear down allocation if creation of single-user notebook failed
      slurmscript += "echo $JOBID $HOSTNAME $PORT;"
      self.log.debug(slurmscript)

      # Perform SSH connection and run generated script
      async with asyncssh.connect(self.ssh_endpoint, username=ssh_username, client_keys=[(k)], known_hosts=None) as conn:
         result = await conn.run(slurmscript)
         status = result.exit_status
         stdout = result.stdout
         stderr = result.stderr

         self.log.debug("stdout was {}".format(stdout))
         self.log.debug("stderr was {}".format(stderr))
         if status != 0:
            raise web.HTTPError(500,
               "Unable to start server over SLURM.")

         job_id, hostname, port = stdout.split()
         self.slurm_job_id = int(job_id)
         self.slurm_job_hostname = hostname + ".gsi.de"
         self.slurm_job_hostport = int(port)

         self.log.debug("Server was spawned with job ID {} on hostname {}, listening on port {}".format(self.slurm_job_id, self.slurm_job_hostname, self.slurm_job_hostport))

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
      ssh_keyfile = self.ssh_keyfile
      ssh_keyphrase = self.user_options['ssh_keyphrase']
      k = asyncssh.read_private_key(ssh_keyfile,ssh_keyphrase)
      
      async with asyncssh.connect(self.ssh_endpoint, username=ssh_username, client_keys=[(k)], known_hosts=None) as conn:
         result = await conn.run("%s %d" % (command, self.slurm_job_id))
         stdout = result.stdout
         stderr = result.stderr
         retcode = result.exit_status
      self.log.debug("sending command {} to job {} for user {} returned {} --- {} --- {}".format(command, self.slurm_job_id, self.user.name, stdout, stderr, retcode))         
      
      return (retcode == 0)

