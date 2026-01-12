"""SlurmRest spawner

This middleware aims to use Slurm REST API to spawn singleuser-server on Slurm cluster.

Spawner authenticate with admin token to submit (sbatch) and control (qstat, qdel) jobs.

This Spawner is a rewrite of BatchSpawner.
"""
import asyncio
import os
import requests
import random
from enum import Enum
import socket
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from jupyterhub.utils import random_port
from jupyterhub.spawner import Spawner
from traitlets import Float, Integer, Unicode, List, Dict


class JobStatus(Enum):
    NOTFOUND = 0
    FAILED = 0
    CANCELLED = 0
    COMPLETED = 0
    RUNNING = 1
    PENDING = 2
    UNKNOWN = 3

class SlurmRestSpawner(Spawner):
    """SlurmRestSpawner class for batch job submission through Slurm Rest API

    This class use Rest API to manage job on a Slurm cluster. Initialy based on
    BatchSpawner.
    """

    slurm_rest_api = Unicode(
        "https://slurm-restapi.local/slurm/v0.0.41",
        help="Slurm Rest endpoint (e.g. https://slurm.local/slurm/v0.0.41)"
    ).tag(config=True)

    start_timeout = Integer(
        300,
        help="Timeout for waiting job to be scheduled"
    ).tag(config=True)

    job_name_prefix = Unicode(
        "jhub-",
        help="Prefix for job name, spawner add login."
    ).tag(config=True)

    job_working_path_prefix = Unicode(
        "/home/",
        help="Prefix for job working path name, spawner add /login."
    ).tag(config=True)

    options_form_template = Unicode(
        F"{Path(__file__).resolve().parent}/default_options_form.j2",
        help="Absolute path for options_form template"
    ).tag(config=True)

    job_script_template = Unicode(
        F"{Path(__file__).resolve().parent}/default_job_script.j2",
        help="Absolute path for job script template"
    ).tag(config=True)

    profiles = List(
        [
        ('Default Job', 'default-job', dict(cpus_per_task = 1, memory_per_node = { "number": 4000}, time_limit = 480, partition = 'cpu')),
        ('GPU Job', 'gpu-job', dict(cpus_per_task = 8, memory_per_node = { "number": 32000}, time_limit = 480, partition = 'gpu', tres_per_job="gres/gpu:1"))
        ],
        help="Profiles (ressources for job) available to user."
    ).tag(config=True)

    jupyter_environments = Dict(
        {
            "default": {
                "description": "Default Jupyter",
                "path": "",
                "modules": "",
                "image": "",
                "script_prologue_template": """
                    echo "Starting Jupyterhub singleuser for {{ user }} on port  ${port}"
                    echo "Configured jupyter_environment PATH : {{ env.path }}"
                    echo "Configured jupyter_environment Image : {{ env.image }}"
                    echo "Configured jupyter_environment Modules : {{ env.modules }}"
                """
            }
        },
        help="Jupyterhub singleuser server execution context"
    ).tag(config=True)

    # getting SLURM JWT admin token from envvar
    SLURM_JWT = os.getenv('SLURM_JWT')

    job_id = Unicode()
    job_status = Unicode()

    def _get_profiles(self):
        """return profiles list, this method could be overrided for customization purpose

        Must return a list of Tuples like ("Display Name", "codename", dict( slurm API Job specs))

        See https://slurm.schedmd.com/rest_api.html#v0.0.44_job for jobs specs.

        Returns:
            list: profile list
        """
        return self.profiles

    def _get_jupyter_environments(self):
        """return jupyter_environments dict of dict

        For environment dict :
        * description (must) : Displayed description
        * path (could) : This string is appends to job $PATH env var (e.g. /dir1:/dir2)
        * module (could) : List of modules to load (separated with space)
        * image (could) : Absolute path for Singularity/Apptainer sif to use
        * script_prologue_template (could): Jinja2 template added to script (after path and module, before jupyter)

        Returns:
            dict: available environments
        """
        return self.jupyter_environments

    def _options_form_default(self):
        """return HTML for options_form (rendered template)"""
        search_path = os.path.dirname(os.path.abspath(self.options_form_template))
        template_name = os.path.basename(self.options_form_template)

        loader = FileSystemLoader(search_path)
        env = Environment(loader=loader, autoescape=True)

        template = env.get_template(template_name)
        context = { "profiles": self._get_profiles(),
                    "jupyter_environments": self._get_jupyter_environments()
                }
        return template.render(**context)

    def _selected_profile_specs(self):
        """return job specs selected by user on options_form

        Dict( slurm API Job specs))
        See https://slurm.schedmd.com/rest_api.html#v0.0.44_job for jobs specs.

        Returns:
            dict: Slurp API job specs
        """
        profiles = self._get_profiles()
        default_profile = profiles[0]
        try:
            user_selected_profile_name = self.user_options['profiles'][0]
        except:
            return default_profile
        for p in profiles:
            if p[1] == user_selected_profile_name:
                return p[2]
        return default_profile

    def _selected_jupyter_environments(self):
        """return jupyter_environments selected by user on options_form

        For environment dict :
        * description (must) : Displayed description
        * path (could) : This string is appends to job $PATH env var (e.g. /dir1:/dir2)
        * module (could) : List of modules to load (separated with space)
        * image (could) : Absolute path for Singularity/Apptainer sif to use
        * script_prologue_template (could): Jinja2 template added to script (after path and module, before jupyter)

        Returns:
            dict: environment dict
        """
        envs = self._get_jupyter_environments()
        default_jupyter_environments = next(iter( envs.items() ))
        try:
            user_selected_jupyter_environments = self.user_options['jupyter_environments'][0]
            return envs[user_selected_jupyter_environments]
        except:
            return default_jupyter_environments

    def _prepare_script(self):
        """return script to submitted

        Returns:
            str: job script
        """

        search_path = os.path.dirname(os.path.abspath(self.job_script_template))
        template_name = os.path.basename(self.job_script_template)

        loader = FileSystemLoader(search_path)
        env = Environment(loader=loader, autoescape=True)

        template = env.get_template(template_name)
        context = { "spawner": self,
                    "selected_jupyter_environment": self._selected_jupyter_environments()
                }
        self.log.debug(template.render(**context))
        return template.render(**context)
        # try:
        #     script_prologue_template = Environment(loader=BaseLoader).from_string(
        #         selected_jupyter_environment['script_prologue_template']
        #         )
        #     context = { "env": selected_jupyter_environment,
        #                 "username": self.user.name }
        #     prologue = script_prologue_template.render(**context)
        # except:
        #     self.log.error("Failed prologue templating")

    async def submit_batch_script(self, port):
        """submit batch to SLURM Rest API

        Args:
            port (int): Singleuser server port

        Returns:
            str: Job identifier
        """
        url = f"{self.slurm_rest_api}/job/submit"
        headers = { "X-SLURM-USER-TOKEN": self.SLURM_JWT,
                    "X-SLURM-USER-NAME": self.user.name
                }
        # get script
        script = self._prepare_script()
        # get Jupyterhub env variables to singleuser job
        jhub_envs = [f"{k}={v}" for k, v in self.get_env().items() ]
        # get job profile specs
        profile = self._selected_profile_specs()

        job_data = {
            "job": {
                "user_id": self.user.name,
                "name": f"{self.job_name_prefix}{self.user.name}",
                "current_working_directory": f"{self.job_working_path_prefix}/{self.user.name}",
                "script": script,
                "environment": jhub_envs + [f"JUPYTERHUB_SINGLEUSER_PORT={port}"],
                "tasks": 1
            }
        }

        job_data['job'] = job_data['job'] | profile

        self.log.debug(job_data)

        response = requests.post(url,
                                headers=headers,
                                json=job_data,
                                timeout=10)

        if response.status_code == 200:
            data = response.json()
            self.log.info(f"Job submitted! Job ID: {data['job_id']}")
            self.job_id = f"{data['job_id']}"
        else:
            self.log.error(f"Failed to submit job. Status code: {response.status_code}")
            self.log.error(response.text)
            self.job_id = ""
        return self.job_id

    async def query_job_status(self):
        """Check job status, return JobStatus object."""
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ""
            return JobStatus.NOTFOUND

        self.log.debug("Spawner querying job: " + self.job_id)
        url = f"{self.slurm_rest_api}/job/{self.job_id}"
        headers = {"X-SLURM-USER-TOKEN": self.SLURM_JWT}
        response = requests.get(url,
                                headers=headers,
                                timeout=10)
        if response.status_code == 200:
            data = response.json()
            self.log.info(f"Job state : {data['jobs'][0]['job_state']}")
            self.log.info(f"Running  on : {data['jobs'][0]['nodes']}")
        else:
            self.job_status = "NOTFOUND"
            return JobStatus.NOTFOUND

        if "RUNNING" in data['jobs'][0]['job_state']:
            self.job_status = "RUNNING"
            return JobStatus.RUNNING
        elif "PENDING" in data['jobs'][0]['job_state']:
            self.job_status = "PENDING"
            return JobStatus.PENDING
        elif "FAILED" in data['jobs'][0]['job_state']:
            self.job_status = "FAILED"
            return JobStatus.FAILED
        elif "CANCELLED" in data['jobs'][0]['job_state']:
            self.job_status = "FAILED"
            return JobStatus.CANCELLED
        elif "COMPLETED" in data['jobs'][0]['job_state']:
            self.job_status = "COMPLETED"
            return JobStatus.COMPLETED
        else:
            self.job_status = "UNKNOWN"
            return JobStatus.UNKNOWN

    async def cancel_batch_job(self):
        """cancel job

        Returns:
            None
        """
        self.log.info("Cancelling job " + self.job_id)
        url = f"{self.slurm_rest_api}/job/{self.job_id}"
        headers = {"X-SLURM-USER-TOKEN": self.SLURM_JWT}
        try:
            response = requests.delete(url,
                                    headers=headers,
                                    timeout=10)
            if response.status_code == 200:
                self.log.info(f"Job deleted : {self.job_id}")
            else:
                self.log.error(f"Failed to delete job. Status code: {response.status_code}")
                self.log.error(response.text)
            return None
        except:
            self.log.error(f"Failed to delete job. Status code: {response.status_code}")
            return None

    def load_state(self, state):
        """load job_id and status from state"""
        super().load_state(state)
        self.job_id = state.get("job_id", "")
        self.job_status = state.get("job_status", "")

    def get_state(self):
        """add job_id to state"""
        state = super().get_state()
        if self.job_id:
            state["job_id"] = self.job_id
        if self.job_status:
            state["job_status"] = self.job_status
        return state

    def clear_state(self):
        """clear job_id state"""
        super().clear_state()
        self.job_id = ""
        self.job_status = ""

    def hostname_resolver(self, hostname):
        """resolve nodename to an IP address reachable by Jupyterhub

        This method could be overrided for customization purpose. (e.g. if
        Jupyterhub could not resolv cluster nodename by itself)

        Args:
            hostname (str): nodename to resolve

        Returns:
            str: node IP address
        """
        return socket.gethostbyname(hostname)

    def state_gethost(self):
        """ return job node ip if job exist

        Get node name in job description, resolve nodename with hostname_resolver

        Returns:
            str: node IP address
        """
        if self.job_id is None or len(self.job_id) == 0:
            self.job_status = ""
            return None

        self.log.debug("Spawner querying job: " + self.job_id)
        url = f"{self.slurm_rest_api}/job/{self.job_id}"
        headers = {"X-SLURM-USER-TOKEN": self.SLURM_JWT}
        response = requests.get(url,
                                headers=headers,
                                timeout=10)
        if response.status_code == 200:
            data = response.json()
            self.log.info(f"Job node : {data['jobs'][0]['nodes']}")
            ip = self.hostname_resolver(data['jobs'][0]['nodes'])
            return ip
        else:
            return None

    async def poll(self):
        """Poll the process"""
        status = await self.query_job_status()
        if status in (JobStatus.PENDING, JobStatus.RUNNING, JobStatus.UNKNOWN):
            return None
        else:
            self.clear_state()
            return 1

    startup_poll_interval = Float(
        2,
        help="Polling interval (seconds) to check job state during startup",
    ).tag(config=True)

    async def start(self):
        """Start the process"""
        self.ip = "0.0.0.0"
        # get a random port and cross finders that this port isn't used on node ;)
        self.port = random.randrange(49152, 65535)

        if self.server:
            self.server.port = self.port

        await self.submit_batch_script(port=self.port)

        # We are called with a timeout, and if the timeout expires this function will
        # be interrupted at the next yield, and self.stop() will be called.
        # So this function should not return unless successful, and if unsuccessful
        # should either raise and Exception or loop forever.
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter batch job submission failure"
            )
        while True:
            status = await self.query_job_status()
            if status == JobStatus.RUNNING:
                break
            elif status == JobStatus.PENDING:
                self.log.debug("Job " + self.job_id + " still pending")
            elif status == JobStatus.FAILED:
                self.log.debug("Job " + self.job_id + " failed")
                raise RuntimeError(
                    "Jupyter batch job failed. Check job logs."
                )
            elif status == JobStatus.UNKNOWN:
                self.log.debug("Job " + self.job_id + " still unknown")
            else:
                self.log.warning(
                    "Job "
                    + self.job_id
                    + " neither pending nor running.\n"
                    + self.job_status
                )
                self.clear_state()
                raise RuntimeError(
                    "The Jupyter batch job has disappeared"
                    " while pending in the queue or died immediately"
                    " after starting."
                )
            await asyncio.sleep(self.startup_poll_interval)

        self.ip = self.state_gethost()

        self.db.commit()
        self.log.info(
            "Notebook server job {} started at {}:{}".format(
                self.job_id, self.ip, self.port
            )
        )

        return self.ip, self.port

    async def stop(self, now=False):
        self.log.info("Stopping server job " + self.job_id)
        await self.cancel_batch_job()
        if now:
            return
        for i in range(10):
            status = await self.query_job_status()
            if status not in (JobStatus.RUNNING, JobStatus.UNKNOWN):
                return
            await asyncio.sleep(1)
        if self.job_id:
            self.log.warning(
                "Notebook server job {} at {}:{} possibly failed to terminate".format(
                    self.job_id, self.ip, self.port
                )
            )

    async def progress(self):
        while True:
            if self.job_status == "PENDING":
                yield {"message": "Pending in queue..."}
            elif self.job_status == "FAILED":
                yield {"message": "Cluster job failed..."}
                return
            elif self.job_status == "RUNNING":
                yield {"message": "Cluster job running... waiting to connect"}
                return
            else:
                yield {"message": "Unknown status..."}
            await asyncio.sleep(1)
