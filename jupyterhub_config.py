import sys
sys.path.append('.')
from slurmrestspawner.slurmrestspawner import SlurmRestSpawner

c = get_config()

c.Authenticator.admin_users = {'admin1'}
c.Authenticator.allowed_users = set()
c.JupyterHub.authenticator_class = "dummy"

# ---------------------------
# BatchSpawner
# ---------------------------


class CustomSlurmSpawner(SlurmRestSpawner):
    def hostname_resolver(self, hostname):
        return f"10.64.{hostname.split("-")[1]}.{hostname.split("-")[2]}"


c.JupyterHub.spawner_class = CustomSlurmSpawner

c.SlurmRestSpawner.slurm_rest_api = "https://slurm-rest.hpc.ifremer.fr/slurm/v0.0.41"

c.SlurmRestSpawner.job_working_path_prefix = "/ontap/user"

c.SlurmRestSpawner.profiles = [
    ('SLURM - cpu - 1 core, 4 gb, 8 hours', 'slurm_cpu_c1_m4_w8', dict(cpus_per_task = 1, memory_per_node = 4000, time_limit = 480, partition = 'cpu')),
    ('SLURM - cpu - 8 core, 32 gb, 8 hours', 'slurm_cpu_c8_m32_w8', dict(cpus_per_task = 8, memory_per_node = 32000, time_limit = 480, partition = 'cpu'))
]

c.SlurmRestSpawner.jupyter_environments = {
            "apptainer_phyocean": {
                "description": "PhyOcean latest",
                "path": "",
                "modules": "matlab/2024b",
                "image": "/app/apptainer-images/phyocean/latest.sif",
                "script_prologue_template": """
                    echo "Phyocean rocks !!"
                """
            },
            "apptainer_pangeo": {
                "description": "Pangeo latest",
                "path": "/custom/path/for/test",
                "image": "/app/apptainer-images/pangeo/latest.sif",
            }
}

c.Spawner.notebook_dir = 'jupyterhub'

c.JupyterHub.cleanup_servers = True
c.Spawner.start_timeout = 120

c.Spawner.cmd = ['jupyter-labhub']

c.Application.log_level = 'INFO'

